import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { loadTesterConfig } from './config.ts';
import { curateNodes, curateXrayJson, curateSingboxJson, type TestResult } from './tester.ts';

const PORT = 3000;
const PROFILES_DIR = path.resolve(process.cwd(), 'profiles');
const ENV_FILE = path.resolve(process.cwd(), '.env');

if (!fs.existsSync(PROFILES_DIR)) {
  fs.mkdirSync(PROFILES_DIR, { recursive: true });
}

// Current test progress state for SSE streaming
interface ActiveTestState {
  running: boolean;
  file?: string;
  total: number;
  completed: number;
  aliveCount: number;
  deadCount: number;
  results: TestResult[];
  mode: 'direct' | 'chain';
  chainProxy?: string;
  error?: string;
}

let activeTest: ActiveTestState = {
  running: false,
  total: 0,
  completed: 0,
  aliveCount: 0,
  deadCount: 0,
  results: [],
  mode: 'direct',
};

const sseClients: http.ServerResponse[] = [];

function notifyClients(data: any) {
  const payload = `data: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(payload);
    } catch {
      // client disconnected
    }
  }
}

function getProfilesList() {
  if (!fs.existsSync(PROFILES_DIR)) return [];
  return fs
    .readdirSync(PROFILES_DIR)
    .filter((f) => f.endsWith('.txt') || f.endsWith('.json') || f.endsWith('.yaml') || f.endsWith('.yml'))
    .map((name) => {
      const filePath = path.join(PROFILES_DIR, name);
      const stat = fs.statSync(filePath);
      return {
        name,
        size: stat.size,
        updatedAt: stat.mtime.toISOString(),
        isEnhanced: name.includes('.enhanced.'),
        isChain: name.includes('.chain.'),
        format: name.endsWith('.json') ? 'json' : (name.endsWith('.yaml') || name.endsWith('.yml')) ? 'yaml' : 'txt',
      };
    });
}

function getEnvData() {
  if (!fs.existsSync(ENV_FILE)) {
    return { tokenSet: false, accountId: '', subSecret: '', chainProxy: 'socks5://127.0.0.1:10808', raw: '' };
  }
  const raw = fs.readFileSync(ENV_FILE, 'utf-8');
  const tokenMatch = raw.match(/CLOUDFLARE_API_TOKEN=["']?([^"'\r\n]+)["']?/);
  const accountMatch = raw.match(/CLOUDFLARE_ACCOUNT_ID=["']?([^"'\r\n]+)["']?/);
  const secretMatch = raw.match(/SUB_SECRET=["']?([^"'\r\n]+)["']?/);
  const chainMatch = raw.match(/CHAIN_PROXY=["']?([^"'\r\n]+)["']?/);
  return {
    tokenSet: Boolean(tokenMatch && tokenMatch[1] && tokenMatch[1].startsWith('cf')),
    tokenSnippet: tokenMatch && tokenMatch[1] ? `${tokenMatch[1].slice(0, 6)}...${tokenMatch[1].slice(-4)}` : '',
    accountId: accountMatch ? accountMatch[1] : '',
    subSecret: secretMatch ? secretMatch[1] : '',
    chainProxy: chainMatch ? chainMatch[1] : (process.env.CHAIN_PROXY || 'socks5://127.0.0.1:10808'),
    raw,
  };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, DELETE');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // SSE progress stream
  if (url.pathname === '/api/progress-stream') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
    sseClients.push(res);
    res.write(`data: ${JSON.stringify({ type: 'init', state: activeTest })}\n\n`);

    req.on('close', () => {
      const idx = sseClients.indexOf(res);
      if (idx !== -1) sseClients.splice(idx, 1);
    });
    return;
  }

  // API: List files in profiles/
  if (url.pathname === '/api/files' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ files: getProfilesList() }));
    return;
  }

  // API: Read file content
  if (url.pathname === '/api/file' && req.method === 'GET') {
    const filename = url.searchParams.get('name');
    if (!filename || filename.includes('..')) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'نام فایل نامعتبر است' }));
      return;
    }
    const filePath = path.join(PROFILES_DIR, filename);
    if (!fs.existsSync(filePath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'فایل یافت نشد' }));
      return;
    }
    const content = fs.readFileSync(filePath, 'utf-8');
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ filename, content }));
    return;
  }

  // API: Save / Overwrite file
  if (url.pathname === '/api/file' && req.method === 'POST') {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      try {
        const { filename, content } = JSON.parse(body);
        if (!filename || filename.includes('..')) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'نام فایل نامعتبر است' }));
          return;
        }
        const filePath = path.join(PROFILES_DIR, filename);
        fs.writeFileSync(filePath, content, 'utf-8');
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, filename }));
      } catch (err: any) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // API: Delete file
  if (url.pathname === '/api/file' && req.method === 'DELETE') {
    const filename = url.searchParams.get('name');
    if (!filename || filename.includes('..')) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'نام فایل نامعتبر است' }));
      return;
    }
    const filePath = path.join(PROFILES_DIR, filename);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true }));
    return;
  }

  // API: Start Xray Testing & Cleaning
  if (url.pathname === '/api/test/start' && req.method === 'POST') {
    if (activeTest.running) {
      res.writeHead(409, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'یک فرآیند تست هم‌اکنون در حال اجراست.' }));
      return;
    }

    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', async () => {
      try {
        const { filename, content, mode, chainProxy, timeoutMs, concurrency } = JSON.parse(body || '{}');

        let targetContent = content;
        let targetName = filename;

        if (!targetContent && targetName) {
          const filePath = path.join(PROFILES_DIR, targetName);
          if (fs.existsSync(filePath)) {
            targetContent = fs.readFileSync(filePath, 'utf-8');
          }
        }

        if (!targetContent) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'محتوا یا فایلی برای تست ارسال نشده است.' }));
          return;
        }

        const isChain = mode === 'chain';
        const defaultChain = getEnvData().chainProxy || 'socks5://127.0.0.1:10808';
        const proxyUri = isChain ? (chainProxy?.trim() || defaultChain) : undefined;

        const config = loadTesterConfig([]);
        if (timeoutMs) config.timeoutMs = parseInt(timeoutMs, 10);
        if (concurrency) config.concurrency = parseInt(concurrency, 10);
        config.chainProxy = proxyUri;

        activeTest = {
          running: true,
          file: targetName,
          total: 0,
          completed: 0,
          aliveCount: 0,
          deadCount: 0,
          results: [],
          mode: isChain ? 'chain' : 'direct',
          chainProxy: proxyUri,
        };

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'started', mode: activeTest.mode }));

        // Run async test task in background
        runBackgroundTest(targetName, targetContent, config, isChain).catch((err) => {
          activeTest.running = false;
          activeTest.error = err.message;
          notifyClients({ type: 'error', error: err.message });
        });
      } catch (err: any) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // API: Get Current Test Status
  if (url.pathname === '/api/test/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(activeTest));
    return;
  }

  // API: Get Cloudflare Configuration & Status
  if (url.pathname === '/api/cf/status' && req.method === 'GET') {
    const envInfo = getEnvData();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(envInfo));
    return;
  }

  // API: Cloudflare Whoami
  if (url.pathname === '/api/cf/whoami' && req.method === 'POST') {
    runWranglerCommand(['whoami'], res);
    return;
  }

  // API: Cloudflare Deploy
  if (url.pathname === '/api/cf/deploy' && req.method === 'POST') {
    // Before deploy, run generate-profiles first
    try {
      const genScript = path.resolve(process.cwd(), 'tools/generate-profiles.ts');
      const gen = spawn('node', ['--experimental-strip-types', genScript], { stdio: 'pipe' });
      let genOut = '';
      gen.stdout.on('data', (d) => (genOut += d));
      gen.stderr.on('data', (d) => (genOut += d));
      gen.on('close', (code) => {
        if (code !== 0) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, output: `خطا در ساخت پروفایل‌ها:\n${genOut}` }));
          return;
        }
        runWranglerCommand(['deploy'], res, genOut);
      });
    } catch (e: any) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, output: e.message }));
    }
    return;
  }

  // API: Cloudflare Put Secret
  if (url.pathname === '/api/cf/secret' && req.method === 'POST') {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      try {
        const { secret } = JSON.parse(body || '{}');
        if (!secret || typeof secret !== 'string') {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'مقدار Secret نمی‌تواند خالی باشد.' }));
          return;
        }

        // Put secret to Cloudflare via wrangler secret put
        const pnpmCmd = 'pnpm';
        const args = ['exec', 'wrangler', 'secret', 'put', 'SUB_SECRET', '--env-file', ENV_FILE];
        const child = spawn(pnpmCmd, args, { stdio: ['pipe', 'pipe', 'pipe'] });

        let output = '';
        child.stdout.on('data', (d) => (output += d));
        child.stderr.on('data', (d) => (output += d));

        // Write secret to child stdin
        child.stdin.write(secret + '\n');
        child.stdin.end();

        child.on('close', (code) => {
          if (code === 0) {
            // Also sync to .env file
            try {
              if (fs.existsSync(ENV_FILE)) {
                let envText = fs.readFileSync(ENV_FILE, 'utf-8');
                if (envText.match(/^SUB_SECRET=/m)) {
                  envText = envText.replace(/^SUB_SECRET=.*/m, `SUB_SECRET="${secret}"`);
                } else {
                  envText += `\nSUB_SECRET="${secret}"\n`;
                }
                fs.writeFileSync(ENV_FILE, envText, 'utf-8');
              }
            } catch (err) {
              console.error('Failed to sync SUB_SECRET in .env:', err);
            }

            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, output }));
          } else {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, output }));
          }
        });
      } catch (err: any) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // API: Regenerate profiles inline bundle
  if (url.pathname === '/api/cf/generate-profiles' && req.method === 'POST') {
    const genScript = path.resolve(process.cwd(), 'tools/generate-profiles.ts');
    const gen = spawn('node', ['--experimental-strip-types', genScript], { stdio: 'pipe' });
    let output = '';
    gen.stdout.on('data', (d) => (output += d));
    gen.stderr.on('data', (d) => (output += d));
    gen.on('close', (code) => {
      res.writeHead(code === 0 ? 200 : 500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: code === 0, output }));
    });
    return;
  }

  // Serve GUI HTML
  if (url.pathname === '/' || url.pathname === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(renderTesterGuiHtml());
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

function runWranglerCommand(wranglerArgs: string[], res: http.ServerResponse, prefixOutput: string = '') {
  const pnpmCmd = 'pnpm';
  const args = ['exec', 'wrangler', ...wranglerArgs, '--env-file', ENV_FILE];
  const child = spawn(pnpmCmd, args, { stdio: ['ignore', 'pipe', 'pipe'] });

  let output = prefixOutput ? `${prefixOutput}\n` : '';
  child.stdout.on('data', (d) => (output += d));
  child.stderr.on('data', (d) => (output += d));

  child.on('close', (code) => {
    res.writeHead(code === 0 ? 200 : 500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: code === 0, output }));
  });

  child.on('error', (err) => {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: false, output: err.message }));
  });
}

async function runBackgroundTest(filename: string | undefined, content: string, config: any, isChain: boolean) {
  const isJson = filename?.endsWith('.json') || content.trim().startsWith('{');
  
  const onProgress = (p: any) => {
    activeTest.completed = p.completed;
    activeTest.total = p.total;
    activeTest.results = p.results;
    activeTest.aliveCount = p.results.filter((r: any) => r.alive).length;
    activeTest.deadCount = p.results.filter((r: any) => !r.alive).length;
    notifyClients({
      type: 'progress',
      completed: p.completed,
      total: p.total,
      lastResult: p.lastResult,
      aliveCount: activeTest.aliveCount,
      deadCount: activeTest.deadCount,
    });
  };

  if (isJson) {
    try {
      const parsed = JSON.parse(content);
      const isSingbox = Boolean(parsed.outbounds || parsed.inbounds || parsed.route);
      
      notifyClients({ type: 'start', format: 'json', file: filename });
      
      const res = isSingbox
        ? await curateSingboxJson(parsed, config, onProgress)
        : await curateXrayJson(parsed, config, onProgress);
        
      const { results, enhancedConfig } = res;
      const alive = results.filter((r) => r.alive);
      const dead = results.filter((r) => !r.alive);
      
      activeTest.completed = results.length;
      activeTest.aliveCount = alive.length;
      activeTest.deadCount = dead.length;
      activeTest.results = results;
      activeTest.running = false;
      
      notifyClients({
        type: 'done',
        total: results.length,
        alive: alive.length,
        dead: dead.length,
        results,
        cleanJson: enhancedConfig,
        file: filename,
        mode: isChain ? 'chain' : 'direct',
      });
      return;
    } catch (err: any) {
      activeTest.running = false;
      activeTest.error = err.message;
      notifyClients({ type: 'error', error: err.message });
      return;
    }
  }

  // Handle .txt lines
  const lines = content
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith('#') && !l.startsWith('//') && l.includes('://'));

  activeTest.total = lines.length;
  notifyClients({ type: 'start', format: 'txt', total: lines.length, file: filename });

  const results = await curateNodes(lines, config, onProgress);

  const alive = results.filter((r) => r.alive).sort((a, b) => a.latencyMs - b.latencyMs);
  const dead = results.filter((r) => !r.alive);

  activeTest.completed = results.length;
  activeTest.aliveCount = alive.length;
  activeTest.deadCount = dead.length;
  activeTest.results = results;
  activeTest.running = false;

  // Format clean alive nodes
  const formattedCleanNodes = alive.map((node, index) => {
    const orderNum = String(index + 1).padStart(2, '0');
    const flag = node.geo?.flagEmoji || '🌐';
    const countryCode = node.geo?.countryCode && node.geo.countryCode !== 'XX' ? node.geo.countryCode : 'Proxy';
    const cityOrCountry = node.geo?.city || node.geo?.countryName || '';
    const loc = cityOrCountry ? ` - ${cityOrCountry}` : '';
    const modeTag = isChain ? ' 🔗' : '';
    const newRemark = `${flag} ${countryCode}${loc} | ${node.latencyMs}ms${modeTag} #${orderNum}`;
    const baseWithoutHash = node.raw.split('#')[0];
    return `${baseWithoutHash}#${encodeURIComponent(newRemark)}`;
  });

  const cleanTextOutput = formattedCleanNodes.join('\n') + (formattedCleanNodes.length ? '\n' : '');

  notifyClients({
    type: 'done',
    total: results.length,
    alive: alive.length,
    dead: dead.length,
    results,
    cleanText: cleanTextOutput,
    file: filename,
    mode: isChain ? 'chain' : 'direct',
  });
}

function renderTesterGuiHtml(): string {
  return `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>پنل مدیریت سابسکریپشن و تستر نودها | Cloudflare Sub Manager</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Vazirmatn:wght@300;400;500;700;900&display=swap">
  <style>
    :root {
      --bg: #07090e;
      --card-bg: rgba(14, 18, 27, 0.85);
      --card-border: rgba(239, 68, 68, 0.22);
      --card-hover: rgba(20, 26, 38, 0.95);
      --red: #ef4444;
      --red-glow: rgba(239, 68, 68, 0.35);
      --red-hover: #dc2626;
      --primary: #ef4444;
      --primary-hover: #dc2626;
      --cf-orange: #f38020;
      --cf-orange-glow: rgba(243, 128, 32, 0.35);
      --success: #10b981;
      --danger: #ef4444;
      --chain: #f59e0b;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --mono-font: 'JetBrains Mono', 'Fira Code', ui-monospace, SFMono-Regular, monospace;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Vazirmatn', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body {
      background-color: var(--bg);
      background-image: 
        linear-gradient(to right, rgba(239, 68, 68, 0.05) 1px, transparent 1px),
        linear-gradient(to bottom, rgba(239, 68, 68, 0.05) 1px, transparent 1px),
        radial-gradient(ellipse 80% 50% at 50% -10%, rgba(220, 38, 38, 0.18), transparent 75%);
      background-size: 38px 38px, 38px 38px, 100% 100%;
      color: var(--text);
      min-height: 100vh;
      padding: 1.5rem 1rem;
    }
    .container {
      max-width: 1240px;
      margin: 0 auto;
    }
    header {
      text-align: center;
      margin-bottom: 1.5rem;
    }
    .brand-tag {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      background: rgba(239, 68, 68, 0.12);
      border: 1px solid rgba(239, 68, 68, 0.3);
      padding: 0.35rem 1rem;
      border-radius: 9999px;
      font-size: 0.8rem;
      color: var(--red);
      font-weight: 700;
      letter-spacing: 0.5px;
      margin-bottom: 0.6rem;
      text-shadow: 0 0 10px var(--red-glow);
    }
    h1 {
      font-size: 1.9rem;
      font-weight: 900;
      color: #fff;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.6rem;
      margin-bottom: 0.4rem;
      letter-spacing: -0.5px;
    }
    .laser-accent {
      color: var(--red);
      text-shadow: 0 0 12px var(--red-glow);
    }
    p.subtitle {
      color: var(--text-muted);
      font-size: 0.9rem;
      max-width: 650px;
      margin: 0 auto;
      line-height: 1.5;
    }

    /* Tabs Navigation */
    .nav-tabs {
      display: flex;
      gap: 0.5rem;
      background: rgba(14, 18, 27, 0.9);
      border: 1px solid var(--card-border);
      border-radius: 0.8rem;
      padding: 0.4rem;
      margin-bottom: 1.5rem;
      backdrop-filter: blur(10px);
    }
    .nav-tab-btn {
      flex: 1;
      padding: 0.75rem 1rem;
      border: none;
      background: transparent;
      color: var(--text-muted);
      border-radius: 0.6rem;
      font-size: 0.95rem;
      font-weight: 700;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.5rem;
      transition: all 0.2s;
    }
    .nav-tab-btn:hover {
      color: #fff;
      background: rgba(255, 255, 255, 0.05);
    }
    .nav-tab-btn.active {
      color: #fff;
      background: linear-gradient(135deg, rgba(239, 68, 68, 0.9), rgba(185, 28, 28, 0.9));
      box-shadow: 0 4px 15px rgba(239, 68, 68, 0.35);
    }
    .nav-tab-btn.cf-tab.active {
      background: linear-gradient(135deg, rgba(243, 128, 32, 0.95), rgba(217, 102, 10, 0.95));
      box-shadow: 0 4px 15px rgba(243, 128, 32, 0.35);
    }

    .tab-content {
      display: none;
    }
    .tab-content.active {
      display: block;
    }

    .grid {
      display: grid;
      grid-template-columns: 390px 1fr;
      gap: 1.5rem;
    }
    @media (max-width: 950px) {
      .grid { grid-template-columns: 1fr; }
    }

    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 1rem;
      padding: 1.4rem;
      backdrop-filter: blur(14px);
      box-shadow: 0 20px 40px -10px rgba(0, 0, 0, 0.7), 0 0 25px rgba(239, 68, 68, 0.05);
      margin-bottom: 1.5rem;
    }
    .card.cf-card {
      border-color: rgba(243, 128, 32, 0.3);
      box-shadow: 0 20px 40px -10px rgba(0, 0, 0, 0.7), 0 0 25px rgba(243, 128, 32, 0.08);
    }
    .card-title {
      font-size: 1.1rem;
      font-weight: 800;
      margin-bottom: 1.2rem;
      display: flex;
      align-items: center;
      gap: 0.5rem;
      color: #fff;
      border-bottom: 1px solid rgba(239, 68, 68, 0.15);
      padding-bottom: 0.75rem;
    }
    .cf-card .card-title {
      border-bottom-color: rgba(243, 128, 32, 0.2);
    }
    .form-group {
      margin-bottom: 1rem;
    }
    label {
      display: block;
      font-size: 0.85rem;
      color: var(--text-muted);
      margin-bottom: 0.4rem;
      font-weight: 500;
    }
    select, input[type="text"], input[type="number"], textarea {
      width: 100%;
      background: rgba(7, 9, 14, 0.85);
      border: 1px solid rgba(239, 68, 68, 0.25);
      border-radius: 0.6rem;
      padding: 0.7rem 0.9rem;
      color: #fff;
      font-size: 0.9rem;
      outline: none;
      transition: all 0.2s;
    }
    select:focus, input:focus, textarea:focus {
      border-color: var(--red);
      box-shadow: 0 0 0 3px rgba(239, 68, 68, 0.25);
    }
    .cf-card select:focus, .cf-card input:focus {
      border-color: var(--cf-orange);
      box-shadow: 0 0 0 3px rgba(243, 128, 32, 0.25);
    }
    textarea {
      resize: vertical;
      min-height: 140px;
      direction: ltr;
      font-family: var(--mono-font);
      font-size: 0.8rem;
      line-height: 1.4;
    }
    .mode-switch {
      display: flex;
      gap: 0.5rem;
      background: rgba(7, 9, 14, 0.85);
      padding: 0.3rem;
      border-radius: 0.7rem;
      border: 1px solid rgba(239, 68, 68, 0.2);
      margin-bottom: 1rem;
    }
    .mode-btn {
      flex: 1;
      padding: 0.65rem 0.5rem;
      border: none;
      background: transparent;
      color: var(--text-muted);
      border-radius: 0.5rem;
      font-size: 0.85rem;
      font-weight: 700;
      cursor: pointer;
      transition: all 0.2s;
    }
    .mode-btn.active.direct {
      background: linear-gradient(135deg, #ef4444, #dc2626);
      color: #fff;
      box-shadow: 0 4px 12px rgba(239, 68, 68, 0.4);
    }
    .mode-btn.active.chain {
      background: linear-gradient(135deg, #f59e0b, #d97706);
      color: #fff;
      box-shadow: 0 4px 12px rgba(245, 158, 11, 0.4);
    }
    .chain-box {
      background: rgba(245, 158, 11, 0.08);
      border: 1px dashed rgba(245, 158, 11, 0.4);
      border-radius: 0.6rem;
      padding: 0.85rem;
      margin-bottom: 1rem;
      display: none;
    }
    .chain-box.show {
      display: block;
    }
    .btn-action {
      width: 100%;
      padding: 0.9rem;
      border: none;
      border-radius: 0.6rem;
      background: linear-gradient(135deg, #ef4444, #b91c1c);
      color: #fff;
      font-size: 1rem;
      font-weight: 800;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.5rem;
      box-shadow: 0 4px 15px rgba(239, 68, 68, 0.35);
      transition: all 0.2s;
    }
    .btn-action:hover {
      background: linear-gradient(135deg, #dc2626, #991b1b);
      transform: translateY(-1px);
      box-shadow: 0 6px 20px rgba(239, 68, 68, 0.45);
    }
    .btn-action:disabled {
      opacity: 0.5;
      cursor: not-allowed;
      transform: none;
    }

    .btn-cf {
      background: linear-gradient(135deg, #f38020, #d9660a);
      box-shadow: 0 4px 15px rgba(243, 128, 32, 0.35);
    }
    .btn-cf:hover {
      background: linear-gradient(135deg, #fa8c2a, #e66d0c);
      box-shadow: 0 6px 20px rgba(243, 128, 32, 0.45);
    }

    .stats-bar {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 0.75rem;
      margin-bottom: 1.25rem;
    }
    .stat-card {
      background: rgba(7, 9, 14, 0.7);
      border: 1px solid var(--card-border);
      border-radius: 0.7rem;
      padding: 0.85rem 0.5rem;
      text-align: center;
    }
    .stat-val {
      font-size: 1.5rem;
      font-weight: 900;
      margin-top: 0.2rem;
      font-family: var(--mono-font);
    }
    .stat-alive { color: var(--success); text-shadow: 0 0 10px rgba(16, 185, 129, 0.3); }
    .stat-dead { color: var(--danger); text-shadow: 0 0 10px rgba(239, 68, 68, 0.3); }
    .stat-total { color: #f8fafc; }
    .stat-lbl {
      font-size: 0.75rem;
      color: var(--text-muted);
      font-weight: 600;
    }
    .results-table-wrap {
      max-height: 480px;
      overflow-y: auto;
      border: 1px solid var(--card-border);
      border-radius: 0.7rem;
      background: rgba(7, 9, 14, 0.7);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      text-align: right;
      font-size: 0.85rem;
    }
    th {
      background: rgba(14, 18, 27, 0.95);
      padding: 0.8rem 0.9rem;
      color: var(--text-muted);
      position: sticky;
      top: 0;
      border-bottom: 1px solid var(--card-border);
      font-weight: 700;
    }
    td {
      padding: 0.7rem 0.9rem;
      border-bottom: 1px solid rgba(239, 68, 68, 0.1);
    }
    tr:last-child td { border-bottom: none; }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 0.25rem;
      padding: 0.2rem 0.55rem;
      border-radius: 0.4rem;
      font-size: 0.75rem;
      font-weight: 700;
    }
    .badge-alive { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }
    .badge-dead { background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); }
    .badge-proto { background: rgba(255, 255, 255, 0.08); color: #e2e8f0; font-family: var(--mono-font); font-size: 0.7rem; }
    .latency-fast { color: #34d399; font-weight: 700; font-family: var(--mono-font); }
    .latency-med { color: #fbbf24; font-weight: 700; font-family: var(--mono-font); }
    .latency-slow { color: #f87171; font-weight: 700; font-family: var(--mono-font); }
    .clean-actions {
      display: flex;
      gap: 0.75rem;
      margin-top: 1.2rem;
    }
    .btn-secondary {
      flex: 1;
      padding: 0.75rem;
      background: rgba(20, 26, 38, 0.8);
      border: 1px solid var(--card-border);
      color: #fff;
      border-radius: 0.6rem;
      font-size: 0.85rem;
      cursor: pointer;
      font-weight: 700;
      transition: all 0.2s;
    }
    .btn-secondary:hover {
      background: rgba(30, 41, 59, 0.9);
      border-color: rgba(239, 68, 68, 0.4);
    }
    .badge-saved {
      background: rgba(16, 185, 129, 0.15);
      color: #6ee7b7;
      padding: 0.6rem 1rem;
      border-radius: 0.6rem;
      border: 1px solid rgba(16, 185, 129, 0.4);
      margin-top: 1rem;
      font-size: 0.85rem;
      display: none;
    }

    /* Terminal Console Window for Cloudflare output */
    .terminal-window {
      background: #050608;
      border: 1px solid rgba(243, 128, 32, 0.3);
      border-radius: 0.75rem;
      overflow: hidden;
      margin-top: 1rem;
      font-family: var(--mono-font);
    }
    .terminal-header {
      background: #0f131a;
      padding: 0.5rem 1rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid rgba(255, 255, 255, 0.08);
      font-size: 0.8rem;
      color: var(--text-muted);
    }
    .terminal-dots {
      display: flex;
      gap: 6px;
    }
    .dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
    }
    .dot-red { background: #ef4444; }
    .dot-yellow { background: #f59e0b; }
    .dot-green { background: #10b981; }
    .terminal-body {
      padding: 1rem;
      max-height: 380px;
      min-height: 180px;
      overflow-y: auto;
      white-space: pre-wrap;
      word-break: break-all;
      font-size: 0.85rem;
      line-height: 1.5;
      color: #38bdf8;
      direction: ltr;
      text-align: left;
    }

    .info-badge {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      padding: 0.35rem 0.75rem;
      background: rgba(255, 255, 255, 0.05);
      border-radius: 0.5rem;
      font-size: 0.85rem;
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="brand-tag">⚡ CLOUDFLARE SUB MANAGER & TESTER</div>
      <h1><span>پنل یکپارچه</span> <span class="laser-accent">وب و کلاودفلر</span></h1>
      <p class="subtitle">تست و بهینه‌سازی نودهای پروکسی، مدیریت پروفایل‌ها و دیپلوی مستقیم به ورکر Cloudflare از طریق مرورگر</p>
    </header>

    <!-- Navigation Tabs -->
    <div class="nav-tabs">
      <button class="nav-tab-btn active" onclick="switchTab('tab-tester')">
        <span>⚡ تستر و پالایشگر نودها (Xray)</span>
      </button>
      <button class="nav-tab-btn cf-tab" onclick="switchTab('tab-cloudflare')">
        <span>☁️ مدیریت و دیپلوی Cloudflare Workers</span>
      </button>
    </div>

    <!-- TAB 1: Tester & Curation -->
    <div id="tab-tester" class="tab-content active">
      <div class="grid">
        <!-- Left: Controls & Profile Select -->
        <div class="card">
          <div class="card-title">🎛️ تنظیمات و ورودی نودها</div>

          <div class="form-group">
            <label>انتخاب فایل از پوشه profiles:</label>
            <select id="fileSelect" onchange="loadSelectedFile()">
              <option value="">-- انتخاب فایل موجود --</option>
            </select>
          </div>

          <div class="form-group">
            <label>محتوای کانفیگ‌ها (لینک‌های پروکسی یا JSON کامل):</label>
            <textarea id="configContent" placeholder="vless://...&#10;vmess://...&#10;ss://..."></textarea>
          </div>

          <label>حالت تست کانفیگ‌ها:</label>
          <div class="mode-switch">
            <button id="btnDirect" class="mode-btn active direct" onclick="setMode('direct')">🌐 مستقیم (Direct)</button>
            <button id="btnChain" class="mode-btn chain" onclick="setMode('chain')">🔗 زنجیره‌ای (Chain Proxy)</button>
          </div>

          <div id="chainBox" class="chain-box">
            <label style="color:#c084fc;">آدرس پروکسی ورودی (Landing Proxy):</label>
            <input type="text" id="chainProxyInput" value="socks5://127.0.0.1:10808" placeholder="socks5://127.0.0.1:10808">
            <small style="color:var(--text-muted); display:block; margin-top:4px;">ترافیک ابتدا از این پروکسی محلی عبور کرده و وارد سرورهای شما می‌شود.</small>
          </div>

          <div style="display:grid; grid-template-columns: 1fr 1fr; gap:0.5rem;" class="form-group">
            <div>
              <label>مهلت پاسخ (ms):</label>
              <input type="number" id="timeoutInput" value="5000" min="1000" max="15000">
            </div>
            <div>
              <label>تست‌های همزمان:</label>
              <input type="number" id="concurrencyInput" value="8" min="1" max="32">
            </div>
          </div>

          <button id="btnRunTest" class="btn-action" onclick="startTest()">
            <span>🚀 شروع تست و پاک‌سازی</span>
          </button>

          <div id="saveNotification" class="badge-saved">
            ✔ کانفیگ‌های سالم ذخیره شدند: <span id="savedFileName" style="font-weight:700;"></span>
          </div>
        </div>

        <!-- Right: Live Monitor & Results -->
        <div class="card">
          <div class="card-title">📊 نتایج تست و کانفیگ‌های سالم</div>

          <div class="stats-bar">
            <div class="stat-card">
              <div class="stat-lbl">کل نودها</div>
              <div id="statTotal" class="stat-val stat-total">0</div>
            </div>
            <div class="stat-card">
              <div class="stat-lbl">سالم و پرسرعت ✅</div>
              <div id="statAlive" class="stat-val stat-alive">0</div>
            </div>
            <div class="stat-card">
              <div class="stat-lbl">ناسالم / سوخته ❌</div>
              <div id="statDead" class="stat-val stat-dead">0</div>
            </div>
          </div>

          <div class="results-table-wrap">
            <table>
              <thead>
                <tr>
                  <th style="width: 70px;">وضعیت</th>
                  <th style="width: 80px;">پروتکل</th>
                  <th>نام / مشخصات نود</th>
                  <th style="width: 90px; text-align: left;">تاخیر (Ping)</th>
                </tr>
              </thead>
              <tbody id="resultsBody">
                <tr>
                  <td colspan="4" style="text-align:center; color:var(--text-muted); padding: 2rem;">
                    فایلی را انتخاب یا کانفیگ‌ها را وارد کنید، سپس روی «شروع تست» کلیک کنید.
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <div class="clean-actions">
            <button class="btn-secondary" id="btnSaveOriginal" onclick="saveToOriginalFile()">
              💾 ذخیره روی فایل اصلی
            </button>
            <button class="btn-secondary" id="btnSaveWithSuffix" onclick="saveWithSuffixFile()">
              ✨ ذخیره با پسوند کنار فایل اصلی
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- TAB 2: Cloudflare Deployment & Ops -->
    <div id="tab-cloudflare" class="tab-content">
      <div class="grid">
        <!-- CF Actions -->
        <div class="card cf-card">
          <div class="card-title">☁️ عملیات Cloudflare Workers</div>

          <div style="margin-bottom: 1.2rem; display:flex; flex-direction:column; gap:0.5rem;">
            <div class="info-badge">
              <span>وضعیت توکن API:</span>
              <strong id="cfTokenStatus" style="color:#10b981;">بررسی...</strong>
            </div>
            <div class="info-badge">
              <span>Account ID:</span>
              <code id="cfAccountId" style="font-family:var(--mono-font); font-size:0.8rem; color:#94a3b8;">-</code>
            </div>
          </div>

          <div class="form-group" style="margin-top: 1rem;">
            <label>کلید رمزنگاری اشتراک (SUB_SECRET):</label>
            <div style="display:flex; gap:0.5rem;">
              <input type="text" id="subSecretInput" placeholder="مثال: my-super-secret-key">
              <button class="btn-secondary" style="width: auto; padding: 0.7rem 1.1rem; white-space: nowrap;" onclick="setSubSecret()">
                🔑 ثبت سکرت
              </button>
            </div>
            <small style="color:var(--text-muted); display:block; margin-top:4px;">
              این مقدار در Cloudflare Workers ذخیره شده و در فایل <code>.env</code> نیز سینک می‌گردد.
            </small>
          </div>

          <div style="display:flex; flex-direction:column; gap:0.75rem; margin-top:1.5rem;">
            <button class="btn-action btn-cf" id="btnDeploy" onclick="deployToCloudflare()">
              <span>🚀 دیپلوی سابسکریپشن به کلاودفلر (Deploy)</span>
            </button>

            <div style="display:grid; grid-template-columns: 1fr 1fr; gap:0.5rem;">
              <button class="btn-secondary" onclick="checkWhoami()">
                <span>👤 تست اتصال (whoami)</span>
              </button>
              <button class="btn-secondary" onclick="rebuildProfiles()">
                <span>🔄 همگام‌سازی فایل‌ها</span>
              </button>
            </div>
          </div>
        </div>

        <!-- CF Console Logs -->
        <div class="card cf-card">
          <div class="card-title" style="justify-content:space-between;">
            <span>🖥️ کنسول لاگ و خروجی عملیات کلاودفلر</span>
            <button class="btn-secondary" style="font-size:0.75rem; padding:0.3rem 0.6rem; width:auto;" onclick="clearCfLog()">پاک کردن لاگ</button>
          </div>

          <div class="terminal-window">
            <div class="terminal-header">
              <div class="terminal-dots">
                <span class="dot dot-red"></span>
                <span class="dot dot-yellow"></span>
                <span class="dot dot-green"></span>
              </div>
              <span id="cfTaskStatus">آماده به کار</span>
            </div>
            <div id="cfConsoleOutput" class="terminal-body">برای مشاهده وضعیت یا دیپلوی، یکی از عملیات را انتخاب کنید.</div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
    let currentMode = 'direct';
    let lastCleanOutput = '';
    let lastTestedFile = '';
    let isTesting = false;

    function switchTab(tabId) {
      document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
      document.querySelectorAll('.nav-tab-btn').forEach(el => el.classList.remove('active'));

      document.getElementById(tabId).classList.add('active');
      if (tabId === 'tab-tester') {
        document.querySelectorAll('.nav-tab-btn')[0].classList.add('active');
      } else {
        document.querySelectorAll('.nav-tab-btn')[1].classList.add('active');
        fetchCfStatus();
      }
    }

    let liveResults = [];

    function setMode(mode) {
      currentMode = mode;
      const btnDirect = document.getElementById('btnDirect');
      const btnChain = document.getElementById('btnChain');
      const chainBox = document.getElementById('chainBox');

      if (mode === 'chain') {
        btnChain.classList.add('active');
        btnDirect.classList.remove('active');
        chainBox.classList.add('show');
      } else {
        btnDirect.classList.add('active');
        btnChain.classList.remove('active');
        chainBox.classList.remove('show');
      }
    }

    async function fetchFiles() {
      try {
        const res = await fetch('/api/files');
        const data = await res.json();
        const select = document.getElementById('fileSelect');
        select.innerHTML = '<option value="">-- انتخاب فایل موجود در profiles/ --</option>';
        data.files.forEach(f => {
          const opt = document.createElement('option');
          opt.value = f.name;
          const tag = f.isChain ? ' [Chain]' : (f.isEnhanced ? ' [Enhanced]' : '');
          opt.textContent = f.name + tag;
          select.appendChild(opt);
        });
      } catch (err) {
        console.error('Error fetching files:', err);
      }
    }

    async function loadSelectedFile() {
      const select = document.getElementById('fileSelect');
      const filename = select.value;
      if (!filename) return;

      try {
        const res = await fetch('/api/file?name=' + encodeURIComponent(filename));
        const data = await res.json();
        if (data.content) {
          document.getElementById('configContent').value = data.content;
          document.getElementById('saveNotification').style.display = 'none';
        }
      } catch (err) {
        alert('خطا در خواندن فایل: ' + err.message);
      }
    }

    async function startTest() {
      if (isTesting) return;

      const content = document.getElementById('configContent').value.trim();
      const select = document.getElementById('fileSelect');
      const filename = select.value;

      if (!content && !filename) {
        alert('لطفاً ابتدا یک فایل انتخاب کرده یا چند کانفیگ درون کادر متن قرار دهید.');
        return;
      }

      const chainProxy = document.getElementById('chainProxyInput').value;
      const timeoutMs = document.getElementById('timeoutInput').value;
      const concurrency = document.getElementById('concurrencyInput').value;

      isTesting = true;
      const btn = document.getElementById('btnRunTest');
      btn.disabled = true;
      btn.innerHTML = '<span>⏳ در حال تست و ارزیابی نودها...</span>';
      document.getElementById('saveNotification').style.display = 'none';

      try {
        const res = await fetch('/api/test/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            filename: filename || undefined,
            content: content || undefined,
            mode: currentMode,
            chainProxy,
            timeoutMs,
            concurrency
          })
        });

        const data = await res.json();
        if (!res.ok) {
          alert('خطا: ' + (data.error || 'خطایی رخ داد'));
          finishTestUi();
        }
      } catch (err) {
        alert('خطا در برقراری ارتباط با سرور: ' + err.message);
        finishTestUi();
      }
    }

    function finishTestUi() {
      isTesting = false;
      const btn = document.getElementById('btnRunTest');
      btn.disabled = false;
      btn.innerHTML = '<span>🚀 شروع تست و پاک‌سازی</span>';
    }

    // SSE Connection for live progress
    const evtSource = new EventSource('/api/progress-stream');
    evtSource.onmessage = function(e) {
      try {
        const msg = JSON.parse(e.data);
        if (msg.type === 'init') {
          if (msg.state.running) {
            isTesting = true;
            document.getElementById('btnRunTest').disabled = true;
            document.getElementById('btnRunTest').innerHTML = '<span>⏳ در حال تست و ارزیابی نودها...</span>';
          }
          if (msg.state.results && msg.state.results.length > 0) {
            liveResults = [...msg.state.results];
            renderTableResults(liveResults);
            document.getElementById('statTotal').textContent = msg.state.total;
            document.getElementById('statAlive').textContent = msg.state.aliveCount;
            document.getElementById('statDead').textContent = msg.state.deadCount;
          }
        } else if (msg.type === 'start') {
          liveResults = [];
          document.getElementById('statTotal').textContent = msg.total || '0';
          document.getElementById('statAlive').textContent = 0;
          document.getElementById('statDead').textContent = 0;
          document.getElementById('resultsBody').innerHTML = '<tr><td colspan="4" style="text-align:center; color:var(--text-muted); padding: 1.5rem;">در حال آغاز تست کانفیگ‌ها...</td></tr>';
        } else if (msg.type === 'progress') {
          document.getElementById('statTotal').textContent = msg.total;
          document.getElementById('statAlive').textContent = msg.aliveCount;
          document.getElementById('statDead').textContent = msg.deadCount;
          
          const btn = document.getElementById('btnRunTest');
          const pct = Math.round((msg.completed / msg.total) * 100);
          btn.innerHTML = '<span>⏳ در حال تست... (' + msg.completed + '/' + msg.total + ' - ' + pct + '%)</span>';

          if (msg.lastResult) {
            liveResults.push(msg.lastResult);
            renderTableResults(liveResults);
          }
        } else if (msg.type === 'done') {
          finishTestUi();
          document.getElementById('statTotal').textContent = msg.total;
          document.getElementById('statAlive').textContent = msg.alive;
          document.getElementById('statDead').textContent = msg.dead;

          liveResults = msg.results || liveResults;
          renderTableResults(liveResults);

          lastCleanOutput = msg.cleanText || (msg.cleanJson ? JSON.stringify(msg.cleanJson, null, 2) : '');
          lastTestedFile = msg.file || document.getElementById('fileSelect').value || '';

          if (msg.alive === 0) {
            alert('هیچ کانفیگ سالمی در این تست پیدا نشد.');
          }
        } else if (msg.type === 'error') {
          finishTestUi();
          alert('خطا در تست: ' + msg.error);
        }
      } catch (e) {}
    };

    function appendLiveResult(r) {
      const tbody = document.getElementById('resultsBody');
      // If table has initial placeholder text, clear it
      if (tbody.children.length === 1 && tbody.children[0].innerText.includes('در حال آغاز')) {
        tbody.innerHTML = '';
      }

      const flag = r.geo?.flagEmoji || '🌐';
      const geoText = r.geo?.countryCode && r.geo.countryCode !== 'XX' ? (' (' + r.geo.countryCode + ')') : '';
      const name = (r.originalRemark || r.raw.slice(0, 40)) + geoText;
      
      let latencyBadge = '';
      if (r.alive) {
        const latClass = r.latencyMs < 1200 ? 'latency-fast' : (r.latencyMs < 2500 ? 'latency-med' : 'latency-slow');
        latencyBadge = '<span class="' + latClass + '">' + r.latencyMs + ' ms</span>';
      } else {
        latencyBadge = '<span style="color:var(--text-muted);">تایم‌اوت</span>';
      }

      const statusBadge = r.alive 
        ? '<span class="badge badge-alive">سالم</span>' 
        : '<span class="badge badge-dead">سوخته</span>';

      const protoBadge = '<span class="badge badge-proto">' + (r.protocol || 'node').toUpperCase() + '</span>';

      const row = document.createElement('tr');
      row.innerHTML = 
        '<td>' + statusBadge + '</td>' +
        '<td>' + protoBadge + '</td>' +
        '<td style="word-break: break-all; direction: ltr; text-align: left;">' +
          '<span style="margin-right: 6px;">' + flag + '</span> ' + escapeHtml(name) +
        '</td>' +
        '<td style="text-align: left;">' + latencyBadge + '</td>';

      tbody.appendChild(row);
      // auto-scroll to bottom of table during live testing
      const wrap = document.querySelector('.results-table-wrap');
      if (wrap) wrap.scrollTop = wrap.scrollHeight;
    }

    function renderTableResults(results) {
      const tbody = document.getElementById('resultsBody');
      if (!results || results.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;">هیچ کانفیگی یافت نشد.</td></tr>';
        return;
      }

      const sorted = [...results].sort((a, b) => {
        if (a.alive && !b.alive) return -1;
        if (!a.alive && b.alive) return 1;
        return a.latencyMs - b.latencyMs;
      });

      tbody.innerHTML = sorted.map(r => {
        const flag = r.geo?.flagEmoji || '🌐';
        const geoText = r.geo?.countryCode && r.geo.countryCode !== 'XX' ? \` (\${r.geo.countryCode})\` : '';
        const name = (r.originalRemark || r.raw.slice(0, 40)) + geoText;
        
        let latencyBadge = '';
        if (r.alive) {
          const latClass = r.latencyMs < 1200 ? 'latency-fast' : (r.latencyMs < 2500 ? 'latency-med' : 'latency-slow');
          latencyBadge = \`<span class="\${latClass}">\${r.latencyMs} ms</span>\`;
        } else {
          latencyBadge = '<span style="color:var(--text-muted);">تایم‌اوت</span>';
        }

        return \`
          <tr>
            <td>
              \${r.alive 
                ? '<span class="badge badge-alive">سالم</span>' 
                : '<span class="badge badge-dead">سوخته</span>'}
            </td>
            <td><span class="badge badge-proto">\${r.protocol.toUpperCase()}</span></td>
            <td style="word-break: break-all; direction: ltr; text-align: left;">
              <span style="margin-right: 6px;">\${flag}</span> \${escapeHtml(name)}
            </td>
            <td style="text-align: left;">\${latencyBadge}</td>
          </tr>
        \`;
      }).join('');
    }

    function escapeHtml(str) {
      return (str || '').replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    }

    async function saveToOriginalFile() {
      if (!lastCleanOutput) {
        alert('ابتدا تست را اجرا نمایید تا کانفیگ‌های سالم مشخص شوند.');
        return;
      }
      const targetFile = lastTestedFile || document.getElementById('fileSelect').value;
      if (!targetFile) {
        const manualName = prompt('نام فایلی که می‌خواهید درون profiles ذخیره شود را وارد کنید (مثال: mynodes.txt):');
        if (!manualName) return;
        return doSaveFile(manualName.trim(), lastCleanOutput);
      }

      if (!confirm('آیا مطمئن هستید که می‌خواهید کانفیگ‌های سالم را مستقیماً روی فایل اصلی (' + targetFile + ') ذخیره کنید؟')) {
        return;
      }
      await doSaveFile(targetFile, lastCleanOutput);
    }

    async function saveWithSuffixFile() {
      if (!lastCleanOutput) {
        alert('ابتدا تست را اجرا نمایید تا کانفیگ‌های سالم مشخص شوند.');
        return;
      }
      const targetFile = lastTestedFile || document.getElementById('fileSelect').value;
      let newFilename = '';

      if (targetFile) {
        const isJson = targetFile.endsWith('.json');
        const ext = isJson ? '.json' : '.txt';
        const base = targetFile.replace(/\.enhanced\.(txt|json)$/, '').replace(/\.chain\.(txt|json)$/, '').replace(/\.(txt|json)$/, '');
        const suffix = currentMode === 'chain' ? '.chain' : '.enhanced';
        newFilename = base + suffix + ext;
      } else {
        const suffix = currentMode === 'chain' ? '.chain.txt' : '.enhanced.txt';
        newFilename = 'clean-nodes' + suffix;
      }

      const confirmedName = prompt('نام فایل خروجی در پوشه profiles:', newFilename);
      if (!confirmedName) return;

      await doSaveFile(confirmedName.trim(), lastCleanOutput);
    }

    async function doSaveFile(filename, content) {
      try {
        const res = await fetch('/api/file', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ filename, content })
        });
        const data = await res.json();
        if (res.ok) {
          const notif = document.getElementById('saveNotification');
          document.getElementById('savedFileName').textContent = filename;
          notif.style.display = 'block';
          alert('فایل با موفقیت در profiles/' + filename + ' ذخیره شد! ✅');
          await fetchFiles();
          document.getElementById('fileSelect').value = filename;
        } else {
          alert('خطا در ذخیره فایل: ' + (data.error || 'ناشناخته'));
        }
      } catch (err) {
        alert('خطای ارتباط با سرور: ' + err.message);
      }
    }

    /* Cloudflare Management Logic */
    async function fetchCfStatus() {
      try {
        const res = await fetch('/api/cf/status');
        const data = await res.json();
        
        const tokenBadge = document.getElementById('cfTokenStatus');
        if (data.tokenSet) {
          tokenBadge.textContent = 'تنظیم شده (' + data.tokenSnippet + ') ✅';
          tokenBadge.style.color = '#10b981';
        } else {
          tokenBadge.textContent = 'تنظیم نشده یا نامعتبر ❌';
          tokenBadge.style.color = '#ef4444';
        }

        document.getElementById('cfAccountId').textContent = data.accountId || 'مشخص نشده';
        if (data.subSecret) {
          document.getElementById('subSecretInput').value = data.subSecret;
        }
        if (data.chainProxy) {
          const chainInput = document.getElementById('chainProxyInput');
          if (chainInput) {
            chainInput.value = data.chainProxy;
            chainInput.placeholder = data.chainProxy;
          }
        }
      } catch (e) {
        console.error('Failed to load CF status:', e);
      }
    }

    function setCfOutput(text, status = 'انجام شد') {
      const consoleEl = document.getElementById('cfConsoleOutput');
      consoleEl.textContent = text;
      consoleEl.scrollTop = consoleEl.scrollHeight;
      document.getElementById('cfTaskStatus').textContent = status;
    }

    function clearCfLog() {
      document.getElementById('cfConsoleOutput').textContent = '';
      document.getElementById('cfTaskStatus').textContent = 'آماده به کار';
    }

    async function checkWhoami() {
      setCfOutput('در حال بررسی احراز هویت با Cloudflare API...\\nلطفاً چند لحظه صبر کنید...', 'در حال پردازش...');
      try {
        const res = await fetch('/api/cf/whoami', { method: 'POST' });
        const data = await res.json();
        setCfOutput(data.output, res.ok ? 'احراز هویت موفق' : 'خطا در احراز هویت');
      } catch (e) {
        setCfOutput('خطا در ارتباط با سرور: ' + e.message, 'خطا');
      }
    }

    async function setSubSecret() {
      const secret = document.getElementById('subSecretInput').value.trim();
      if (!secret) {
        alert('لطفاً مقدار Secret را وارد کنید.');
        return;
      }
      setCfOutput('در حال ثبت SUB_SECRET در Cloudflare Workers و ذخیره در .env...', 'در حال پردازش...');
      try {
        const res = await fetch('/api/cf/secret', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ secret })
        });
        const data = await res.json();
        if (res.ok) {
          setCfOutput(data.output + '\\n✔ سکرت با موفقیت ذخیره شد!', 'موفقیت‌آمیز');
          alert('سکرت SUB_SECRET با موفقیت تنظیم شد!');
        } else {
          setCfOutput(data.output || data.error, 'خطا');
          alert('خطا در ثبت سکرت: ' + (data.output || data.error));
        }
      } catch (e) {
        setCfOutput('خطا: ' + e.message, 'خطا');
      }
    }

    async function rebuildProfiles() {
      setCfOutput('در حال بازسازی src/generated-profiles.ts از روی فایل‌های profiles/...', 'در حال همگام‌سازی...');
      try {
        const res = await fetch('/api/cf/generate-profiles', { method: 'POST' });
        const data = await res.json();
        setCfOutput(data.output, res.ok ? 'همگام‌سازی انجام شد' : 'خطا در همگام‌سازی');
        fetchFiles();
      } catch (e) {
        setCfOutput('خطا: ' + e.message, 'خطا');
      }
    }

    async function deployToCloudflare() {
      const btn = document.getElementById('btnDeploy');
      btn.disabled = true;
      btn.innerHTML = '<span>⏳ در حال آماده‌سازی و دیپلوی به Cloudflare...</span>';
      setCfOutput('1. همگام‌سازی پروفایل‌ها و کانفیگ‌ها...\\n2. آغاز Wrangler Deploy...\\nلطفاً چند لحظه شکیبا باشید...', 'در حال دیپلوی...');

      try {
        const res = await fetch('/api/cf/deploy', { method: 'POST' });
        const data = await res.json();
        if (res.ok) {
          setCfOutput(data.output + '\\n\\n🎉 دیپلوی با موفقیت به پایان رسید!', 'دیپلوی موفق');
        } else {
          setCfOutput(data.output || 'خطا در فرآیند دیپلوی', 'خطا در دیپلوی');
        }
      } catch (e) {
        setCfOutput('خطای شبکه: ' + e.message, 'خطا');
      } finally {
        btn.disabled = false;
        btn.innerHTML = '<span>🚀 دیپلوی سابسکریپشن به کلاودفلر (Deploy)</span>';
      }
    }

    fetchFiles();
    fetchCfStatus();
  </script>
</body>
</html>`;
}

server.listen(PORT, '127.0.0.1', () => {
  console.log(`\n======================================================`);
  console.log(`🚀 Cloudflare Sub Manager & Tester GUI is running!`);
  console.log(`🌐 Open in browser: http://localhost:${PORT}`);
  console.log(`======================================================\n`);
});
