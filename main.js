/**
 * Benny's Hub - Electron Main Process
 * 
 * Handles all backend operations:
 * - File I/O for keyboard predictions, journal entries
 * - Launching external Python apps (messenger, search)
 * - Window management
 * - Streaming platform automation
 * - Local HTTP server for YouTube embeds and other HTTP-dependent features
 */

const { app, BrowserWindow, ipcMain, shell, session } = require('electron');
const path = require('path');
const fs = require('fs');
const http = require('http');
const { spawn, exec } = require('child_process');

// Paths
const APP_DIR = __dirname;
const BENNYSHUB_DIR = path.join(APP_DIR, 'bennyshub');
const DATA_DIR = path.join(APP_DIR, 'data');
const APPS_DIR = path.join(BENNYSHUB_DIR, 'apps', 'tools');

// Data file paths
const KEYBOARD_PREDICTIONS_PATH = path.join(APPS_DIR, 'keyboard', 'web_keyboard_predictions.json');
const JOURNAL_ENTRIES_PATH = path.join(APPS_DIR, 'journal', 'entries.json');
const JOURNAL_QUESTIONS_PATH = path.join(APPS_DIR, 'journal', 'questions.json');
const STREAMING_DIR = path.join(APPS_DIR, 'streaming');
const STREAMING_DATA_DIR = path.join(STREAMING_DIR, 'data');
const STREAMING_DATA_JSON_PATH = path.join(STREAMING_DIR, 'data.json');
const STREAMING_EPISODES_PATH = path.join(STREAMING_DIR, 'episodes.json');
const STREAMING_LAST_WATCHED_PATH = path.join(STREAMING_DATA_DIR, 'last_watched.json');
const STREAMING_SEARCH_HISTORY_PATH = path.join(STREAMING_DIR, 'search_history.json');

// Shared settings paths
const VOICE_SETTINGS_PATH = path.join(BENNYSHUB_DIR, 'shared', 'voice-settings.json');

// External Python scripts
const MESSENGER_SCRIPT = path.join(APPS_DIR, 'messenger', 'ben_discord_app.py');
const SEARCH_SCRIPT = path.join(APPS_DIR, 'search', 'narbe_scan_browser.py');
const DM_LISTENER_SCRIPT = path.join(APPS_DIR, 'messenger', 'simple_dm_listener.py');
const CONTROL_BAR_SCRIPT = path.join(APPS_DIR, 'streaming', 'utils', 'control_bar.py');
const YTSEARCH_SERVER_SCRIPT = path.join(APPS_DIR, 'ytsearch', 'server.py');

// Chrome path
const CHROME_PATH = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';

// HARDWARE ACCELERATION - Enabled for WebGL games (Dice, Basketball, Bowling)
// Only disable if non-3D games are crashing
// app.disableHardwareAcceleration();
// app.commandLine.appendSwitch('disable-gpu');
// app.commandLine.appendSwitch('disable-software-rasterizer');

let mainWindow;
let dmListenerProcess = null;
let ytsearchServerProcess = null;
let hubServer = null;
let hubServerPort = 8765;

// Ensure data directories exist
function ensureDataDirs() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
  if (!fs.existsSync(STREAMING_DATA_DIR)) {
    fs.mkdirSync(STREAMING_DATA_DIR, { recursive: true });
  }
}

// ============ LOCAL HTTP SERVER ============
// Serves the hub via localhost so YouTube embeds and other HTTP features work properly

const MIME_TYPES = {
  '.html': 'text/html',
  '.js': 'application/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.mp3': 'audio/mpeg',
  '.wav': 'audio/wav',
  '.ogg': 'audio/ogg',
  '.mp4': 'video/mp4',
  '.webm': 'video/webm',
  '.webp': 'image/webp'
};

// API Proxy configuration for external services (bypass CORS)
const API_PROXY_SERVICES = {
  'tmdb': 'https://api.themoviedb.org',
  'opensymbols': 'https://www.opensymbols.org/api/v1',
  'freesound': 'https://api.freesound.org',
  'freesound-proxy': 'https://aged-thunder-a674.narbehousellc.workers.dev'
};

// Handle streaming data save endpoint
function handleSaveStreamingData(req, res) {
  let body = '';
  req.on('data', chunk => body += chunk);
  req.on('end', () => {
    try {
      const data = JSON.parse(body);
      const dataPath = path.join(STREAMING_DIR, 'data.json');
      fs.writeFileSync(dataPath, JSON.stringify(data, null, 2), 'utf8');
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ success: true }));
      console.log('[HUB-SERVER] Saved streaming data.json');
    } catch (error) {
      console.error('[HUB-SERVER] Error saving streaming data:', error);
      res.writeHead(500, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ error: error.message }));
    }
  });
}

// Handle streaming genres save endpoint
function handleSaveStreamingGenres(req, res) {
  let body = '';
  req.on('data', chunk => body += chunk);
  req.on('end', () => {
    try {
      const data = JSON.parse(body);
      const genresPath = path.join(STREAMING_DIR, 'genres.json');
      fs.writeFileSync(genresPath, JSON.stringify(data, null, 2), 'utf8');
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ success: true }));
      console.log('[HUB-SERVER] Saved streaming genres.json');
    } catch (error) {
      console.error('[HUB-SERVER] Error saving streaming genres:', error);
      res.writeHead(500, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ error: error.message }));
    }
  });
}

// Handle API proxy requests
function handleApiProxy(req, res, urlPath, queryString) {
  const https = require('https');
  
  // Parse path: /api/proxy/<service>/<path>
  const parts = urlPath.split('/').filter(p => p);
  if (parts.length < 3) {
    res.writeHead(400, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ error: 'Invalid proxy URL. Use /api/proxy/<service>/<path>' }));
    return;
  }
  
  const service = parts[2].toLowerCase();
  const apiPath = parts.slice(3).join('/');
  
  const baseUrl = API_PROXY_SERVICES[service];
  if (!baseUrl) {
    res.writeHead(400, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ error: `Unknown service: ${service}. Supported: tmdb, opensymbols, freesound, freesound-proxy` }));
    return;
  }
  
  let targetUrl = `${baseUrl}/${apiPath}`;
  if (queryString) {
    targetUrl += `?${queryString}`;
  }
  
  console.log(`[API-PROXY] ${req.method} ${service} -> ${targetUrl}`);
  
  const parsedUrl = new URL(targetUrl);
  const options = {
    hostname: parsedUrl.hostname,
    port: 443,
    path: parsedUrl.pathname + parsedUrl.search,
    method: req.method,
    headers: {
      'User-Agent': 'BennysHub/1.0',
      'Accept': 'application/json'
    },
    rejectUnauthorized: false // Allow self-signed certs
  };
  
  const proxyReq = https.request(options, (proxyRes) => {
    const chunks = [];
    proxyRes.on('data', chunk => chunks.push(chunk));
    proxyRes.on('end', () => {
      const body = Buffer.concat(chunks);
      res.writeHead(proxyRes.statusCode, {
        'Content-Type': proxyRes.headers['content-type'] || 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
      });
      res.end(body);
    });
  });
  
  proxyReq.on('error', (err) => {
    console.error('[API-PROXY] Error:', err.message);
    res.writeHead(502, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ error: `Proxy error: ${err.message}` }));
  });
  
  // Forward POST body if present
  if (req.method === 'POST' || req.method === 'PUT') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      if (body) {
        proxyReq.setHeader('Content-Type', req.headers['content-type'] || 'application/json');
        proxyReq.setHeader('Content-Length', Buffer.byteLength(body));
        proxyReq.write(body);
      }
      proxyReq.end();
    });
  } else {
    proxyReq.end();
  }
}

function startHubServer() {
  return new Promise((resolve, reject) => {
    hubServer = http.createServer((req, res) => {
      // Parse URL and decode it
      const urlParts = req.url.split('?');
      let urlPath = decodeURIComponent(urlParts[0]);
      const queryString = urlParts[1] || '';
      
      // Handle CORS preflight
      if (req.method === 'OPTIONS') {
        res.writeHead(200, {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type'
        });
        res.end();
        return;
      }
      
      // Handle API proxy requests
      if (urlPath.startsWith('/api/proxy/')) {
        handleApiProxy(req, res, urlPath, queryString);
        return;
      }
      
      // Handle streaming editor save endpoints
      if (urlPath === '/api/save-data' && req.method === 'POST') {
        handleSaveStreamingData(req, res);
        return;
      }
      
      if (urlPath === '/api/save-genres' && req.method === 'POST') {
        handleSaveStreamingGenres(req, res);
        return;
      }
      
      if (urlPath === '/') urlPath = '/index.html';
      
      // Security: prevent directory traversal
      const safePath = path.normalize(urlPath).replace(/^(\.\.[\/\\])+/, '');
      const filePath = path.join(BENNYSHUB_DIR, safePath);
      
      // Check if file exists
      if (!fs.existsSync(filePath)) {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('404 Not Found');
        return;
      }
      
      // Check if it's a directory
      const stat = fs.statSync(filePath);
      if (stat.isDirectory()) {
        const indexPath = path.join(filePath, 'index.html');
        if (fs.existsSync(indexPath)) {
          serveFile(indexPath, res);
        } else {
          res.writeHead(403, { 'Content-Type': 'text/plain' });
          res.end('403 Forbidden');
        }
        return;
      }
      
      serveFile(filePath, res);
    });
    
    function serveFile(filePath, res) {
      const ext = path.extname(filePath).toLowerCase();
      const contentType = MIME_TYPES[ext] || 'application/octet-stream';
      
      fs.readFile(filePath, (err, data) => {
        if (err) {
          res.writeHead(500, { 'Content-Type': 'text/plain' });
          res.end('500 Internal Server Error');
          return;
        }
        
        res.writeHead(200, { 
          'Content-Type': contentType,
          'Access-Control-Allow-Origin': '*'
        });
        res.end(data);
      });
    }
    
    // Try to start on preferred port, fall back to alternatives
    const tryPort = (port) => {
      hubServer.once('error', (err) => {
        if (err.code === 'EADDRINUSE' && port < 8800) {
          console.log(`[HUB-SERVER] Port ${port} in use, trying ${port + 1}`);
          tryPort(port + 1);
        } else {
          reject(err);
        }
      });
      
      hubServer.listen(port, '127.0.0.1', () => {
        hubServerPort = port;
        console.log(`[HUB-SERVER] Running at http://127.0.0.1:${hubServerPort}`);
        resolve(hubServerPort);
      });
    };
    
    tryPort(hubServerPort);
  });
}

// ============ WINDOW MANAGEMENT ============

async function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1920,
    height: 1080,
    fullscreen: true,
    frame: false,
    backgroundColor: '#1a1a2e',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      nodeIntegration: false,
      contextIsolation: true,
      webSecurity: true,
      // Allow preload script to work in iframes (same origin)
      nodeIntegrationInSubFrames: false,
      // This allows iframes to access the parent's electronAPI
      sandbox: false,
      // Allow mixed content for YouTube embeds
      allowRunningInsecureContent: true
    }
  });

  // Grant all permissions for media playback (YouTube embeds)
  mainWindow.webContents.session.setPermissionRequestHandler((webContents, permission, callback) => {
    const allowedPermissions = ['media', 'mediaKeySystem', 'fullscreen', 'geolocation', 'notifications', 'midi', 'midiSysex', 'pointerLock', 'openExternal'];
    if (allowedPermissions.includes(permission)) {
      callback(true);
    } else {
      console.log(`[PERMISSION] Denied: ${permission}`);
      callback(false);
    }
  });

  // Also handle permission checks (synchronous)
  mainWindow.webContents.session.setPermissionCheckHandler((webContents, permission, requestingOrigin, details) => {
    // Allow all media-related permissions, especially for YouTube
    if (permission === 'media' || permission === 'mediaKeySystem' || permission === 'fullscreen') {
      return true;
    }
    // Allow for localhost and YouTube domains
    if (requestingOrigin.includes('127.0.0.1') || requestingOrigin.includes('youtube.com') || requestingOrigin.includes('googlevideo.com')) {
      return true;
    }
    return true; // Allow all by default
  });

  // Inject preload into all frames (including iframes)
  mainWindow.webContents.on('did-attach-webview', (event, webContents) => {
    webContents.session.setPreloads([path.join(__dirname, 'preload.js')]);
  });

  // Load via localhost server for YouTube embeds to work
  // This gives us a proper HTTP origin instead of file://
  const serverUrl = `http://127.0.0.1:${hubServerPort}/index.html`;
  console.log(`[HUB] Loading main window from: ${serverUrl}`);
  mainWindow.loadURL(serverUrl);
  
  // CRASH HANDLERS - Log renderer crashes
  mainWindow.webContents.on('render-process-gone', (event, details) => {
    console.error('!!! RENDERER CRASHED !!!');
    console.error('Reason:', details.reason);
    console.error('Exit code:', details.exitCode);
    // Write to file for persistence
    const crashLog = `[${new Date().toISOString()}] Renderer crashed: ${details.reason} (exit: ${details.exitCode})\n`;
    fs.appendFileSync(path.join(__dirname, 'crash.log'), crashLog);
  });

  mainWindow.webContents.on('crashed', (event, killed) => {
    console.error('!!! WEBCONTENTS CRASHED !!!', killed ? '(killed)' : '');
  });

  mainWindow.webContents.on('unresponsive', () => {
    console.error('!!! RENDERER UNRESPONSIVE !!!');
  });

  // Focus window ONLY on initial startup - use 'once' to prevent stealing focus later
  // This prevents focus issues when apps run in iframes (editors, games, etc.)
  let initialLoadComplete = false;
  mainWindow.webContents.once('did-finish-load', () => {
    if (initialLoadComplete) return;
    initialLoadComplete = true;
    
    const focusWindow = () => {
      if (mainWindow && !initialLoadComplete) return; // Double-check flag
      if (mainWindow) {
        mainWindow.show();
        mainWindow.focus();
        mainWindow.setFullScreen(true);
        // Don't focus webContents - let the page handle its own focus
      }
    };
    
    // Immediate focus on startup
    focusWindow();
    
    // A few delayed attempts for startup only, then stop
    const timeouts = [
      setTimeout(focusWindow, 500),
      setTimeout(focusWindow, 1000),
      setTimeout(focusWindow, 2000)
    ];
    
    // Clear all focus attempts after 3 seconds to prevent interference
    setTimeout(() => {
      timeouts.forEach(t => clearTimeout(t));
    }, 3000);
  });

  // Note: Removed aggressive blur handler - it was causing focus fighting
  // with external Python apps (messenger, search). The window will stay
  // minimized when external apps are running, and restore when they close.

  // Handle window close
  mainWindow.on('closed', () => {
    mainWindow = null;
    // Clean up DM listener
    if (dmListenerProcess) {
      dmListenerProcess.kill();
    }
  });

  // Open DevTools in development (disabled for production)
  // mainWindow.webContents.openDevTools();
}

app.whenReady().then(async () => {
  ensureDataDirs();
  
  // Start the local HTTP server FIRST - this is required for YouTube embeds to work
  try {
    await startHubServer();
    console.log('[HUB] Local server started successfully');
  } catch (err) {
    console.error('[HUB] Failed to start local server:', err);
    // Continue anyway - app will work but YouTube embeds may not
  }
  
  // Configure Content Security Policy to allow CDN resources for games
  // This allows Three.js, Cannon.js, YouTube player, localhost servers, search APIs, and other external libraries
  session.defaultSession.webRequest.onHeadersReceived((details, callback) => {
    // Don't modify headers for YouTube-related domains - let them handle their own CSP
    const youtubeUrls = ['youtube.com', 'ytimg.com', 'googlevideo.com', 'google.com', 'gstatic.com', 'ggpht.com'];
    const url = details.url.toLowerCase();
    if (youtubeUrls.some(domain => url.includes(domain))) {
      callback({ responseHeaders: details.responseHeaders });
      return;
    }
    
    callback({
      responseHeaders: {
        ...details.responseHeaders,
        'Content-Security-Policy': [
          "default-src 'self' 'unsafe-inline' 'unsafe-eval' 'wasm-unsafe-eval' data: blob: http://localhost:* http://127.0.0.1:*; " +
          "script-src 'self' 'unsafe-inline' 'unsafe-eval' 'wasm-unsafe-eval' https://cdnjs.cloudflare.com https://unpkg.com https://www.googletagmanager.com https://www.google-analytics.com https://www.youtube.com https://s.ytimg.com https://www.google.com https://challenges.cloudflare.com http://localhost:* http://127.0.0.1:* blob:; " +
          "script-src-elem 'self' 'unsafe-inline' 'unsafe-eval' 'wasm-unsafe-eval' https://cdnjs.cloudflare.com https://unpkg.com https://www.youtube.com https://s.ytimg.com https://www.google.com https://challenges.cloudflare.com http://localhost:* http://127.0.0.1:* blob:; " +
          "connect-src 'self' https://unpkg.com https://cdnjs.cloudflare.com https://www.google-analytics.com https://*.googleapis.com https://*.workers.dev https://www.youtube.com https://www.google.com https://challenges.cloudflare.com https://api.duckduckgo.com https://*.wikipedia.org http://localhost:* http://127.0.0.1:* wss: ws:; " +
          "img-src 'self' data: blob: https: http://localhost:* http://127.0.0.1:*; " +
          "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com http://localhost:* http://127.0.0.1:*; " +
          "font-src 'self' data: https://fonts.gstatic.com; " +
          "worker-src 'self' blob:; " +
          "media-src 'self' data: blob: https://*.googlevideo.com https://*.youtube.com http://localhost:* http://127.0.0.1:*; " +
          "frame-src 'self' blob: https://www.youtube.com https://www.youtube-nocookie.com https://challenges.cloudflare.com http://localhost:* http://127.0.0.1:*;"
        ]
      }
    });
  });
  
  await createWindow();
  
  // Start DM listener in background
  startDMListener();
  
  // Start navigation signal watcher for control bar communication
  startNavSignalWatcher();
  
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on('window-all-closed', () => {
  // Kill tracked Python processes
  if (dmListenerProcess) {
    dmListenerProcess.kill();
    dmListenerProcess = null;
  }
  if (ytsearchServerProcess) {
    ytsearchServerProcess.kill();
    ytsearchServerProcess = null;
  }
  if (editorServerProcess) {
    editorServerProcess.kill();
    editorServerProcess = null;
  }
  
  // Close the hub server
  if (hubServer) {
    hubServer.close();
    hubServer = null;
  }
  
  // Force kill any remaining Python processes
  if (process.platform === 'win32') {
    exec('taskkill /F /IM python.exe', () => {});
  }
  
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

// ============ HELPER FUNCTIONS ============

function loadJSON(filePath, defaultValue = {}) {
  try {
    if (fs.existsSync(filePath)) {
      const data = fs.readFileSync(filePath, 'utf8');
      return JSON.parse(data);
    }
  } catch (e) {
    console.error(`Error loading ${filePath}:`, e);
  }
  return defaultValue;
}

function saveJSON(filePath, data) {
  try {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
    return true;
  } catch (e) {
    console.error(`Error saving ${filePath}:`, e);
    return false;
  }
}

function startDMListener() {
  if (fs.existsSync(DM_LISTENER_SCRIPT)) {
    try {
      dmListenerProcess = spawn('python', [DM_LISTENER_SCRIPT], {
        cwd: path.dirname(DM_LISTENER_SCRIPT),
        stdio: 'ignore',
        detached: false,
        windowsHide: true
      });
      console.log('[DM-LISTENER] Started');
    } catch (e) {
      console.error('[DM-LISTENER] Failed to start:', e);
    }
  }
}

// Navigation signal file path - control_bar.py writes here to request navigation
const NAV_SIGNAL_PATH = path.join(BENNYSHUB_DIR, 'nav_signal.json');
let lastNavTimestamp = 0;

function startNavSignalWatcher() {
  // Poll the navigation signal file every 300ms
  setInterval(() => {
    try {
      if (fs.existsSync(NAV_SIGNAL_PATH)) {
        const data = fs.readFileSync(NAV_SIGNAL_PATH, 'utf8');
        const signal = JSON.parse(data);
        
        if (signal.timestamp && signal.timestamp > lastNavTimestamp) {
          lastNavTimestamp = signal.timestamp;
          console.log('[NAV-SIGNAL] Received:', signal);
          
          // Restore and focus the main window, ensure fullscreen
          if (mainWindow) {
            mainWindow.restore();
            mainWindow.focus();
            mainWindow.show();
            mainWindow.setFullScreen(true);  // Always restore fullscreen
            
            // Send navigation event to renderer
            mainWindow.webContents.send('nav-signal', signal);
          }
          
          // Delete the signal file after processing
          try {
            fs.unlinkSync(NAV_SIGNAL_PATH);
          } catch (e) {
            // Ignore deletion errors
          }
        }
      }
    } catch (e) {
      // Signal file doesn't exist or invalid JSON - that's fine
    }
  }, 300);
}

// ============ VOICE SETTINGS API ============
// Provides centralized voice settings storage that syncs across all apps

const DEFAULT_VOICE_SETTINGS = {
  ttsEnabled: true,
  voiceIndex: 0,
  voiceName: null,
  rate: 1.0,
  pitch: 1.0,
  volume: 1.0
};

ipcMain.handle('voice:getSettings', async () => {
  return loadJSON(VOICE_SETTINGS_PATH, DEFAULT_VOICE_SETTINGS);
});

ipcMain.handle('voice:saveSettings', async (event, settings) => {
  const result = saveJSON(VOICE_SETTINGS_PATH, settings);
  
  // Broadcast to all windows/webContents
  if (mainWindow && mainWindow.webContents) {
    mainWindow.webContents.send('voice-settings-changed', settings);
    
    // Also send to all iframes
    mainWindow.webContents.executeJavaScript(`
      (function() {
        const iframes = document.querySelectorAll('iframe');
        iframes.forEach(iframe => {
          try {
            if (iframe.contentWindow) {
              iframe.contentWindow.postMessage({
                type: 'narbe-voice-settings-changed',
                settings: ${JSON.stringify(settings)}
              }, '*');
            }
          } catch(e) {}
        });
      })();
    `).catch(() => {});
  }
  
  return result;
});

// ============ KEYBOARD API ============

ipcMain.handle('keyboard:getPredictions', async () => {
  return loadJSON(KEYBOARD_PREDICTIONS_PATH, { frequent_words: {}, bigrams: {}, trigrams: {} });
});

ipcMain.handle('keyboard:savePrediction', async (event, { word, timestamp }) => {
  const data = loadJSON(KEYBOARD_PREDICTIONS_PATH, { frequent_words: {}, bigrams: {}, trigrams: {} });
  
  if (!data.frequent_words) data.frequent_words = {};
  if (!data.frequent_words[word]) {
    data.frequent_words[word] = { count: 0 };
  }
  data.frequent_words[word].count++;
  data.frequent_words[word].last_used = timestamp;
  
  return saveJSON(KEYBOARD_PREDICTIONS_PATH, data);
});

ipcMain.handle('keyboard:saveNgram', async (event, { context, next_word, timestamp }) => {
  const data = loadJSON(KEYBOARD_PREDICTIONS_PATH, { frequent_words: {}, bigrams: {}, trigrams: {} });
  
  const words = context.trim().split(/\s+/);
  const key = words.join(' ').toUpperCase();
  const nextUpper = next_word.toUpperCase();
  
  if (words.length === 1) {
    if (!data.bigrams) data.bigrams = {};
    if (!data.bigrams[key]) data.bigrams[key] = {};
    if (!data.bigrams[key][nextUpper]) {
      data.bigrams[key][nextUpper] = { count: 0 };
    }
    data.bigrams[key][nextUpper].count++;
    data.bigrams[key][nextUpper].last_used = timestamp;
  } else if (words.length === 2) {
    if (!data.trigrams) data.trigrams = {};
    if (!data.trigrams[key]) data.trigrams[key] = {};
    if (!data.trigrams[key][nextUpper]) {
      data.trigrams[key][nextUpper] = { count: 0 };
    }
    data.trigrams[key][nextUpper].count++;
    data.trigrams[key][nextUpper].last_used = timestamp;
  }
  
  return saveJSON(KEYBOARD_PREDICTIONS_PATH, data);
});

ipcMain.handle('keyboard:clearPredictions', async () => {
  const defaultData = { frequent_words: {}, bigrams: {}, trigrams: {} };
  return saveJSON(KEYBOARD_PREDICTIONS_PATH, defaultData);
});

// ============ JOURNAL API ============

ipcMain.handle('journal:getEntries', async () => {
  return loadJSON(JOURNAL_ENTRIES_PATH, { entries: [] });
});

ipcMain.handle('journal:saveEntries', async (event, data) => {
  return saveJSON(JOURNAL_ENTRIES_PATH, data);
});

ipcMain.handle('journal:getQuestions', async () => {
  return loadJSON(JOURNAL_QUESTIONS_PATH, { questions: [] });
});

// ============ STREAMING API ============

// Episode cache (loaded from episodes.json)
let episodeCache = null;

function loadEpisodeCache() {
  if (episodeCache !== null) return episodeCache;
  
  try {
    // Load from episodes.json
    episodeCache = loadJSON(STREAMING_EPISODES_PATH, {});
    const showCount = Object.keys(episodeCache).length;
    let episodeCount = 0;
    for (const show of Object.keys(episodeCache)) {
      for (const season of Object.keys(episodeCache[show])) {
        episodeCount += episodeCache[show][season].length;
      }
    }
    console.log(`[STREAMING] Loaded ${episodeCount} episodes for ${showCount} shows from episodes.json`);
  } catch (e) {
    console.error('[STREAMING] Error loading episodes:', e);
    episodeCache = {};
  }
  
  return episodeCache || {};
}

ipcMain.handle('streaming:getData', async () => {
  return loadJSON(STREAMING_DATA_JSON_PATH, []);
});

ipcMain.handle('streaming:getEpisodes', async (event, showTitle) => {
  const cache = loadEpisodeCache();
  if (showTitle) {
    const key = showTitle.toLowerCase().trim();
    return cache[key] || {};
  }
  return cache;
});

ipcMain.handle('streaming:getLastWatched', async (event, showTitle) => {
  const data = loadJSON(STREAMING_LAST_WATCHED_PATH, {});
  if (showTitle) {
    const key = showTitle.toLowerCase().trim();
    return data[key] || null;
  }
  return data;
});

ipcMain.handle('streaming:saveProgress', async (event, { show, season, episode, url }) => {
  const data = loadJSON(STREAMING_LAST_WATCHED_PATH, {});
  const key = show.toLowerCase().trim();
  data[key] = { season, episode, url, timestamp: Date.now() };
  return saveJSON(STREAMING_LAST_WATCHED_PATH, data);
});

ipcMain.handle('streaming:getSearchHistory', async () => {
  return loadJSON(STREAMING_SEARCH_HISTORY_PATH, []);
});

ipcMain.handle('streaming:saveSearch', async (event, term) => {
  let history = loadJSON(STREAMING_SEARCH_HISTORY_PATH, []);
  term = term.trim();
  if (term) {
    // Remove existing duplicate
    history = history.filter(h => h.toLowerCase() !== term.toLowerCase());
    // Add to front
    history.unshift(term);
    // Keep max 50
    history = history.slice(0, 50);
    saveJSON(STREAMING_SEARCH_HISTORY_PATH, history);
  }
  return history;
});

ipcMain.handle('streaming:clearSearchHistory', async () => {
  return saveJSON(STREAMING_SEARCH_HISTORY_PATH, []);
});

ipcMain.handle('streaming:launch', async (event, { url, title, type, showTitle }) => {
  try {
    // showTitle is the base show name (e.g. "Breaking Bad"), title may include S#E# suffix
    const controlBarTitle = showTitle || title;
    console.log(`[STREAMING] Launching: ${title} | ${url} | ${type} | controlBar: ${controlBarTitle}`);
    
    // Launch Chrome with the URL and remote debugging for control_bar.py automation
    const args = ['--new-window', '--start-fullscreen', '--remote-debugging-port=9222', url];
    
    const chromeProcess = spawn(CHROME_PATH, args, {
      detached: true,
      stdio: 'ignore'
    });
    chromeProcess.unref();
    
    // Minimize Electron window so Chrome takes focus
    if (mainWindow) {
      mainWindow.minimize();
    }
    
    // Determine delay for control bar based on platform
    // Control bar launches early for user interaction, then sends automation keys after 3s
    let delay = 5000; // Default - page should be loading
    if (url.includes('plex.tv') || url.includes('plex.direct') || url.includes(':32400')) {
      delay = 10000; // Plex needs more time to load
    } else if (url.includes('pluto.tv')) {
      delay = 12000; // PlutoTV is slow
    } else if (url.includes('youtube.com') || url.includes('youtu.be')) {
      delay = 6000; // YouTube loads faster
    }
    
    // Launch control bar after delay - it handles automation via _bootstrap_once()
    // Use controlBarTitle (base show name) so it can find the URL in last_watched.json
    setTimeout(() => {
      launchControlBar('basic', controlBarTitle);
    }, delay);
    
    return { success: true };
  } catch (e) {
    console.error('[STREAMING] Launch error:', e);
    return { success: false, error: e.message };
  }
});

function launchControlBar(mode, showTitle) {
  if (fs.existsSync(CONTROL_BAR_SCRIPT)) {
    const args = [CONTROL_BAR_SCRIPT, '--mode', mode, '--app-title', 'Streaming Hub'];
    if (showTitle) {
      args.push('--show', showTitle);
    }
    
    spawn('python', args, {
      cwd: path.dirname(CONTROL_BAR_SCRIPT),
      detached: true,
      stdio: 'ignore'
    });
  }
}

// ============ EXTERNAL APP LAUNCHERS ============

ipcMain.handle('launch:messenger', async () => {
  try {
    if (fs.existsSync(MESSENGER_SCRIPT)) {
      spawn('python', [MESSENGER_SCRIPT], {
        cwd: path.dirname(MESSENGER_SCRIPT),
        detached: true,
        stdio: 'ignore'
      });
      // Don't minimize - let the messenger app take focus naturally
      // Electron stays fullscreen in the background
      return { success: true };
    }
    return { success: false, error: 'Messenger script not found' };
  } catch (e) {
    return { success: false, error: e.message };
  }
});

ipcMain.handle('launch:search', async () => {
  try {
    if (fs.existsSync(SEARCH_SCRIPT)) {
      spawn('python', [SEARCH_SCRIPT], {
        cwd: path.dirname(SEARCH_SCRIPT),
        detached: true,
        stdio: 'ignore'
      });
      // Do NOT minimize Electron for search app, as requested
      // This allows it to stay in background and be restored more easily
      return { success: true };
    }
    return { success: false, error: 'Search script not found' };
  } catch (e) {
    return { success: false, error: e.message };
  }
});

// ============ EDITOR LAUNCHER ============
// Launches editors in Chrome browser via localhost server for proper mouse/keyboard support

const EDITOR_SERVER_SCRIPT = path.join(BENNYSHUB_DIR, 'shared', 'editor_server.py');
let editorServerProcess = null;
let editorServerPort = null;

// Available editors
const EDITOR_PATHS = {
  streaming: { path: 'apps/tools/streaming', file: 'editor.html' },
  triviamaster: { path: 'apps/games/TRIVIAMASTER/trivia editor', file: 'index.html' },
  trivia: { path: 'apps/games/TRIVIAMASTER/trivia editor', file: 'index.html' },
  golf: { path: 'apps/games/BENNYSMINIGOLF/COURSE CREATOR', file: 'index.html' },
  minigolf: { path: 'apps/games/BENNYSMINIGOLF/COURSE CREATOR', file: 'index.html' },
  matchymatch: { path: 'apps/games/BENNYSMATCHYMATCH', file: 'editor.html' },
  matchy: { path: 'apps/games/BENNYSMATCHYMATCH', file: 'editor.html' },
  wordjumble: { path: 'apps/games/BENNYSWORDJUMBLE', file: 'editor.html' },
  jumble: { path: 'apps/games/BENNYSWORDJUMBLE', file: 'editor.html' },
  phraseboard: { path: 'apps/tools/phraseboard', file: 'phrase-builder.html' },
  phrase: { path: 'apps/tools/phraseboard', file: 'phrase-builder.html' },
  peggle: { path: 'apps/games/BENNYSPEGGLE', file: 'editor.html' },
};

// Find a free port
async function findFreePort(start = 8800, end = 8900) {
  const net = require('net');
  for (let port = start; port < end; port++) {
    const available = await new Promise((resolve) => {
      const tester = net.createServer()
        .once('error', () => resolve(false))
        .once('listening', () => {
          tester.close();
          resolve(true);
        })
        .listen(port, '127.0.0.1');
    });
    if (available) return port;
  }
  return null;
}

// Start the editor server (if not already running)
async function startEditorServer() {
  if (editorServerProcess && editorServerPort) {
    // Check if server is still responding
    const net = require('net');
    const isRunning = await new Promise((resolve) => {
      const client = net.createConnection({ port: editorServerPort, host: '127.0.0.1' }, () => {
        client.end();
        resolve(true);
      });
      client.on('error', () => resolve(false));
    });
    
    if (isRunning) {
      console.log(`[EDITOR-SERVER] Already running on port ${editorServerPort}`);
      return editorServerPort;
    }
  }
  
  // Find a free port
  editorServerPort = await findFreePort();
  if (!editorServerPort) {
    console.error('[EDITOR-SERVER] Could not find a free port');
    return null;
  }
  
  // Start the Python server
  if (fs.existsSync(EDITOR_SERVER_SCRIPT)) {
    editorServerProcess = spawn('python', [EDITOR_SERVER_SCRIPT, '--port', editorServerPort.toString(), '--no-browser'], {
      cwd: path.dirname(EDITOR_SERVER_SCRIPT),
      detached: false,
      stdio: 'pipe',
      windowsHide: true
    });
    
    editorServerProcess.on('error', (err) => {
      console.error('[EDITOR-SERVER] Error:', err);
    });
    
    editorServerProcess.stdout.on('data', (data) => {
      console.log(`[EDITOR-SERVER] ${data}`);
    });
    
    editorServerProcess.stderr.on('data', (data) => {
      console.log(`[EDITOR-SERVER] ${data}`);
    });
    
    // Wait for server to start
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    console.log(`[EDITOR-SERVER] Started on port ${editorServerPort}`);
    return editorServerPort;
  }
  
  console.error('[EDITOR-SERVER] Script not found:', EDITOR_SERVER_SCRIPT);
  return null;
}

// Launch an editor in Chrome
ipcMain.handle('launch:editor', async (event, editorName) => {
  try {
    const name = editorName.toLowerCase();
    const editorInfo = EDITOR_PATHS[name];
    
    if (!editorInfo) {
      return { success: false, error: `Unknown editor: ${editorName}` };
    }
    
    // Start the editor server
    const port = await startEditorServer();
    if (!port) {
      return { success: false, error: 'Could not start editor server' };
    }
    
    // Build the URL
    const url = `http://127.0.0.1:${port}/${editorInfo.path}/${editorInfo.file}`;
    console.log(`[EDITOR] Opening ${editorName} at ${url}`);
    
    // Launch Chrome in app mode - creates a clean, consistent window for all editors
    // --app= creates a window without address bar/tabs, like a standalone app
    // --window-size sets a reasonable default size
    const args = ['--app=' + url, '--window-size=1400,900'];
    const chromeProcess = spawn(CHROME_PATH, args, {
      detached: true,
      stdio: 'ignore'
    });
    chromeProcess.unref();
    
    // Don't minimize - let Chrome open on top naturally
    // User can Alt+Tab between them as needed
    
    return { success: true, url };
  } catch (e) {
    console.error('[EDITOR] Launch error:', e);
    return { success: false, error: e.message };
  }
});

// Get list of available editors
ipcMain.handle('editor:list', async () => {
  return Object.keys(EDITOR_PATHS).filter((key, idx, arr) => {
    // Remove aliases (entries where the path matches a previous entry)
    const info = EDITOR_PATHS[key];
    const firstMatch = arr.find(k => EDITOR_PATHS[k].path === info.path && EDITOR_PATHS[k].file === info.file);
    return firstMatch === key;
  });
});

// YouTube Search server launcher - starts a localhost server for YouTube embed to work
ipcMain.handle('launch:ytsearch-server', async () => {
  try {
    // Check if server is already running by checking if port 3000 is in use
    const net = require('net');
    const portInUse = await new Promise((resolve) => {
      const tester = net.createServer()
        .once('error', () => resolve(true))
        .once('listening', () => {
          tester.close();
          resolve(false);
        })
        .listen(3001, '127.0.0.1');
    });
    
    if (portInUse) {
      console.log('[YTSEARCH] Server already running on port 3001');
      return { success: true, url: 'http://localhost:3001' };
    }
    
    if (fs.existsSync(YTSEARCH_SERVER_SCRIPT)) {
      ytsearchServerProcess = spawn('python', [YTSEARCH_SERVER_SCRIPT], {
        cwd: path.dirname(YTSEARCH_SERVER_SCRIPT),
        detached: false,
        stdio: 'ignore',
        windowsHide: true
      });
      
      console.log('[YTSEARCH] Server started on port 3001');
      
      // Wait a moment for the server to start
      await new Promise(resolve => setTimeout(resolve, 500));
      
      return { success: true, url: 'http://localhost:3001' };
    }
    return { success: false, error: 'YTSearch server script not found' };
  } catch (e) {
    console.error('[YTSEARCH] Server launch error:', e);
    return { success: false, error: e.message };
  }
});

// ============ WINDOW CONTROL ============

ipcMain.handle('window:focus', async () => {
  if (mainWindow) {
    mainWindow.restore();
    mainWindow.focus();
    mainWindow.setFullScreen(true);
  }
});

ipcMain.handle('window:minimize', async () => {
  if (mainWindow) {
    mainWindow.minimize();
  }
});

ipcMain.handle('window:close', async () => {
  if (mainWindow) {
    mainWindow.close();
  }
});

ipcMain.handle('window:toggleFullscreen', async () => {
  if (mainWindow) {
    mainWindow.setFullScreen(!mainWindow.isFullScreen());
  }
});

// ============ UTILITY ============

// Get the local server URL for loading apps
ipcMain.handle('getServerUrl', async () => {
  if (hubServer && hubServerPort) {
    return `http://127.0.0.1:${hubServerPort}/`;
  }
  return null;
});

ipcMain.handle('app:getPath', async (event, name) => {
  return app.getPath(name);
});

ipcMain.handle('shell:openExternal', async (event, url) => {
  await shell.openExternal(url);
});

// Kill Chrome browsers (used when returning from streaming)
ipcMain.handle('chrome:close', async () => {
  return new Promise((resolve) => {
    exec('taskkill /F /IM chrome.exe', (error) => {
      resolve({ success: !error });
    });
  });
});

// Kill control bar
ipcMain.handle('controlBar:close', async () => {
  return new Promise((resolve) => {
    exec('taskkill /F /FI "WINDOWTITLE eq Control Bar*"', (error) => {
      // Also try to kill by Python script name
      exec('wmic process where "commandline like \'%control_bar.py%\'" delete', () => {
        resolve({ success: true });
      });
    });
  });
});

// ============ SYSTEM CONTROLS ============

// Volume control using PowerShell and nircmd
ipcMain.handle('system:volumeUp', async () => {
  return new Promise((resolve) => {
    // Use PowerShell to increase volume
    exec('powershell -Command "(New-Object -ComObject WScript.Shell).SendKeys([char]175)"', (error) => {
      if (error) {
        // Fallback: try nircmd if available
        exec('nircmd.exe changesysvolume 6553', (err2) => {
          resolve({ success: !err2 });
        });
      } else {
        resolve({ success: true });
      }
    });
  });
});

ipcMain.handle('system:volumeDown', async () => {
  return new Promise((resolve) => {
    exec('powershell -Command "(New-Object -ComObject WScript.Shell).SendKeys([char]174)"', (error) => {
      if (error) {
        exec('nircmd.exe changesysvolume -6553', (err2) => {
          resolve({ success: !err2 });
        });
      } else {
        resolve({ success: true });
      }
    });
  });
});

ipcMain.handle('system:volumeMute', async () => {
  return new Promise((resolve) => {
    exec('powershell -Command "(New-Object -ComObject WScript.Shell).SendKeys([char]173)"', (error) => {
      if (error) {
        exec('nircmd.exe mutesysvolume 2', (err2) => {
          resolve({ success: !err2 });
        });
      } else {
        resolve({ success: true });
      }
    });
  });
});

ipcMain.handle('system:volumeMax', async () => {
  return new Promise((resolve) => {
    // Set volume to 100% using PowerShell
    const ps = `
      $obj = New-Object -ComObject WScript.Shell
      # Press volume up many times to ensure max
      # Loop 50 times with a delay to ensure the system registers each keypress
      for ($i = 0; $i -lt 50; $i++) { $obj.SendKeys([char]175); Start-Sleep -Milliseconds 60 }
    `;
    exec(`powershell -Command "${ps.replace(/\n/g, '; ')}"`, (error) => {
      if (error) {
        exec('nircmd.exe setsysvolume 65535', (err2) => {
          resolve({ success: !err2 });
        });
      } else {
        resolve({ success: true });
      }
    });
  });
});

// Timer-based shutdown
ipcMain.handle('system:shutdownTimer', async (event, minutes) => {
  return new Promise((resolve) => {
    const seconds = minutes * 60;
    exec(`shutdown /s /t ${seconds}`, (error) => {
      resolve({ success: !error, error: error?.message });
    });
  });
});

// Cancel shutdown timer
ipcMain.handle('system:cancelShutdown', async () => {
  return new Promise((resolve) => {
    exec('shutdown /a', (error) => {
      resolve({ success: !error, error: error?.message });
    });
  });
});

// Restart computer
ipcMain.handle('system:restart', async () => {
  return new Promise((resolve) => {
    exec('shutdown /r /t 5', (error) => {
      resolve({ success: !error, error: error?.message });
    });
  });
});

// Shutdown computer immediately
ipcMain.handle('system:shutdown', async () => {
  return new Promise((resolve) => {
    exec('shutdown /s /t 5', (error) => {
      resolve({ success: !error, error: error?.message });
    });
  });
});

// Close app
ipcMain.handle('system:closeApp', async () => {
  // Kill tracked Python processes
  if (dmListenerProcess) {
    dmListenerProcess.kill();
    dmListenerProcess = null;
  }
  if (ytsearchServerProcess) {
    ytsearchServerProcess.kill();
    ytsearchServerProcess = null;
  }
  if (editorServerProcess) {
    editorServerProcess.kill();
    editorServerProcess = null;
  }
  
  // Close the hub server
  if (hubServer) {
    hubServer.close();
    hubServer = null;
  }
  
  // Force kill any remaining Python processes spawned by this app
  // This ensures DM listener and other background scripts are terminated
  try {
    exec('taskkill /F /IM python.exe', (error) => {
      if (error) {
        console.log('[CLOSE] No Python processes to kill or already terminated');
      } else {
        console.log('[CLOSE] Killed Python processes');
      }
      app.quit();
    });
  } catch (e) {
    console.error('[CLOSE] Error killing Python processes:', e);
    app.quit();
  }
  
  return { success: true };
});
