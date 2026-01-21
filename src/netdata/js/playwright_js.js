const { chromium } = require('playwright');
const readline = require('readline');
process.chdir(__dirname);

// Global browser and context pool
let browser = null;
const contexts = new Map(); // contextId -> { context, page }
let contextCounter = 0;

// Initialize readline interface for communication with Rust
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
});

// Stealth script to inject - enhanced for headless
const STEALTH_SCRIPT = `
    // Override webdriver property
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    });
    
    // Chrome object
    window.chrome = {
        runtime: {},
        loadTimes: function() {},
        csi: function() {},
        app: {}
    };
    
    // Override plugins to look real
    Object.defineProperty(navigator, 'plugins', {
        get: () => [
            { 
                name: 'Chrome PDF Plugin', 
                filename: 'internal-pdf-viewer',
                description: 'Portable Document Format',
                length: 1
            },
            { 
                name: 'Chrome PDF Viewer', 
                filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai',
                description: '',
                length: 1
            },
            { 
                name: 'Native Client',
                filename: 'internal-nacl-plugin',
                description: '',
                length: 2
            }
        ]
    });
    
    // Languages
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-GB', 'en', 'en-US']
    });
    
    // Hardware concurrency
    Object.defineProperty(navigator, 'hardwareConcurrency', {
        get: () => 8
    });
    
    // Device memory
    Object.defineProperty(navigator, 'deviceMemory', {
        get: () => 8
    });
    
    // Platform
    Object.defineProperty(navigator, 'platform', {
        get: () => 'Linux x86_64'
    });
    
    // Permissions
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : originalQuery(parameters)
    );
    
    // Mock connection
    Object.defineProperty(navigator, 'connection', {
        get: () => ({
            effectiveType: '4g',
            rtt: 100,
            downlink: 10,
            saveData: false
        })
    });
    
    // Fix for headless detection via chrome.runtime
    if (!window.chrome) {
        window.chrome = {};
    }
    if (!window.chrome.runtime) {
        window.chrome.runtime = {};
    }
`;

// Initialize browser once
async function initBrowser(headlessMode = false) {
    if (!browser) {
        const launchOptions = {
            headless: headlessMode,
            args: [
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
            ]
        };
        
        // Additional flags for headless mode to avoid detection
        if (headlessMode) {
            launchOptions.args.push(
                '--window-size=1920,1080',
                '--start-maximized',
                '--disable-gpu',
                '--disable-software-rasterizer'
            );
        }
        
        browser = await chromium.launch(launchOptions);
        // console.error(`[SERVER] Browser initialized (headless: ${headlessMode})`);
    }
    return browser;
}

// Create a new context with specific proxy
async function createContext(proxy, headers, headlessMode = false) {
  await initBrowser(headlessMode);
    
    const contextConfig = {
        userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        viewport: { width: 1920, height: 1080 },
        locale: 'en-GB',
        timezoneId: 'Europe/London'
    };
    
    // Add proxy if provided
    if (proxy && proxy.server) {
        contextConfig.proxy = {
            server: proxy.server,
            ...(proxy.username && { username: proxy.username }),
            ...(proxy.password && { password: proxy.password })
        };
    }

    // Add custom headers if provided
    if (headers && Object.keys(headers).length > 0) {
        contextConfig.extraHTTPHeaders = headers;
    }
    
    const context = await browser.newContext(contextConfig);
    await context.addInitScript(STEALTH_SCRIPT);
    
    const page = await context.newPage();
    
    const contextId = `ctx_${contextCounter++}`;
    contexts.set(contextId, { context, page });
    
    return contextId;
}

// Close a context
async function closeContext(contextId) {
    const ctx = contexts.get(contextId);
    if (ctx) {
        await ctx.context.close();
        contexts.delete(contextId);
        return true;
    }
    return false;
}

// Navigate to URL and return response data
async function navigate(contextId, url, timeout = 60000) {
    const ctx = contexts.get(contextId);
    if (!ctx) {
        throw new Error(`Context ${contextId} not found`);
    }
    
    const { page } = ctx;
    
    try {
        const response = await page.goto(url, {
            waitUntil: 'networkidle',
            timeout: timeout
        });
        
        const statusCode = response ? response.status() : 200;
        const ok = response ? response.ok() : true;
        const finalUrl = page.url();
        const content = await page.content();
        const reason = response ? response.statusText() : '';
        const cookies = await ctx.context.cookies();
        
        // Convert cookies to a simple map { name: value }
        const cookieMap = {};
        cookies.forEach(c => {
            cookieMap[c.name] = c.value;
        });
        
        return {
            success: true,
            content,
            statusCode,
            url: finalUrl,
            ok,
            reason,
            cookies: cookieMap
        };
    } catch (error) {
        return {
            success: false,
            content: '',
            statusCode: 0,
            url: page.url(),
            ok: false,
            reason: error.message,
            cookies: {}
        };
    }
}

// Get page content
async function getContent(contextId) {
    const ctx = contexts.get(contextId);
    if (!ctx) {
        throw new Error(`Context ${contextId} not found`);
    }
    
    return await ctx.page.content();
}

// Set cookies for a context
async function setCookies(contextId, cookies) {
    const ctx = contexts.get(contextId);
    if (!ctx) {
        throw new Error(`Context ${contextId} not found`);
    }
    
    // Convert HashMap { name: value } to Playwright cookie format
    const cookieArray = Object.entries(cookies).map(([name, value]) => ({
        name: name,
        value: value,
        domain: new URL(ctx.page.url()).hostname,
        path: '/'
    }));
    
    await ctx.context.addCookies(cookieArray);
    return true;
}

// Handle commands from Rust
async function handleCommand(command) {
    try {
        const cmd = JSON.parse(command);
        
        switch (cmd.action) {
            case 'init':
                await initBrowser(cmd.headless || false);
                return { success: true, message: 'Browser initialized' };
            
            case 'create_context':
                const contextId = await createContext(cmd.proxy, cmd.headers, cmd.headless || false);
                return { success: true, contextId };
            
            case 'navigate':
                const navResult = await navigate(cmd.contextId, cmd.url, cmd.timeout);
                return navResult;
            
            case 'get_content':
                const content = await getContent(cmd.contextId);
                return { success: true, content };
            
            case 'set_cookies':
                await setCookies(cmd.contextId, cmd.cookies);
                return { success: true };
            
            case 'close_context':
                const closed = await closeContext(cmd.contextId);
                return { success: closed };
            
            case 'shutdown':
                if (browser) {
                    await browser.close();
                }
                process.exit(0);
            
            default:
                return { success: false, error: 'Unknown action' };
        }
    } catch (error) {
        return { success: false, error: error.message };
    }
}

// Main loop - read commands from stdin
// console.error('[SERVER] Playwright server ready');

rl.on('line', async (line) => {
    if (!line.trim()) return;
    
    const result = await handleCommand(line);
    console.log(JSON.stringify(result));
});

// Cleanup on exit
process.on('SIGINT', async () => {
    console.error('[SERVER] Shutting down...');
    if (browser) {
        await browser.close();
    }
    process.exit(0);
});

process.on('SIGTERM', async () => {
    if (browser) {
        await browser.close();
    }
    process.exit(0);
});
