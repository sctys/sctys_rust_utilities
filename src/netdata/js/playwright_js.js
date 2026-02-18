// Use playwright-extra with stealth plugin for better bot detection evasion
const { chromium } = require('playwright-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const readline = require('readline');
process.chdir(__dirname);

// Apply all stealth evasions
chromium.use(StealthPlugin());

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

// Comprehensive stealth script - patches things not covered by the plugin
const STEALTH_SCRIPT = `
    (() => {
        // 1. Ensure webdriver is hidden (belt-and-suspenders)
        try {
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
                configurable: true
            });
            delete navigator.__proto__.webdriver;
        } catch(e) {}

        // 2. Full chrome object matching real Chrome
        if (!window.chrome || !window.chrome.runtime) {
            window.chrome = {
                app: {
                    isInstalled: false,
                    InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' },
                    RunningState: { CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' }
                },
                runtime: {
                    OnInstalledReason: { CHROME_UPDATE: 'chrome_update', INSTALL: 'install', SHARED_MODULE_UPDATE: 'shared_module_update', UPDATE: 'update' },
                    OnRestartRequiredReason: { APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic' },
                    PlatformArch: { ARM: 'arm', ARM64: 'arm64', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' },
                    PlatformNaclArch: { ARM: 'arm', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' },
                    PlatformOs: { ANDROID: 'android', CROS: 'cros', LINUX: 'linux', MAC: 'mac', OPENBSD: 'openbsd', WIN: 'win' },
                    RequestUpdateCheckStatus: { NO_UPDATE: 'no_update', THROTTLED: 'throttled', UPDATE_AVAILABLE: 'update_available' }
                },
                loadTimes: function() { return {}; },
                csi: function() { return {}; }
            };
        }

        // 3. Permissions API
        try {
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.__proto__.query = function(parameters) {
                if (parameters.name === 'notifications') {
                    return Promise.resolve({ state: Notification.permission });
                }
                return originalQuery.call(this, parameters);
            };
        } catch(e) {}

        // 4. Realistic plugins (PluginArray-like)
        try {
            const makePlugin = (name, filename, description, mimeTypes) => {
                const plugin = Object.create(Plugin.prototype);
                Object.defineProperties(plugin, {
                    name: { value: name, enumerable: true },
                    filename: { value: filename, enumerable: true },
                    description: { value: description, enumerable: true },
                    length: { value: mimeTypes.length, enumerable: true }
                });
                mimeTypes.forEach((mt, i) => {
                    const mimeType = Object.create(MimeType.prototype);
                    Object.defineProperties(mimeType, {
                        type: { value: mt.type, enumerable: true },
                        suffixes: { value: mt.suffixes, enumerable: true },
                        description: { value: mt.description, enumerable: true },
                        enabledPlugin: { value: plugin, enumerable: true }
                    });
                    Object.defineProperty(plugin, i, { value: mimeType, enumerable: true });
                });
                return plugin;
            };

            const plugins = [
                makePlugin('Chrome PDF Plugin', 'internal-pdf-viewer', 'Portable Document Format', [
                    { type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format' }
                ]),
                makePlugin('Chrome PDF Viewer', 'mhjfbmdgcfjbbpaeojofohoefgiehjai', '', [
                    { type: 'application/pdf', suffixes: 'pdf', description: '' }
                ]),
                makePlugin('Native Client', 'internal-nacl-plugin', '', [
                    { type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable' },
                    { type: 'application/x-pnacl', suffixes: '', description: 'Portable Native Client Executable' }
                ])
            ];

            const pluginArray = Object.create(PluginArray.prototype);
            plugins.forEach((p, i) => {
                Object.defineProperty(pluginArray, i, { value: p, enumerable: true });
            });
            Object.defineProperty(pluginArray, 'length', { value: plugins.length });
            pluginArray.item = (i) => plugins[i];
            pluginArray.namedItem = (name) => plugins.find(p => p.name === name) || null;
            pluginArray.refresh = () => {};

            Object.defineProperty(navigator, 'plugins', {
                get: () => pluginArray,
                configurable: true
            });
        } catch(e) {}

        // 5. MimeTypes
        try {
            const mimeTypes = [
                { type: 'application/pdf', suffixes: 'pdf', description: '' },
                { type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format' },
                { type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable' },
                { type: 'application/x-pnacl', suffixes: '', description: 'Portable Native Client Executable' }
            ];
            const mimeTypeArray = Object.create(MimeTypeArray.prototype);
            mimeTypes.forEach((mt, i) => {
                const mimeType = Object.create(MimeType.prototype);
                Object.defineProperties(mimeType, {
                    type: { value: mt.type, enumerable: true },
                    suffixes: { value: mt.suffixes, enumerable: true },
                    description: { value: mt.description, enumerable: true }
                });
                Object.defineProperty(mimeTypeArray, i, { value: mimeType, enumerable: true });
                Object.defineProperty(mimeTypeArray, mt.type, { value: mimeType, enumerable: true });
            });
            Object.defineProperty(mimeTypeArray, 'length', { value: mimeTypes.length });
            Object.defineProperty(navigator, 'mimeTypes', {
                get: () => mimeTypeArray,
                configurable: true
            });
        } catch(e) {}

        // 6. Languages
        try {
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-GB', 'en', 'en-US'],
                configurable: true
            });
        } catch(e) {}

        // 7. Hardware concurrency
        try {
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 8,
                configurable: true
            });
        } catch(e) {}

        // 8. Device memory
        try {
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8,
                configurable: true
            });
        } catch(e) {}

        // 9. Platform
        try {
            Object.defineProperty(navigator, 'platform', {
                get: () => 'Linux x86_64',
                configurable: true
            });
        } catch(e) {}

        // 10. Network connection
        try {
            Object.defineProperty(navigator, 'connection', {
                get: () => ({
                    effectiveType: '4g',
                    rtt: 50,
                    downlink: 10,
                    saveData: false,
                    type: 'wifi',
                    addEventListener: () => {},
                    removeEventListener: () => {}
                }),
                configurable: true
            });
        } catch(e) {}

        // 11. Fix window.outerWidth/outerHeight (0 in headless)
        try {
            if (window.outerWidth === 0) {
                Object.defineProperty(window, 'outerWidth', { get: () => window.innerWidth, configurable: true });
            }
            if (window.outerHeight === 0) {
                Object.defineProperty(window, 'outerHeight', { get: () => window.innerHeight, configurable: true });
            }
        } catch(e) {}

        // 12. Screen properties consistency
        try {
            Object.defineProperty(screen, 'availWidth', { get: () => 1920, configurable: true });
            Object.defineProperty(screen, 'availHeight', { get: () => 1080, configurable: true });
            Object.defineProperty(screen, 'width', { get: () => 1920, configurable: true });
            Object.defineProperty(screen, 'height', { get: () => 1080, configurable: true });
            Object.defineProperty(screen, 'colorDepth', { get: () => 24, configurable: true });
            Object.defineProperty(screen, 'pixelDepth', { get: () => 24, configurable: true });
        } catch(e) {}

        // 13. Canvas fingerprint noise (subtle random noise to avoid identical fingerprints)
        try {
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function(type, ...args) {
                const ctx = this.getContext('2d');
                if (ctx) {
                    const imageData = ctx.getImageData(0, 0, this.width, this.height);
                    for (let i = 0; i < imageData.data.length; i += 4) {
                        imageData.data[i] = imageData.data[i] ^ (Math.random() * 2 | 0);
                    }
                    ctx.putImageData(imageData, 0, 0);
                }
                return originalToDataURL.apply(this, [type, ...args]);
            };

            const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
            CanvasRenderingContext2D.prototype.getImageData = function(x, y, w, h) {
                const imageData = originalGetImageData.apply(this, arguments);
                for (let i = 0; i < imageData.data.length; i += 4) {
                    imageData.data[i] = imageData.data[i] ^ (Math.random() * 2 | 0);
                }
                return imageData;
            };
        } catch(e) {}

        // 14. WebGL vendor/renderer spoofing
        try {
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Intel Inc.';  // UNMASKED_VENDOR_WEBGL
                if (parameter === 37446) return 'Intel Iris OpenGL Engine';  // UNMASKED_RENDERER_WEBGL
                return getParameter.apply(this, arguments);
            };
            const getParameter2 = WebGL2RenderingContext.prototype.getParameter;
            WebGL2RenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Intel Inc.';
                if (parameter === 37446) return 'Intel Iris OpenGL Engine';
                return getParameter2.apply(this, arguments);
            };
        } catch(e) {}

        // 15. Notification permission
        try {
            Object.defineProperty(Notification, 'permission', {
                get: () => 'default',
                configurable: true
            });
        } catch(e) {}

        // 16. Hide automation-related error stacks
        try {
            const originalError = Error;
            Error = function(...args) {
                const err = new originalError(...args);
                if (err.stack) {
                    err.stack = err.stack.replace(/\s+at\s+.*playwright.*\n?/g, '');
                }
                return err;
            };
            Error.prototype = originalError.prototype;
            Error.captureStackTrace = originalError.captureStackTrace;
        } catch(e) {}

        // 17. Consistent navigator.vendor
        try {
            Object.defineProperty(navigator, 'vendor', {
                get: () => 'Google Inc.',
                configurable: true
            });
        } catch(e) {}

        // 18. maxTouchPoints (desktop = 0)
        try {
            Object.defineProperty(navigator, 'maxTouchPoints', {
                get: () => 0,
                configurable: true
            });
        } catch(e) {}
    })();
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
                '--disable-infobars',
                '--disable-breakpad',
                '--disable-client-side-phishing-detection',
                '--disable-component-extensions-with-background-pages',
                '--disable-default-apps',
                '--disable-extensions',
                '--disable-hang-monitor',
                '--disable-ipc-flooding-protection',
                '--disable-popup-blocking',
                '--disable-prompt-on-repost',
                '--disable-sync',
                '--force-color-profile=srgb',
                '--metrics-recording-only',
                '--no-first-run',
                '--password-store=basic',
                '--use-mock-keychain',
                '--window-size=1920,1080',
                '--lang=en-GB',
                // Prevent headless detection via GPU
                '--use-gl=swiftshader',
                // Prevent CDP detection
                '--disable-remote-debugging',
            ]
        };

        browser = await chromium.launch(launchOptions);
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
        timezoneId: 'Europe/London',
        colorScheme: 'light',
        deviceScaleFactor: 1,
        hasTouch: false,
        isMobile: false,
        javaScriptEnabled: true,
        acceptDownloads: false,
        ignoreHTTPSErrors: false,
        extraHTTPHeaders: {
            'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
        }
    };

    // Add proxy if provided
    if (proxy && proxy.server) {
        contextConfig.proxy = {
            server: proxy.server,
            ...(proxy.username && { username: proxy.username }),
            ...(proxy.password && { password: proxy.password })
        };
    }

    // Merge custom headers if provided (override defaults)
    if (headers && Object.keys(headers).length > 0) {
        contextConfig.extraHTTPHeaders = {
            ...contextConfig.extraHTTPHeaders,
            ...headers
        };
    }

    const context = await browser.newContext(contextConfig);

    // Inject stealth script on every page/frame
    await context.addInitScript(STEALTH_SCRIPT);

    const page = await context.newPage();

    // Simulate realistic mouse movement before navigation
    await page.mouse.move(
        Math.floor(Math.random() * 1920),
        Math.floor(Math.random() * 1080)
    );

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
rl.on('line', async (line) => {
    if (!line.trim()) return;

    const result = await handleCommand(line);
    console.log(JSON.stringify(result));
});

// Cleanup on exit
process.on('SIGINT', async () => {
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
