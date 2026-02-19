(() => {
    // 1. Hide webdriver property
    try {
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
            configurable: true
        });
        delete navigator.__proto__.webdriver;
    } catch(e) {}

    // 2. Full chrome object matching real Chrome
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

    // 4. Realistic plugins
    try {
        Object.defineProperty(navigator, 'plugins', {
            get: () => {
                return [
                    {
                        0: {type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format'},
                        description: 'Portable Document Format',
                        filename: 'internal-pdf-viewer',
                        length: 1,
                        name: 'Chrome PDF Plugin'
                    },
                    {
                        0: {type: 'application/pdf', suffixes: 'pdf', description: ''},
                        description: '',
                        filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai',
                        length: 1,
                        name: 'Chrome PDF Viewer'
                    },
                    {
                        0: {type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable'},
                        1: {type: 'application/x-pnacl', suffixes: '', description: 'Portable Native Client Executable'},
                        description: '',
                        filename: 'internal-nacl-plugin',
                        length: 2,
                        name: 'Native Client'
                    }
                ];
            },
            configurable: true
        });
    } catch(e) {}

    // 5. MimeTypes (empty = bot signal)
    try {
        Object.defineProperty(navigator, 'mimeTypes', {
            get: () => {
                return [
                    { type: 'application/pdf', suffixes: 'pdf', description: '' },
                    { type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format' },
                    { type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable' },
                    { type: 'application/x-pnacl', suffixes: '', description: 'Portable Native Client Executable' }
                ];
            },
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

    // 10. Vendor
    try {
        Object.defineProperty(navigator, 'vendor', {
            get: () => 'Google Inc.',
            configurable: true
        });
    } catch(e) {}

    // 11. maxTouchPoints (desktop = 0)
    try {
        Object.defineProperty(navigator, 'maxTouchPoints', {
            get: () => 0,
            configurable: true
        });
    } catch(e) {}

    // 12. Network connection
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

    // 13. Fix outerWidth/outerHeight (0 in headless)
    try {
        if (window.outerWidth === 0) {
            Object.defineProperty(window, 'outerWidth', { get: () => window.innerWidth, configurable: true });
        }
        if (window.outerHeight === 0) {
            Object.defineProperty(window, 'outerHeight', { get: () => window.innerHeight, configurable: true });
        }
    } catch(e) {}

    // 14. Screen properties
    try {
        Object.defineProperty(screen, 'availWidth', { get: () => 1920, configurable: true });
        Object.defineProperty(screen, 'availHeight', { get: () => 1080, configurable: true });
        Object.defineProperty(screen, 'width', { get: () => 1920, configurable: true });
        Object.defineProperty(screen, 'height', { get: () => 1080, configurable: true });
        Object.defineProperty(screen, 'colorDepth', { get: () => 24, configurable: true });
        Object.defineProperty(screen, 'pixelDepth', { get: () => 24, configurable: true });
    } catch(e) {}

    // 15. Canvas fingerprint noise
    try {
        const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(type, ...args) {
            const ctx2d = this.getContext('2d');
            if (ctx2d) {
                const imgData = ctx2d.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imgData.data.length; i += 4) {
                    imgData.data[i] = imgData.data[i] ^ (Math.random() * 2 | 0);
                }
                ctx2d.putImageData(imgData, 0, 0);
            }
            return origToDataURL.apply(this, [type, ...args]);
        };
    } catch(e) {}

    // 16. WebGL vendor/renderer spoofing
    try {
        const getParam = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel Iris OpenGL Engine';
            return getParam.apply(this, arguments);
        };
        const getParam2 = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel Iris OpenGL Engine';
            return getParam2.apply(this, arguments);
        };
    } catch(e) {}

    // 17. Notification permission
    try {
        Object.defineProperty(Notification, 'permission', {
            get: () => 'default',
            configurable: true
        });
    } catch(e) {}
})();