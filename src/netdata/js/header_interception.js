(function() {
    // Global storage for all intercepted headers and cookies
    window.__allHeaders = {};
    window.__allCookies = {};
    
    // Track specific headers of interest (including cookies)
    window.__trackedHeaderKeys = ['cookie', 'set-cookie', 'authorization', 'x-csrf-token'];
    
    // =====================================================================
    // 1. Intercept fetch API
    // =====================================================================
    const originalFetch = window.fetch;
    window.fetch = async function(resource, options = {}) {
        try {
        const url = resource instanceof Request ? resource.url : String(resource);
        
        // Get headers from Request object
        let headers = {};
        if (resource instanceof Request) {
            resource.headers.forEach((value, name) => {
            headers[name.toLowerCase()] = value;
            });
        }
        
        // Get headers from options
        if (options.headers) {
            if (options.headers instanceof Headers) {
            options.headers.forEach((value, name) => {
                headers[name.toLowerCase()] = value;
            });
            } else if (typeof options.headers === 'object') {
            Object.keys(options.headers).forEach(name => {
                headers[name.toLowerCase()] = options.headers[name];
            });
            }
        }
        
        // Add default headers that browsers typically include
        if (!headers['user-agent']) {
            headers['user-agent'] = navigator.userAgent;
        }
        
        if (!headers['accept']) {
            headers['accept'] = '*/*';
        }
        
        if (!headers['accept-language']) {
            headers['accept-language'] = navigator.language;
        }
        
        // Ensure we capture the cookie header if not already present
        if (!headers['cookie'] && document.cookie) {
            headers['cookie'] = document.cookie;
        }
        
        // Store headers
        window.__allHeaders[url] = headers;
        
        // Extract cookies if present
        if (headers['cookie']) {
            window.__allCookies[url] = parseCookieString(headers['cookie']);
        }
        
        // Log the interception
        console.log(`[Fetch] Intercepted headers for ${url}:`, headers);
        
        // Track the response for Set-Cookie headers
        const response = await originalFetch.apply(this, arguments);
        
        // Clone the response to leave the original intact
        const clonedResponse = response.clone();
        
        // Extract and log Set-Cookie headers
        const setCookieHeader = clonedResponse.headers.get('set-cookie');
        if (setCookieHeader) {
            console.log(`[Fetch Response] Set-Cookie for ${url}:`, setCookieHeader);
            
            // Parse and store the cookies
            const responseCookies = parseSetCookieString(setCookieHeader);
            if (!window.__allCookies[url]) {
            window.__allCookies[url] = {};
            }
            Object.assign(window.__allCookies[url], responseCookies);
        }
        
        return response;
        } catch (e) {
        console.error('Error intercepting fetch:', e);
        // Continue with original fetch even if our interception fails
        return originalFetch.apply(this, arguments);
        }
    };

    // =====================================================================
    // 2. Intercept XMLHttpRequest
    // =====================================================================
    const originalXHROpen = XMLHttpRequest.prototype.open;
    const originalXHRSetRequestHeader = XMLHttpRequest.prototype.setRequestHeader;
    const originalXHRSend = XMLHttpRequest.prototype.send;
    const originalGetAllResponseHeaders = XMLHttpRequest.prototype.getAllResponseHeaders;
    const originalWithCredentialsSetter = Object.getOwnPropertyDescriptor(XMLHttpRequest.prototype, 'withCredentials').set;
    
    XMLHttpRequest.prototype.open = function(method, url, ...rest) {
        this._url = url;
        this._method = method;
        this._headers = {
        'user-agent': navigator.userAgent,
        'accept': '*/*',
        'accept-language': navigator.language
        };
        
        // Force withCredentials for cross-origin requests to ensure cookies are sent
        this._originalWithCredentials = false;
        
        // Also capture the full request details for debugging
        this._requestDetails = {
        method,
        url,
        async: rest[0] !== false,
        cookies: {
            document: document.cookie,
            parsed: parseCookieString(document.cookie)
        }
        };
        
        console.log(`[XHR Debug] Opening ${method} request to ${url}`);
        
        return originalXHROpen.apply(this, arguments);
    };
    
    // Override withCredentials to both track and allow the original behavior
    Object.defineProperty(XMLHttpRequest.prototype, 'withCredentials', {
        get: function() {
        return this._originalWithCredentials === true;
        },
        set: function(value) {
        console.log(`[XHR Debug] withCredentials set to ${value} for ${this._url}`);
        this._originalWithCredentials = value;
        
        // If we're setting this explicitly, remember it
        if (this._requestDetails) {
            this._requestDetails.withCredentials = value;
        }
        
        // Call original setter
        if (originalWithCredentialsSetter) {
            return originalWithCredentialsSetter.call(this, value);
        }
        }
    });
    
    XMLHttpRequest.prototype.setRequestHeader = function(name, value) {
        if (this._headers) {
        this._headers[name.toLowerCase()] = value;
        
        // Track separately to ensure we catch all headers
        if (!this._rawHeaders) this._rawHeaders = {};
        this._rawHeaders[name] = value;
        }
        return originalXHRSetRequestHeader.apply(this, arguments);
    };
    
    XMLHttpRequest.prototype.send = function(...args) {
        try {
        // Force including credentials right before sending
        if (this.withCredentials !== true) {
            console.log(`[XHR Debug] Auto-enabling withCredentials for ${this._url}`);
            this.withCredentials = true;
        }
        
        // Get all cookies from document.cookie again right before sending
        // This ensures we capture any cookies set after open() but before send()
        const currentCookies = document.cookie;
        
        if (currentCookies) {
            // Ensure cookies are in the headers
            this._headers['cookie'] = currentCookies;
            
            // Manually set the Cookie header to make sure it's included
            try {
            // Only call setRequestHeader if not already set
            if (!this._rawHeaders || !this._rawHeaders['Cookie']) {
                originalXHRSetRequestHeader.call(this, 'Cookie', currentCookies);
            }
            } catch (e) {
            console.warn(`[XHR Debug] Couldn't set Cookie header manually: ${e.message}`);
            }
        }
        
        if (this._url && this._headers) {
            // Store headers
            window.__allHeaders[this._url] = this._headers;
            
            // Extract cookies if present
            if (this._headers['cookie']) {
            window.__allCookies[this._url] = parseCookieString(this._headers['cookie']);
            }
            
            // Also store all browser cookies at time of request
            if (!window.__browserCookies) window.__browserCookies = {};
            window.__browserCookies[this._url] = {
            cookieString: currentCookies,
            parsed: parseCookieString(currentCookies),
            timestamp: new Date().toISOString()
            };
            
            // Log the interception with detailed info
            console.log(`[XHR] Intercepted request to ${this._url}:`, {
            method: this._method,
            headers: this._headers,
            withCredentials: this.withCredentials,
            documentCookie: currentCookies
            });
        }
        
        // Set up a more comprehensive response header capture
        const self = this;
        const originalStateChange = this.onreadystatechange;
        
        this.onreadystatechange = function() {
            if (this.readyState === 4) {
            try {
                // Get all response headers
                const allHeaders = originalGetAllResponseHeaders.call(this);
                const responseHeaders = {};
                
                // Parse the headers string
                allHeaders.trim().split(/[\r\n]+/).forEach(line => {
                const parts = line.split(': ');
                const header = parts.shift();
                const value = parts.join(': ');
                responseHeaders[header.toLowerCase()] = value;
                });
                
                // Store response headers for this URL
                if (!window.__responseHeaders) window.__responseHeaders = {};
                window.__responseHeaders[self._url] = responseHeaders;
                
                // Log all response headers
                console.log(`[XHR Response] Headers for ${self._url}:`, responseHeaders);
                
                // Check specifically for Set-Cookie header
                const setCookieHeader = this.getResponseHeader('set-cookie');
                if (setCookieHeader) {
                console.log(`[XHR Response] Set-Cookie for ${self._url}:`, setCookieHeader);
                
                // Parse and store the cookies
                const responseCookies = parseSetCookieString(setCookieHeader);
                if (!window.__allCookies[self._url]) {
                    window.__allCookies[self._url] = {};
                }
                Object.assign(window.__allCookies[self._url], responseCookies);
                }
                
                // Also update our document cookie record
                setTimeout(() => {
                // Check if document.cookie changed after this response
                const newDocCookies = document.cookie;
                if (newDocCookies !== currentCookies) {
                    console.log(`[XHR Response] Document cookies changed after request to ${self._url}`, {
                    before: currentCookies,
                    after: newDocCookies
                    });
                    
                    // Update the global cookie store
                    window.__allCookies['_document_cookie_'] = parseCookieString(newDocCookies);
                }
                }, 50); // Small delay to ensure cookies are set
            } catch (e) {
                console.error('Error capturing XHR response headers:', e);
            }
            }
            
            // Call original handler if it exists
            if (typeof originalStateChange === 'function') {
            originalStateChange.apply(this, arguments);
            }
        };
        } catch (e) {
        console.error('Error intercepting XHR:', e);
        }
        
        return originalXHRSend.apply(this, arguments);
    };

    // =====================================================================
    // 3. Intercept Image loads
    // =====================================================================
    const originalImageSrc = Object.getOwnPropertyDescriptor(Image.prototype, 'src');
    
    if (originalImageSrc && originalImageSrc.set) {
        Object.defineProperty(Image.prototype, 'src', {
        set: function(value) {
            try {
            const headers = {
                'user-agent': navigator.userAgent,
                'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
                'accept-language': navigator.language,
                'referer': document.location.href
            };
            
            // Add cookies by default
            if (document.cookie) {
                headers['cookie'] = document.cookie;
            }
            
            // Store headers
            window.__allHeaders[value] = headers;
            
            // Extract cookies if present
            if (headers['cookie']) {
                window.__allCookies[value] = parseCookieString(headers['cookie']);
            }
            
            // Log the interception
            console.log(`[Image] Intercepted headers for ${value}:`, headers);
            } catch (e) {
            console.error('Error intercepting Image src:', e);
            }
            
            return originalImageSrc.set.call(this, value);
        },
        get: originalImageSrc.get
        });
    }

    // =====================================================================
    // 4. Additional script/stylesheet/resource loads
    // =====================================================================
    // Monitor script elements
    const originalScriptSrc = Object.getOwnPropertyDescriptor(HTMLScriptElement.prototype, 'src');
    if (originalScriptSrc && originalScriptSrc.set) {
        Object.defineProperty(HTMLScriptElement.prototype, 'src', {
        set: function(value) {
            try {
            const headers = {
                'user-agent': navigator.userAgent,
                'accept': '*/*',
                'accept-language': navigator.language,
                'referer': document.location.href
            };
            
            // Add cookies by default
            if (document.cookie) {
                headers['cookie'] = document.cookie;
            }
            
            // Store headers
            window.__allHeaders[value] = headers;
            
            // Extract cookies if present
            if (headers['cookie']) {
                window.__allCookies[value] = parseCookieString(headers['cookie']);
            }
            
            // Log the interception
            console.log(`[Script] Intercepted headers for ${value}:`, headers);
            } catch (e) {
            console.error('Error intercepting Script src:', e);
            }
            
            return originalScriptSrc.set.call(this, value);
        },
        get: originalScriptSrc.get
        });
    }
    
    // Monitor link/stylesheet elements
    const originalLinkHref = Object.getOwnPropertyDescriptor(HTMLLinkElement.prototype, 'href');
    if (originalLinkHref && originalLinkHref.set) {
        Object.defineProperty(HTMLLinkElement.prototype, 'href', {
        set: function(value) {
            try {
            let accept = '*/*';
            if (this.rel === 'stylesheet') {
                accept = 'text/css,*/*;q=0.1';
            }
            
            const headers = {
                'user-agent': navigator.userAgent,
                'accept': accept,
                'accept-language': navigator.language,
                'referer': document.location.href
            };
            
            // Add cookies by default
            if (document.cookie) {
                headers['cookie'] = document.cookie;
            }
            
            // Store headers
            window.__allHeaders[value] = headers;
            
            // Extract cookies if present
            if (headers['cookie']) {
                window.__allCookies[value] = parseCookieString(headers['cookie']);
            }
            
            // Log the interception
            console.log(`[Link] Intercepted headers for ${value}:`, headers);
            } catch (e) {
            console.error('Error intercepting Link href:', e);
            }
            
            return originalLinkHref.set.call(this, value);
        },
        get: originalLinkHref.get
        });
    }

    // =====================================================================
    // 5. Global cookie monitoring
    // =====================================================================
    // Track document.cookie changes
    const originalCookie = Object.getOwnPropertyDescriptor(Document.prototype, 'cookie');
    if (originalCookie && originalCookie.set) {
        Object.defineProperty(Document.prototype, 'cookie', {
        set: function(value) {
            try {
            console.log(`[Document] Cookie set: ${value}`);
            
            // Store in global cookies collection
            const url = '_document_cookie_';
            if (!window.__allCookies[url]) {
                window.__allCookies[url] = {};
            }
            
            // Parse and add the cookie
            const cookieParts = value.split(';')[0].split('=');
            if (cookieParts.length >= 2) {
                const cookieName = cookieParts[0].trim();
                const cookieValue = cookieParts.slice(1).join('=').trim();
                window.__allCookies[url][cookieName] = cookieValue;
            }
            } catch (e) {
            console.error('Error intercepting document.cookie:', e);
            }
            
            return originalCookie.set.call(this, value);
        },
        get: originalCookie.get
        });
    }

    // =====================================================================
    // 6. Helper functions
    // =====================================================================
    // Parse cookie string into object
    function parseCookieString(cookieStr) {
        if (!cookieStr) return {};
        
        const cookieObj = {};
        const cookies = cookieStr.split(';');
        
        cookies.forEach(cookie => {
        const parts = cookie.split('=');
        if (parts.length >= 2) {
            const name = parts[0].trim();
            const value = parts.slice(1).join('=').trim();
            cookieObj[name] = value;
        }
        });
        
        return cookieObj;
    }
    
    // Parse Set-Cookie header into object
    function parseSetCookieString(setCookieStr) {
        if (!setCookieStr) return {};
        
        const cookieObj = {};
        const cookies = setCookieStr.split(',');
        
        cookies.forEach(cookie => {
        const mainParts = cookie.split(';');
        if (mainParts.length >= 1) {
            const keyValuePair = mainParts[0].split('=');
            if (keyValuePair.length >= 2) {
            const name = keyValuePair[0].trim();
            const value = keyValuePair.slice(1).join('=').trim();
            cookieObj[name] = value;
            }
        }
        });
        
        return cookieObj;
    }

    // =====================================================================
    // 7. Enhanced helper methods
    // =====================================================================
    // Method to get all headers
    window.__getInterceptedHeaders = function() {
        return window.__allHeaders;
    };
    
    // Method to get all cookies
    window.__getInterceptedCookies = function() {
        return window.__allCookies;
    };
    
    // Method to get document cookies
    window.__getDocumentCookies = function() {
        return window.__allCookies['_document_cookie_'] || {};
    };
    
    // Method to get cookies for a specific URL
    window.__getCookiesForUrl = function(url) {
        return window.__allCookies[url] || {};
    };
    
    // Method to get specific header values
    window.__getSpecificHeaders = function(headerNames) {
        const result = {};
        
        Object.keys(window.__allHeaders).forEach(url => {
        result[url] = {};
        headerNames.forEach(headerName => {
            const normalizedName = headerName.toLowerCase();
            if (window.__allHeaders[url][normalizedName]) {
            result[url][normalizedName] = window.__allHeaders[url][normalizedName];
            }
        });
        });
        
        return result;
    };
    
    // Method to clear headers
    window.__clearInterceptedHeaders = function() {
        window.__allHeaders = {};
        return true;
    };
    
    // Method to clear cookies
    window.__clearInterceptedCookies = function() {
        window.__allCookies = {};
        return true;
    };
    
    // Method to set tracked header keys
    window.__setTrackedHeaderKeys = function(headerKeys) {
        if (Array.isArray(headerKeys)) {
        window.__trackedHeaderKeys = headerKeys.map(key => key.toLowerCase());
        return true;
        }
        return false;
    };
    
    // Method to get specific tracked headers only
    window.__getTrackedHeaders = function() {
        return window.__getSpecificHeaders(window.__trackedHeaderKeys);
    };
    
    console.log('Enhanced header and cookie interception system initialized successfully');
    }
)();