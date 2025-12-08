use std::fs;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a Node.js script with stealth
    let script = r#"
const { chromium } = require('playwright');

(async () => {
    const browser = await chromium.launch({
        headless: false,
        // proxy: {
        //     server: 'http://proxy-server:port',
        //     username: 'user',
        //     password: 'pass'
        // }
        args: [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-web-security',
            '--window-size=1920,1080',
            '--start-maximized',
            '--disable-gpu',
            '--disable-software-rasterizer'
        ]
    });
    
    const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        viewport: { width: 1920, height: 1080 },
        locale: 'en-GB',
        timezoneId: 'Europe/London',
        hasTouch: false,
        isMobile: false,
        deviceScaleFactor: 1,
    });
    
    // Inject stealth script before navigation
    await context.addInitScript(() => {
        // Hide webdriver
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        
        // Add chrome object
        window.chrome = {
            runtime: {}
        };
        
        // Mock plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                { name: 'Chrome PDF Plugin' },
                { name: 'Chrome PDF Viewer' },
                { name: 'Native Client' }
            ]
        });
        
        // Languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-GB', 'en', 'en-US']
        });
        
        // Permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(parameters)
        );
    });
    
    const page = await context.newPage();
    
    await page.goto('https://www.scoresway.com/en_GB/soccer/competitions', {
        waitUntil: 'networkidle'
    });
    
    const title = await page.title();
    console.log('Title:', title);
    
    const content = await page.content();
    console.log('Content length:', content.length);
    
    // Output content as JSON
    console.log('CONTENT_START');
    console.log(JSON.stringify({ content, title }));
    console.log('CONTENT_END');
    
    await browser.close();
})();
"#;

    // Write script to temp file
    fs::write("scraper.js", script)?;

    println!("Running Node.js Playwright...");

    // Execute Node.js script
    let output = Command::new("node").arg("scraper.js").output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("STDOUT: {}", stdout);
    if !stderr.is_empty() {
        eprintln!("STDERR: {}", stderr);
    }

    // Parse the output
    if let Some(start) = stdout.find("CONTENT_START") {
        if let Some(end) = stdout.find("CONTENT_END") {
            let json_str = &stdout[start + 13..end].trim();
            println!("\n✓ Successfully scraped page!");
            println!("Data length: {} bytes", json_str.len());
        }
    }

    // Cleanup
    fs::remove_file("scraper.js")?;

    Ok(())
}
