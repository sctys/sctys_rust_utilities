use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_apigateway::types::{EndpointConfiguration, EndpointType};
use aws_sdk_apigateway::{config::Region, Client as ApiGatewayClient};
use futures::future::join_all;
use rand::prelude::*;
use std::collections::HashSet;
use std::error::Error;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use strum_macros::Display;

fn to_kebab_case_with_digit_boundary(s: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, &ch) in chars.iter().enumerate() {
        let prev = if i > 0 { Some(chars[i - 1]) } else { None };

        if ch.is_uppercase() {
            if i > 0 && (prev.unwrap().is_lowercase() || prev.unwrap().is_ascii_digit()) {
                result.push('-');
            }
            result.push(ch.to_ascii_lowercase());
        } else if ch.is_ascii_digit() {
            // insert `-` before digit if the previous is a letter
            if i > 0 && (prev.unwrap().is_ascii_alphabetic()) {
                result.push('-');
            }
            result.push(ch);
        } else {
            result.push(ch);
        }
    }

    result
}

#[derive(Debug, Clone, Display)]
pub enum ApiGatewayRegion {
    UsEast1,
    UsEast2,
    UsWest1,
    UsWest2,
    EuWest1,
    EuWest2,
    EuWest3,
    EuCentral1,
    CaCentral1,
    ApSouth1,
    ApNortheast3,
    ApNortheast2,
    ApSoutheast1,
    ApSoutheast2,
    ApNortheast1,
    SaEast1,
    ApEast1,
    AfSouth1,
    EuSouth1,
    MeSouth1,
}

impl ApiGatewayRegion {
    pub fn to_aws_name(&self) -> String {
        to_kebab_case_with_digit_boundary(self.to_string().as_str())
    }

    pub fn get_default_regions() -> Vec<Self> {
        vec![
            Self::UsEast1,
            Self::UsEast2,
            Self::UsWest1,
            Self::UsWest2,
            Self::EuWest1,
            Self::EuWest2,
            Self::EuWest3,
            Self::EuCentral1,
            Self::CaCentral1,
        ]
    }

    pub fn get_extra_regions() -> Vec<Self> {
        vec![
            Self::UsEast1,
            Self::UsEast2,
            Self::UsWest1,
            Self::UsWest2,
            Self::EuWest1,
            Self::EuWest2,
            Self::EuWest3,
            Self::EuCentral1,
            Self::CaCentral1,
            Self::ApSouth1,
            Self::ApNortheast3,
            Self::ApNortheast2,
            Self::ApSoutheast1,
            Self::ApSoutheast2,
            Self::ApNortheast1,
            Self::SaEast1,
        ]
    }

    pub fn get_all_regions() -> Vec<Self> {
        vec![
            Self::UsEast1,
            Self::UsEast2,
            Self::UsWest1,
            Self::UsWest2,
            Self::EuWest1,
            Self::EuWest2,
            Self::EuWest3,
            Self::EuCentral1,
            Self::CaCentral1,
            Self::ApSouth1,
            Self::ApNortheast3,
            Self::ApNortheast2,
            Self::ApSoutheast1,
            Self::ApSoutheast2,
            Self::ApNortheast1,
            Self::SaEast1,
            Self::ApEast1,
            Self::AfSouth1,
            Self::EuSouth1,
            Self::MeSouth1,
        ]
    }
}

#[derive(Debug, Clone)]
pub struct ApiGatewayConfig {
    site: String,
    regions: Vec<ApiGatewayRegion>,
    access_key_id: Option<String>,
    access_key_secret: Option<String>,
    verbose: bool,
}

impl Default for ApiGatewayConfig {
    fn default() -> Self {
        ApiGatewayConfig {
            site: String::new(),
            regions: ApiGatewayRegion::get_default_regions(),
            access_key_id: None,
            access_key_secret: None,
            verbose: true,
        }
    }
}

impl ApiGatewayConfig {
    fn url_site_from_url(url: &str) -> String {
        url.split('/').take(3).collect::<Vec<_>>().join("/")
    }

    pub fn form_config(url: &str, regions: Option<Vec<ApiGatewayRegion>>) -> Self {
        let site = Self::url_site_from_url(url);
        ApiGatewayConfig {
            site,
            regions: regions.unwrap_or(ApiGatewayRegion::get_default_regions()),
            access_key_id: None,
            access_key_secret: None,
            verbose: true,
        }
    }
}

pub struct ApiGateway {
    config: ApiGatewayConfig,
    api_name: String,
    pub endpoints: Arc<Mutex<Vec<String>>>,
}

impl ApiGateway {
    pub fn new(config: ApiGatewayConfig) -> Self {
        let site = if config.site.ends_with("/") {
            config.site[..config.site.len() - 1].to_string()
        } else {
            config.site.clone()
        };

        let api_name = format!("{} - IP Rotate API", site);

        ApiGateway {
            config,
            api_name,
            endpoints: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start(
        &self,
        force: bool,
        require_manual_deletion: bool,
        endpoints: Vec<String>,
    ) -> Vec<String> {
        // If endpoints given already, assign and continue
        if !endpoints.is_empty() {
            *self.endpoints.lock().unwrap() = endpoints.clone();
            return endpoints;
        }

        // Otherwise, start/locate new endpoints
        if self.config.verbose {
            let plural = if self.config.regions.len() > 1 {
                "s"
            } else {
                ""
            };
            println!(
                "Starting API gateway{} in {} regions.",
                plural,
                self.config.regions.len()
            );
        }

        let mut new_endpoints = 0;
        // let results = self.runtime.block_on(self.init_gateways(force, require_manual_deletion));
        let results = self.init_gateways(force, require_manual_deletion).await;

        let mut all_endpoints = Vec::new();
        for gateway_result in results.into_iter().flatten() {
            if gateway_result.success {
                all_endpoints.push(gateway_result.endpoint.clone());
                if gateway_result.is_new {
                    new_endpoints += 1;
                }
            }
        }

        *self.endpoints.lock().unwrap() = all_endpoints.clone();

        if self.config.verbose {
            println!(
                "Using {} endpoints with name '{}' ({} new).",
                all_endpoints.len(),
                self.api_name,
                new_endpoints
            );
        }

        all_endpoints
    }

    pub async fn shutdown(&self, endpoints: Option<Vec<String>>) -> Vec<String> {
        if self.config.verbose {
            let plural = if self.config.regions.len() > 1 {
                "s"
            } else {
                ""
            };
            println!(
                "Deleting gateway{} for site '{}'.",
                plural, self.config.site
            );
        }

        // let deleted = self.runtime.block_on(self.delete_gateways(endpoints));
        let deleted = self.delete_gateways(endpoints).await;
        let deleted_flat: Vec<String> = deleted.into_iter().flatten().collect();

        if self.config.verbose {
            println!(
                "Deleted {} endpoints for site '{}'.",
                deleted_flat.len(),
                self.config.site
            );
        }

        deleted_flat
    }

    async fn init_gateways(
        &self,
        force: bool,
        require_manual_deletion: bool,
    ) -> Vec<Result<GatewayResult, Box<dyn Error + Send + Sync>>> {
        let mut futures = Vec::new();

        for region_name in &self.config.regions {
            let region_name = region_name.to_aws_name();
            let api_name = self.api_name.clone();
            let site = self.config.site.clone();
            let access_key_id = self.config.access_key_id.clone();
            let access_key_secret = self.config.access_key_secret.clone();
            let verbose = self.config.verbose;

            futures.push(tokio::spawn(async move {
                init_gateway(
                    region_name,
                    api_name,
                    site,
                    access_key_id,
                    access_key_secret,
                    force,
                    require_manual_deletion,
                    verbose,
                )
                .await
            }));
        }

        let results = join_all(futures).await;

        results
            .into_iter()
            .map(|res| match res {
                Ok(inner_res) => inner_res,
                Err(e) => Err(Box::new(e) as Box<dyn Error + Send + Sync>),
            })
            .collect()
    }

    async fn delete_gateways(&self, endpoints: Option<Vec<String>>) -> Vec<Vec<String>> {
        let mut futures = Vec::new();

        for region_name in &self.config.regions {
            let region_name = region_name.to_aws_name();
            let api_name = self.api_name.clone();
            let access_key_id = self.config.access_key_id.clone();
            let access_key_secret = self.config.access_key_secret.clone();
            let endpoint_ids = endpoints.clone();
            let verbose = self.config.verbose;

            futures.push(tokio::spawn(async move {
                delete_gateway(
                    region_name,
                    api_name,
                    access_key_id,
                    access_key_secret,
                    endpoint_ids,
                    verbose,
                )
                .await
            }));
        }

        let results = join_all(futures).await;

        results.into_iter().filter_map(|res| res.ok()).collect()
    }

    pub async fn reqwest_send(
        &self,
        client: &reqwest::Client,
        mut request: reqwest::Request,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        {
            // Get endpoints
            let endpoints = self.endpoints.lock().unwrap();
            if endpoints.is_empty() {
                return Err(Box::new(std::io::Error::other("No endpoints available")));
            }

            // Get random endpoint
            let mut rng = rand::thread_rng();
            let endpoint = endpoints.choose(&mut rng).unwrap();

            // Replace URL with our endpoint
            let url = request.url().clone();
            let url_str = url.as_str();
            let protocol_split: Vec<&str> = url_str.split("://").collect();

            if protocol_split.len() != 2 {
                return Err(Box::new(std::io::Error::other("Invalid URL format")));
            }

            let site_path = protocol_split[1]
                .split('/')
                .skip(1)
                .collect::<Vec<&str>>()
                .join("/");
            let new_url = format!("https://{}/ProxyStage/{}", endpoint, site_path);

            *request.url_mut() = reqwest::Url::parse(&new_url)
                .map_err(|e| std::io::Error::other(format!("Failed to parse URL: {}", e)))?;

            // Replace host with endpoint host
            let headers = request.headers_mut();
            headers.insert(
                "Host",
                reqwest::header::HeaderValue::from_str(endpoint).unwrap(),
            );

            // Auto generate random X-Forwarded-For if doesn't exist
            let x_forwarded_for = headers
                .get("X-Forwarded-For")
                .map(|v| v.to_str().unwrap_or("").to_string());

            headers.remove("X-Forwarded-For");

            let forwarded_ip = if let Some(ip) = x_forwarded_for {
                ip
            } else {
                // Generate random IPv4

                Ipv4Addr::new(
                    rng.gen_range(1..255),
                    rng.gen_range(0..255),
                    rng.gen_range(0..255),
                    rng.gen_range(1..255),
                )
                .to_string()
            };

            headers.insert(
                "X-My-X-Forwarded-For",
                reqwest::header::HeaderValue::from_str(&forwarded_ip).unwrap(),
            );
        };

        // Send the request
        Ok(client.execute(request).await.map_err(Box::new)?)
    }

    pub async fn rquest_send(
        &self,
        client: &rquest::Client,
        mut request: rquest::Request,
    ) -> Result<rquest::Response, Box<dyn Error + Send + Sync>> {
        {
            // Get endpoints
            let endpoints = self.endpoints.lock().unwrap();
            if endpoints.is_empty() {
                return Err(Box::new(std::io::Error::other("No endpoints available")));
            }

            // Get random endpoint
            let mut rng = rand::thread_rng();
            let endpoint = endpoints.choose(&mut rng).unwrap();

            // Replace URL with our endpoint
            let url = request.url().clone();
            let url_str = url.as_str();
            let protocol_split: Vec<&str> = url_str.split("://").collect();

            if protocol_split.len() != 2 {
                return Err(Box::new(std::io::Error::other("Invalid URL format")));
            }

            let site_path = protocol_split[1]
                .split('/')
                .skip(1)
                .collect::<Vec<&str>>()
                .join("/");
            let new_url = format!("https://{}/ProxyStage/{}", endpoint, site_path);

            *request.url_mut() = reqwest::Url::parse(&new_url)
                .map_err(|e| std::io::Error::other(format!("Failed to parse URL: {}", e)))?;

            // Replace host with endpoint host
            let headers = request.headers_mut();
            headers.insert(
                "Host",
                rquest::header::HeaderValue::from_str(endpoint).unwrap(),
            );

            // Auto generate random X-Forwarded-For if doesn't exist
            let x_forwarded_for = headers
                .get("X-Forwarded-For")
                .map(|v| v.to_str().unwrap_or("").to_string());

            headers.remove("X-Forwarded-For");

            let forwarded_ip = if let Some(ip) = x_forwarded_for {
                ip
            } else {
                // Generate random IPv4

                Ipv4Addr::new(
                    rng.gen_range(1..255),
                    rng.gen_range(0..255),
                    rng.gen_range(0..255),
                    rng.gen_range(1..255),
                )
                .to_string()
            };

            headers.insert(
                "X-My-X-Forwarded-For",
                rquest::header::HeaderValue::from_str(&forwarded_ip).unwrap(),
            );
        };

        // Send the request
        Ok(client.execute(request).await.map_err(Box::new)?)
    }
}

struct GatewayResult {
    success: bool,
    endpoint: String,
    is_new: bool,
}

async fn create_aws_client(
    region_name: &str,
    access_key_id: Option<String>,
    access_key_secret: Option<String>,
) -> Result<ApiGatewayClient, Box<dyn Error + Send + Sync>> {
    let region_provider =
        RegionProviderChain::first_try(Region::new(region_name.to_string())).or_default_provider();

    let sdk_config = if let (Some(id), Some(secret)) = (access_key_id, access_key_secret) {
        // Create static credentials provider
        let credentials_provider = SharedCredentialsProvider::new(
            aws_credential_types::Credentials::new(id, secret, None, None, "static"),
        );

        aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .credentials_provider(credentials_provider)
            .load()
            .await
    } else {
        aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await
    };

    let client = ApiGatewayClient::new(&sdk_config);
    Ok(client)
}

async fn get_gateways(
    client: &ApiGatewayClient,
) -> Result<Vec<aws_sdk_apigateway::types::RestApi>, Box<dyn Error + Send + Sync>> {
    let mut apis = Vec::new();
    let mut position = None;

    loop {
        let request = client.get_rest_apis().limit(500);

        let request = if let Some(pos) = &position {
            request.position(pos)
        } else {
            request
        };

        let response = request.send().await?;

        if let Some(items) = response.items {
            apis.extend(items);
        }

        position = response.position;

        if position.is_none() {
            break;
        }
    }

    Ok(apis)
}

#[allow(clippy::too_many_arguments)]
async fn init_gateway(
    region_name: String,
    api_name: String,
    site: String,
    access_key_id: Option<String>,
    access_key_secret: Option<String>,
    force: bool,
    require_manual_deletion: bool,
    verbose: bool,
) -> Result<GatewayResult, Box<dyn Error + Send + Sync>> {
    // Create AWS client
    let client = match create_aws_client(&region_name, access_key_id, access_key_secret).await {
        Ok(client) => client,
        Err(e) => {
            if verbose {
                println!(
                    "Failed to create AWS client for region {}: {}",
                    region_name, e
                );
            }
            return Ok(GatewayResult {
                success: false,
                endpoint: String::new(),
                is_new: false,
            });
        }
    };

    // If API gateway already exists for host, return pre-existing endpoint
    if !force {
        match get_gateways(&client).await {
            Ok(apis) => {
                for api in apis {
                    if let Some(name) = api.name {
                        if name.starts_with(&api_name) {
                            return Ok(GatewayResult {
                                success: true,
                                endpoint: format!(
                                    "{}.execute-api.{}.amazonaws.com",
                                    api.id.unwrap_or_default(),
                                    region_name
                                ),
                                is_new: false,
                            });
                        }
                    }
                }
            }
            Err(e) => {
                if verbose {
                    println!(
                        "Could not create region (some regions require manual enabling): {}",
                        region_name
                    );
                    println!("Error: {}", e);
                }
                return Ok(GatewayResult {
                    success: false,
                    endpoint: String::new(),
                    is_new: false,
                });
            }
        }
    }

    // Create simple rest API resource
    let new_api_name = if require_manual_deletion {
        format!("{} (Manual Deletion Required)", api_name)
    } else {
        api_name
    };

    let endpoint_config = EndpointConfiguration::builder()
        .types(EndpointType::Regional)
        .build();

    let create_api_response = client
        .create_rest_api()
        .name(new_api_name)
        .endpoint_configuration(endpoint_config)
        .send()
        .await?;

    let rest_api_id = create_api_response.id.unwrap_or_default();

    // Get ID for new resource
    let get_resource_response = client
        .get_resources()
        .rest_api_id(&rest_api_id)
        .send()
        .await?;

    let get_resource_response_items = get_resource_response.items.unwrap_or_default();
    let parent_resource = get_resource_response_items
        .first()
        .ok_or("No root resource found")?;
    let parent_id = parent_resource.id.as_ref().unwrap();

    // Create "Resource" (wildcard proxy path)
    let create_resource_response = client
        .create_resource()
        .rest_api_id(&rest_api_id)
        .parent_id(parent_id)
        .path_part("{proxy+}")
        .send()
        .await?;

    let resource_id = create_resource_response.id.unwrap_or_default();

    // Allow all methods to root resource
    client
        .put_method()
        .rest_api_id(&rest_api_id)
        .resource_id(parent_id)
        .http_method("ANY")
        .authorization_type("NONE")
        .request_parameters("method.request.path.proxy", true)
        .request_parameters("method.request.header.X-My-X-Forwarded-For", true)
        .send()
        .await?;

    // Make root resource route traffic to host
    client
        .put_integration()
        .rest_api_id(&rest_api_id)
        .resource_id(parent_id)
        .http_method("ANY")
        .integration_http_method("ANY")
        .set_type(Some(aws_sdk_apigateway::types::IntegrationType::HttpProxy))
        .uri(&site)
        .connection_type(aws_sdk_apigateway::types::ConnectionType::Internet)
        .request_parameters(
            "integration.request.path.proxy",
            "method.request.path.proxy",
        )
        .request_parameters(
            "integration.request.header.X-Forwarded-For",
            "method.request.header.X-My-X-Forwarded-For",
        )
        .send()
        .await?;

    // Handle proxy+ path
    client
        .put_method()
        .rest_api_id(&rest_api_id)
        .resource_id(&resource_id)
        .http_method("ANY")
        .authorization_type("NONE")
        .request_parameters("method.request.path.proxy", true)
        .request_parameters("method.request.header.X-My-X-Forwarded-For", true)
        .send()
        .await?;

    client
        .put_integration()
        .rest_api_id(&rest_api_id)
        .resource_id(&resource_id)
        .http_method("ANY")
        .integration_http_method("ANY")
        .set_type(Some(aws_sdk_apigateway::types::IntegrationType::HttpProxy))
        .uri(format!("{}/{{proxy}}", site))
        .connection_type(aws_sdk_apigateway::types::ConnectionType::Internet)
        .request_parameters(
            "integration.request.path.proxy",
            "method.request.path.proxy",
        )
        .request_parameters(
            "integration.request.header.X-Forwarded-For",
            "method.request.header.X-My-X-Forwarded-For",
        )
        .send()
        .await?;

    // Creates deployment resource, so that our API to be callable
    client
        .create_deployment()
        .rest_api_id(&rest_api_id)
        .stage_name("ProxyStage")
        .send()
        .await?;

    // Return endpoint name and whether it is newly created
    Ok(GatewayResult {
        success: true,
        endpoint: format!("{}.execute-api.{}.amazonaws.com", rest_api_id, region_name),
        is_new: true,
    })
}

async fn delete_gateway(
    region_name: String,
    api_name: String,
    access_key_id: Option<String>,
    access_key_secret: Option<String>,
    endpoints: Option<Vec<String>>,
    verbose: bool,
) -> Vec<String> {
    // Create AWS client
    let client = match create_aws_client(&region_name, access_key_id, access_key_secret).await {
        Ok(client) => client,
        Err(e) => {
            if verbose {
                println!(
                    "Failed to create AWS client for region {}: {}",
                    region_name, e
                );
            }
            return Vec::new();
        }
    };

    // Extract endpoint IDs from given endpoints
    let endpoint_ids: Option<HashSet<String>> = endpoints.map(|eps| {
        eps.iter()
            .filter_map(|endpoint| endpoint.split('.').next().map(|s| s.to_string()))
            .collect()
    });

    // Get all gateway apis (or skip if we don't have permission)
    let apis = match get_gateways(&client).await {
        Ok(apis) => apis,
        Err(e) => {
            if verbose {
                println!("Failed to get gateways for region {}: {}", region_name, e);
            }
            return Vec::new();
        }
    };

    let mut deleted = Vec::new();

    for api in apis {
        if let (Some(name), Some(id)) = (api.name, api.id) {
            // Check if hostname matches
            if name == api_name {
                // If endpoints list is given, only delete if within list
                if let Some(ref ids) = endpoint_ids {
                    if !ids.contains(&id) {
                        continue;
                    }
                }

                // Attempt delete with retry logic for rate limiting
                let mut success = false;
                for attempt in 0..3 {
                    match client.delete_rest_api().rest_api_id(&id).send().await {
                        Ok(_) => {
                            deleted.push(id.clone());
                            success = true;
                            break;
                        }
                        Err(e) => {
                            if verbose {
                                println!("Delete attempt {}: {:?}", attempt, e);
                            }
                            if attempt < 2 {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                }

                if !success && verbose {
                    println!("Failed to delete API {}", id);
                }
            }
        }
    }

    deleted
}

#[allow(dead_code)]
// Example usage
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Configuration
    let config = ApiGatewayConfig {
        site: "https://example.com".to_string(),
        regions: ApiGatewayRegion::get_default_regions(),
        access_key_id: Some("YOUR_AWS_ACCESS_KEY".to_string()),
        access_key_secret: Some("YOUR_AWS_SECRET_KEY".to_string()),
        verbose: true,
    };

    // Create and start the gateway
    let gateway = ApiGateway::new(config);
    gateway.start(false, false, Vec::new()).await;

    // Create a request using reqwest
    let client = reqwest::Client::new();
    let request = client.get("https://example.com/test").build()?;

    // Send the request through the gateway
    let response = gateway.reqwest_send(&client, request).await?;
    println!("Status: {}", response.status());

    // Shutdown when done
    gateway.shutdown(None).await;

    Ok(())
}
