use std::num::NonZeroUsize;
use std::time::Duration;
use alloy::network::AnyNetwork;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::client::RpcClient;
use alloy::transports::http::Http;
use alloy::transports::layers::FallbackLayer;
use tower::ServiceBuilder;
use url::Url;

pub mod evm;

pub fn create_fallback_provider(rpc_urls: &[String]) -> anyhow::Result<DynProvider<AnyNetwork>> {
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    let mut transports = Vec::new();
    for url_str in rpc_urls {
        match Url::parse(url_str) {
            Ok(url) => {
                transports.push(Http::with_client(http_client.clone(), url));
            }
            Err(e) => {
                tracing::warn!(error = %e, url = url_str, "Ignoring invalid fallback RPC URL");
            }
        }
    }

    if transports.is_empty() {
        anyhow::bail!("No RPC URLs provided")
    }

    let fallback_layer = FallbackLayer::default()
        .with_active_transport_count(NonZeroUsize::new(transports.len()).unwrap());

    let transport = ServiceBuilder::new()
        .layer(fallback_layer)
        .service(transports);

    let client = RpcClient::builder().transport(transport, false);
    let provider = ProviderBuilder::new()
        .network::<AnyNetwork>()
        .connect_client(client)
        .erased();

    Ok(provider)
}