use std::task::{Context, Poll};
use std::time::Duration;
use alloy::network::AnyNetwork;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::client::RpcClient;
use alloy::rpc::json_rpc::{ResponsePacket, ResponsePayload};
use alloy::transports::http::Http;
use alloy::transports::layers::FallbackLayer;
use alloy::transports::{BoxFuture, TransportError, TransportErrorKind};
use tower::{Layer, Service, ServiceBuilder};
use tracing::{debug, warn};
use url::Url;

#[derive(Clone)]
pub struct RpcErrorFallbackLayer;

impl<S> Layer<S> for RpcErrorFallbackLayer {
    type Service = RpcErrorFallbackService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcErrorFallbackService { inner }
    }
}

#[derive(Clone)]
pub struct RpcErrorFallbackService<S> {
    inner: S,
}

impl<S, Req> Service<Req> for RpcErrorFallbackService<S>
where
    S: Service<Req, Response = ResponsePacket, Error = TransportError> + Clone + Send + 'static,
    S::Future: Send + 'static,
    Req: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let res = match inner.call(req).await {
                Ok(r) => r,
                Err(e) => {
                    debug!("Transport/HTTP error detected on node. Details: {}", e);
                    return Err(e);
                }
            };

            let error_msg = match &res {
                ResponsePacket::Single(r) => {
                    if let ResponsePayload::Failure(err) = &r.payload {
                        Some(format!("code: {}, msg: {}", err.code, err.message))
                    } else {
                        None
                    }
                }
                ResponsePacket::Batch(responses) => {
                    responses.iter().find_map(|r| {
                        if let ResponsePayload::Failure(err) = &r.payload {
                            Some(format!("code: {}, msg: {}", err.code, err.message))
                        } else {
                            None
                        }
                    })
                }
            };

            if let Some(msg) = error_msg {
                debug!("RPC error detected, forcing fallback node change. Details: {}", msg);
                let io_err = std::io::Error::other(msg);
                return Err(TransportErrorKind::custom(io_err));
            }

            Ok(res)
        })
    }
}

pub fn create_fallback_provider(rpc_urls: &[String]) -> anyhow::Result<DynProvider<AnyNetwork>> {
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    let mut transports = Vec::new();
    for url_str in rpc_urls {
        match Url::parse(url_str) {
            Ok(url) => {
                let http_transport = Http::with_client(http_client.clone(), url);

                let wrapped = ServiceBuilder::new()
                    .layer(RpcErrorFallbackLayer)
                    .service(http_transport);

                transports.push(wrapped);
            }
            Err(e) => {
                warn!(error = %e, url = url_str, "Ignoring invalid fallback RPC URL");
            }
        }
    }

    if transports.is_empty() {
        anyhow::bail!("No RPC URLs provided")
    }

    let fallback_layer = FallbackLayer::default();

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