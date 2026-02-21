use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use crate::AppState;
use crate::db::DatabaseAdapter;
use crate::model::WebhookEvent;

use tracing::{debug, error, info, instrument, trace, warn, Instrument};

#[instrument(skip(state))]
pub fn start_janitor(state: Arc<AppState>, interval: Duration) -> JoinHandle<()> {
    info!(?interval, "Starting janitor service");

    let span = tracing::info_span!(parent: None, "janitor_service");

    tokio::spawn(async move {
        let mut interval_timer = tokio::time::interval(interval);

        loop {
            interval_timer.tick().await;

            debug!("Checking for expired invoices...");

            let expired_addresses = state.db.expire_old_invoices().await
                .unwrap_or_else(|e| {
                    error!(error = %e, "Failed to fetch/expire old invoices from DB");
                    vec![]
                });

            if expired_addresses.is_empty() {
                trace!("No expired invoices found");
                continue;
            }

            info!(count = expired_addresses.len(), "Found expired invoices, processing cleanup");

            let mut to_remove: HashMap<String, Vec<String>> = HashMap::new();

            for (invoice_id, network, address) in expired_addresses {
                let expire_span = tracing::info_span!("expire_invoice", id = %invoice_id, net = %network);

                async {
                    info!(
                        address = %address,
                        "Marking invoice as expired"
                    );

                    let webhook_job = WebhookEvent::InvoiceExpired {
                        invoice_id: invoice_id.clone(),
                    };

                    if let Err(e) = state.db.add_webhook_job(&invoice_id,
                                                                  &webhook_job).await {
                        error!(error = %e, "Failed to add InvoiceExpired webhook job");
                    }

                    to_remove.entry(network)
                        .or_insert_with(Vec::new)
                        .push(address);
                }.instrument(expire_span).await;
            }

            for (network, addresses) in to_remove {
                debug!(network = %network, count = addresses.len(),
                    "Removing addresses from watcher");

                if let Err(e) = state.db.remove_watch_addresses_bulk(
                    &network, &addresses).await
                {
                    error!(
                        network = %network,
                        addresses = ?addresses,
                        error = %e,
                        "Failed to remove addresses from watcher in bulk"
                    );
                }
            }
        }
    }.instrument(span))
}