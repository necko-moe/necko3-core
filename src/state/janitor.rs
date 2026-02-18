use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use crate::AppState;
use crate::db::DatabaseAdapter;
use crate::model::WebhookEvent;

pub fn start_janitor(state: Arc<AppState>, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);

        loop {
            interval.tick().await;
            println!("checking for expired invoices...");

            let expired_addresses = state.db.expire_old_invoices().await
                .unwrap_or_else(|e| {
                    eprintln!("failed to get expired invoices: {}", e);
                    vec![]
                });

            let mut to_remove: HashMap<String, Vec<String>> = HashMap::new();

            for (invoice_id, network, address) in expired_addresses {
                println!("marking invoice {} (address {}) as expired", invoice_id, address);

                if let Err(e) = state.db.add_webhook_job(&invoice_id, &WebhookEvent::InvoiceExpired {
                    invoice_id: invoice_id.clone(),
                }).await {
                    eprintln!("Error adding webhook job (InvoiceExpired) for {}: {}", invoice_id, e);
                }

                to_remove.entry(network)
                    .or_insert_with(Vec::new)
                    .push(address);
            }

            for (network, addresses) in to_remove {
                if let Err(e) = state.db.remove_watch_addresses_bulk(&network, &addresses).await {
                    println!("failed to remove addresses {:?} (chain '{}') from watcher: {}",
                             addresses, network, e);
                }
            }
        }
    })
}