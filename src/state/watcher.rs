use crate::db::DatabaseAdapter;
use crate::model::PaymentEvent;
use crate::AppState;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

pub fn start_invoice_watcher(state: Arc<AppState>, mut rx: Receiver<PaymentEvent>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let Ok(Some(invoice)) = state.db.get_pending_invoice_by_address(
                &event.network, &event.to).await
            else {
                eprintln!("(???) unable to find invoice for {}", event.to);
                continue;
            };

            if event.network != invoice.network || event.token != invoice.token {
                continue;
            }

            let now = chrono::Utc::now();
            if invoice.expires_at < now {
                println!("detected transaction on EXPIRED invoice. skipping \
                        (you lost your tokens idiot)");
                continue
            }

            match state.db.add_payment_attempt(
                &invoice.id,
                &event.from,
                &event.to,
                &event.tx_hash.to_string(),
                event.amount_raw,
                event.block_number,
                &event.network
            ).await {
                Ok(_) => {
                    println!(
                        "payment detected: {} {} for invoice {}. waiting for confirmations...",
                        event.amount,
                        event.token,
                        invoice.id
                    );
                }
                Err(e) => {
                    eprintln!("failed to add payment attempt: {}", e);
                }
            }
        }
    })
}