use crate::db::DatabaseAdapter;
use crate::model::{PaymentEvent, WebhookEvent};
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
                    if let Err(e) = state.db.add_webhook_job(&invoice.id, &WebhookEvent::TxDetected {
                        invoice_id: invoice.id.clone(),
                        tx_hash: event.tx_hash.to_string(),
                        amount: event.amount.clone(),
                        currency: event.token.clone(),
                    }).await {
                        eprintln!("Error adding webhook job (TxDetected) for {}: {}", invoice.id, e);
                    }

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