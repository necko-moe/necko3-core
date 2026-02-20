use crate::db::DatabaseAdapter;
use crate::model::{PaymentEvent, WebhookEvent};
use crate::AppState;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use tracing::{debug, error, info, instrument, warn, Instrument};

#[instrument(skip_all)]
pub fn start_invoice_watcher(state: Arc<AppState>, mut rx: Receiver<PaymentEvent>) -> JoinHandle<()> {
    info!("Starting invoice watcher service");

    let span = tracing::info_span!("invoice_watcher_loop");

    tokio::spawn(async move {
        debug!("Invoice watcher loop started, waiting for events...");

        while let Some(event) = rx.recv().await {
            let process_span = tracing::info_span!(
                "process_payment",
                tx_hash = %event.tx_hash,
                amount = %event.amount,
                network = %event.network,
                token = %event.token
            );

            async {
                debug!("Processing new payment event");

                let invoice = match state.db.get_pending_invoice_by_address(
                    &event.network, &event.to).await
                {
                    Ok(Some(inv)) => inv,
                    Ok(None) => {
                        warn!(to_address = %event.to,
                            "Received payment to an address with no pending invoice \
                            (orphan payment?)");
                        return;
                    }
                    Err(e) => {
                        error!(error = %e, "DB error while fetching invoice");
                        return;
                    }
                };

                if event.network != invoice.network || event.token != invoice.token {
                    warn!(
                        expected_network = %invoice.network,
                        expected_token = %invoice.token,
                        got_network = %event.network,
                        got_token = %event.token,
                        "Payment mismatch: received wrong token or network for this invoice"
                    );
                    return;
                }

                match state.db.add_payment_attempt(
                    &invoice.id,
                    &event.from,
                    &event.to,
                    &event.tx_hash.to_string(),
                    event.amount_raw,
                    event.block_number,
                    &event.network,
                    event.log_index
                ).await {
                    Ok(_) => {
                        info!(invoice_id = %invoice.id,
                            "Payment successfully linked to invoice. Waiting for confirmations...");

                        let webhook_event = WebhookEvent::TxDetected {
                            invoice_id: invoice.id.clone(),
                            tx_hash: event.tx_hash.to_string(),
                            amount: event.amount.clone(),
                            currency: event.token.clone(),
                        };

                        if let Err(e) = state.db.add_webhook_job(
                            &invoice.id, &webhook_event).await
                        {
                            error!(
                                invoice_id = %invoice.id,
                                error = %e,
                                "Failed to add TxDetected webhook job"
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            invoice_id = %invoice.id,
                            error = %e,
                            "CRITICAL: Failed to save payment attempt to DB"
                        );
                    }
                }
            }.instrument(process_span).await;
        }

        warn!("Invoice watcher channel closed, service stopping");
    }.instrument(span))
}