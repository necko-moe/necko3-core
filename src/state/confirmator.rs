use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use crate::AppState;
use crate::chain::BlockchainAdapter;
use crate::db::DatabaseAdapter;
use crate::model::WebhookEvent;

use tracing::{debug, error, info, instrument, trace, warn, Instrument};

#[instrument(skip(state))]
pub fn start_confirmator(state: Arc<AppState>, interval: Duration) -> JoinHandle<()> {
    info!(?interval, "Starting payment confirmator service");

    let span = tracing::info_span!(parent: None, "confirmator_service");

    tokio::spawn(async move {
        let mut interval_timer = tokio::time::interval(interval);

        loop {
            interval_timer.tick().await;

            trace!("Scanning for confirming payments...");

            let payments = match state.db.get_confirming_payments().await {
                Ok(p) => p,
                Err(e) => {
                    error!(error = %e, "Failed to fetch confirming payments from DB");
                    continue;
                }
            };

            if !payments.is_empty() {
                debug!(count = payments.len(), "Processing confirming payments batch");
            }

            for payment in payments {
                let verify_span = tracing::info_span!(
                    "verify_payment",
                    id = %payment.id,
                    tx = %payment.tx_hash,
                    net = %payment.network
                );

                async {
                    let blockchain = match state.db.get_chain(&payment.network).await {
                        Ok(Some(bc)) => bc,
                        Ok(None) => {
                            error!("Blockchain adapter not found for active payment");
                            return;
                        }
                        Err(e) => {
                            error!(error = %e, "DB error while fetching chain adapter");
                            return;
                        }
                    };

                    let (last_processed, required) = {
                        let chain_config_lock = blockchain.config();
                        let guard = chain_config_lock.read().unwrap();
                        (guard.last_processed_block, guard.required_confirmations)
                    };

                    let target_block = payment.block_number + required;

                    if last_processed < target_block {
                        trace!(
                            current = last_processed,
                            needed = target_block,
                            confirmations = required,
                            "Not enough confirmations yet"
                        );
                        return;
                    }

                    debug!("Threshold reached, verifying transaction on-chain...");

                    match blockchain.get_tx_block_number(&payment.tx_hash).await {
                        Ok(Some(actual_block)) => {
                            if actual_block != payment.block_number {
                                warn!(
                                    old_block = payment.block_number,
                                    new_block = actual_block,
                                    "Transaction moved to a different block (Chain Reorg). \
                                    Updating DB..."
                                );

                                if let Err(e) = state.db.update_payment_block(&payment.id,
                                                                              actual_block).await {
                                    error!(error = %e, "Failed to update payment block after reorg");
                                }

                                return;
                            }

                            info!(confirmations = required,
                                "Payment confirmed and verified on-chain. Finalizing...");

                            match state.db.finalize_payment(&payment.id).await {
                                Ok(true) => {
                                    info!("Invoice fully paid!");

                                    let invoice = match state.db.get_invoice(
                                        &payment.invoice_id).await
                                    {
                                        Ok(Some(invoice)) => invoice,
                                        Ok(None) => {
                                            error!(inv_id = %payment.invoice_id, "Invoice \
                                            disappeared from DB before finalization (???)");
                                            return;
                                        }
                                        Err(e) => {
                                            error!(inv_id = %payment.invoice_id, error = %e,
                                                "DB error getting invoice");
                                            return;
                                        }
                                    };

                                    let webhook_event = WebhookEvent::InvoicePaid {
                                        invoice_id: payment.invoice_id.clone(),
                                        paid_amount: invoice.paid,
                                    };

                                    if let Err(e) = state.db.add_webhook_job(&payment.invoice_id,
                                                                             &webhook_event).await {
                                        error!(error = %e, "Failed to add InvoicePaid webhook job");
                                    }

                                    debug!(address = %payment.to, "Removing address from watcher");

                                    if let Err(e) = state.db.remove_watch_address(
                                        &payment.network, &payment.to).await
                                    {
                                        error!(error = %e, "Failed to remove address from watcher");
                                    }
                                }
                                Ok(false) => {
                                    info!("Invoice isn't fully paid");

                                    let webhook_event = WebhookEvent::TxConfirmed {
                                        invoice_id: payment.invoice_id.clone(),
                                        tx_hash: payment.tx_hash,
                                        confirmations: required,
                                    };

                                    if let Err(e) = state.db.add_webhook_job(&payment.invoice_id,
                                                                             &webhook_event).await {
                                        error!(error = %e, "Failed to add TxConfirmed webhook job");
                                    }
                                },
                                Err(e) => {
                                    error!(error = %e,
                                        "CRITICAL: DB error during payment finalization")
                                },
                            }
                        }
                        Ok(None) => {
                            warn!("Transaction cannot be found in chain (possible deep reorg or \
                            dropped tx). Waiting...");
                        }
                        Err(e) => {
                            warn!(error = %e, "RPC error while verifying transaction status. Will \
                            retry.");
                        },
                    }
                }.instrument(verify_span).await;
            }
        }
    }.instrument(span))
}