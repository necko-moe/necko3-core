use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use crate::AppState;
use crate::chain::BlockchainAdapter;
use crate::db::DatabaseAdapter;
use crate::model::WebhookEvent;

pub fn start_confirmator(state: Arc<AppState>, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);

        loop {
            interval.tick().await;

            let payments = state.db.get_confirming_payments().await.unwrap_or_default();

            for payment in payments {
                let blockchain = match state.db.get_chain(&payment.network).await {
                    Ok(Some(bc)) => bc,
                    Ok(None) => {
                        eprintln!("Error getting chain {} (not found)", payment.network);
                        continue;
                    }
                    Err(e) => {
                        eprintln!("Error getting chain {}: {}", payment.network, e);
                        continue;
                    }
                };

                let (last_processed, required) = {
                    let chain_config_lock = blockchain.config();
                    let guard = chain_config_lock.read().unwrap();
                    (guard.last_processed_block, guard.required_confirmations)
                };

                if last_processed >= payment.block_number + required {
                    match blockchain.get_tx_block_number(&payment.tx_hash).await {
                        Ok(Some(actual_block)) => {
                            if actual_block != payment.block_number {
                                println!("tx moved from {} to {}. updating DB...",
                                         payment.tx_hash, actual_block);

                                if let Err(e) = state.db.update_payment_block(&payment.id, actual_block).await {
                                    eprintln!("Error updating payment block {}: {}", payment.id, e);
                                }
                                continue
                            }

                            println!("payment {} confirmed and verified! finalizing...", payment.tx_hash);
                            match state.db.finalize_payment(&payment.id).await {
                                Ok(true) => {
                                    let invoice = match state.db.get_invoice(&payment.invoice_id).await { 
                                        Ok(Some(invoice)) => invoice,
                                        Ok(None) => {
                                            eprintln!("Error getting invoice {} (not found)", payment.invoice_id);
                                            continue;
                                        }
                                        Err(e) => {
                                            eprintln!("Error getting invoice {}: {}", payment.invoice_id, e);
                                            continue;
                                        }
                                    };
                                    
                                    if let Err(e) = state.db.add_webhook_job(&payment.invoice_id, &WebhookEvent::InvoicePaid {
                                        invoice_id: payment.invoice_id.clone(),
                                        paid_amount: invoice.paid,
                                    }).await {
                                        eprintln!("Error adding webhook job (InvoicePaid) for {}: {}", payment.invoice_id, e);
                                    }
                                    if let Err(e) = state.db.remove_watch_address(
                                        &payment.network, &payment.to).await
                                    {
                                        eprintln!("Error deleting watch address {}: {}", payment.id, e);
                                    }
                                }
                                Ok(false) => {
                                    if let Err(e) = state.db.add_webhook_job(&payment.invoice_id, &WebhookEvent::TxConfirmed {
                                        invoice_id: payment.invoice_id.clone(),
                                        tx_hash: payment.tx_hash,
                                        confirmations: required,
                                    }).await {
                                        eprintln!("Error adding webhook job (TxConfirmed) for {}: {}", payment.invoice_id, e);
                                    }
                                },
                                Err(e) => eprintln!("Error finalizing payment {}: {}", payment.id, e),
                            }
                        }
                        Ok(None) => {
                            println!("cannot find transaction {} in chain (reorged?)", payment.tx_hash);
                        }
                        Err(e) => eprintln!("Confirmator error while verifying tx: {}", e),
                    }
                }
            }
        }
    })
}