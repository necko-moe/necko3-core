use alloy_primitives::BlockNumber;
use tracing::{error, warn};
use necko3_database::traits::DatabaseExt;
use necko3_types::blockchain::ChainEvent;
use necko3_types::{PaymentStatus, UpsertPayment};
use crate::tasks::orchestrator::{NeckoOrchestrator, PaymentDetails};
use crate::types::{CoreEvent, NeckoEvent};

impl<D: DatabaseExt> NeckoOrchestrator<D> {
    pub(super) async fn process_chain_event(&mut self, event: ChainEvent) {
        match event {
            ChainEvent::PaymentDetected { chain_name, tx_hash, from, to,
                asset, amount_raw, amount_human, block_number,
                block_hash, log_index, required_confirmations }
            => {
                let new_payment = UpsertPayment {
                    from,
                    to,
                    network: chain_name.clone(),
                    asset,
                    tx_hash: tx_hash.clone(),
                    amount_raw,
                    amount_human,
                    block_number,
                    block_hash: block_hash.clone(),
                    log_index,
                    required_confirmations,
                };

                if let Err(e) = self.db.upsert_payment(&new_payment).await {
                    warn!(error = %e, "Failed to upsert payment");
                }
            }
            ChainEvent::PaymentReorged { tx_hash, new_block_number, new_block_hash, .. } => {
                self.handle_payment_reorged(tx_hash, new_block_number, new_block_hash).await;
            }
            ChainEvent::PaymentConfirmed { tx_hash, block_number, block_hash, confirmed_after } => {
                self.handle_payment_confirmed(tx_hash, block_number, block_hash, confirmed_after).await;
            }
            ChainEvent::PaymentFailed { tx_hash } => {
                self.handle_payment_failed(tx_hash).await;
            }
            ChainEvent::BlockProcessed { chain_id, chain_name, block_number, block_hash } => {
                if let Err(e) = self.db.update_chain_block(&chain_name, block_number).await {
                    warn!(error = %e, chain_name, block_number,
                        "Failed to update chain block");
                }

                if let Err(e) = self.db.upsert_indexed_block(chain_id, block_number, block_hash.clone()).await {
                    warn!(error = %e, chain_id, block_number, block_hash,
                        "Failed to upsert indexed block");
                }
            }
            ChainEvent::BlocksReorged { chain_id, new_blocks, pending_transactions } => {
                self.handle_blocks_reorged(chain_id, new_blocks, pending_transactions).await;
            }
        }
    }
    
    async fn handle_payment_reorged(
        &self,
        tx_hash: String,
        new_block_number: u64,
        new_block_hash: String
    ) {
        let payment = match self.db.get_payment_by_tx_hash(tx_hash.clone()).await {
            Ok(Some(p)) => p,
            Ok(None) => {
                error!(tx_hash,
                    "Received ChainEvent::PaymentReorged, but tx_hash is not existing in the storage");
                return
            }
            Err(e) => {
                error!(error = %e, tx_hash,
                    "Failed to get payment by tx_hash");
                return
            }
        };

        if let Err(e) = self.db.update_payment_block(
            payment.id, new_block_number, new_block_hash.clone()).await
        {
            warn!(error = %e, payment_id = %payment.id, "Failed to update payment block");
        }

        if let Err(e) = self.core_event_tx.send(
            NeckoEvent::Core(CoreEvent::TransactionReorged {
                db_transaction_id: payment.id,
                tx_hash: tx_hash.clone(),
                new_block_number,
                new_block_hash,
            })).await {
            warn!(error = %e, payment_id = %payment.id, tx_hash,
                        "Failed to send CoreEvent::TransactionReorged event");
        }
    }
    
    async fn handle_payment_confirmed(
        &self,
        tx_hash: String,
        block_number: u64,
        block_hash: String,
        confirmed_after: u64,
    ) {
        let payment = match self.db.get_payment_by_tx_hash(tx_hash.clone()).await {
            Ok(Some(p)) => p,
            Ok(None) => {
                error!(tx_hash,
                    "Received ChainEvent::PaymentConfirmed, but tx_hash is not existing in the storage");
                return
            }
            Err(e) => {
                error!(error = %e, tx_hash,
                    "Failed to get payment by tx_hash");
                return
            }
        };

        self.finalize_payment(PaymentDetails {
            payment_id: payment.id,
            address_to: payment.to,
            tx_hash,
            block_number,
            block_hash,
            confirmed_after,
        }).await;
    }
    
    async fn handle_payment_failed(
        &self,
        tx_hash: String,
    ) {
        let payment = match self.db.get_payment_by_tx_hash(tx_hash.clone()).await {
            Ok(Some(p)) => p,
            Ok(None) => {
                error!(tx_hash,
                    "Received ChainEvent::PaymentFailed, but tx_hash is not existing in the storage");
                return
            }
            Err(e) => {
                error!(error = %e, tx_hash,
                    "Failed to get payment by tx_hash");
                return
            }
        };

        if let Err(e) = self.db.update_payment_status(payment.id, PaymentStatus::Failed).await {
            warn!(error = %e, payment_id = %payment.id,
                "Failed to update payment status")
        }

        if let Err(e) = self.core_event_tx.send(
            NeckoEvent::Core(CoreEvent::TransactionFailed {
                db_transaction_id: payment.id,
                tx_hash: tx_hash.clone(),
            })).await {
            warn!(error = %e, payment_id = %payment.id, tx_hash,
                "Failed to send CoreEvent::TransactionFailed event");
        }
    }
    
    async fn handle_blocks_reorged(
        &self,
        chain_id: i32,
        new_blocks: Vec<(BlockNumber, String)>,
        pending_transactions: Vec<String>,
    ) {
        if let Err(e) = self.db.upsert_indexed_blocks_batch(chain_id, &new_blocks).await {
            warn!(error = %e, chain_id, "Failed to upsert indexed blocks")
        }

        for tx_hash in pending_transactions.iter() {
            if let Err(e) = self.core_event_tx.send(
                NeckoEvent::Core(CoreEvent::TransactionLost {
                    tx_hash: tx_hash.clone(),
                })).await {
                warn!(error = %e, chain_id, tx_hash,
                            "Failed to send CoreEvent::TransactionLost event");
            }
        }

        match self.db.mark_txs_as_pending(&pending_transactions).await {
            Ok(skipped) => {
                if skipped.is_empty() {
                    return;
                }

                warn!(received_len = pending_transactions.len(), skipped_len = skipped.len(),
                            skipped = ?skipped,
                            "Received pending_transactions from ChainEvent::BlocksReorged, \
                            but these tx_hashes are not existing in the storage");
            }
            Err(e) => {
                warn!(error = %e, chain_id, "Failed to mark transactions as pending")
            }
        };
    }
}