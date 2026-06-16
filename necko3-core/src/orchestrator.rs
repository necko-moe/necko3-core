use necko3_blockchain::traits::worker::BlockchainWorker;
use std::collections::HashMap;
use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tracing::{debug, error, warn};
use uuid::Uuid;
use necko3_blockchain::backends::evm::EvmBlockchain;
use necko3_blockchain::traits::adapter::BlockchainAdapter;
use necko3_database::decorators::notifying::DbEvent;
use necko3_database::traits::DatabaseExt;
use necko3_types::blockchain::{Asset, ChainEvent, ChainState, StateCommand, TrackTransaction};
use necko3_types::{ChainData, ChainType, PaymentStatus, UpsertPayment};
use crate::types::CoreEvent;

pub struct NeckoOrchestrator {
    db: Arc<dyn DatabaseExt>,
    db_event_rx: mpsc::Receiver<DbEvent>,

    active_workers: Arc<DashMap<String, AbortHandle>>,

    chain_event_tx: mpsc::Sender<ChainEvent>,
    chain_event_rx: mpsc::Receiver<ChainEvent>,

    core_event_tx: mpsc::Sender<CoreEvent>,

    worker_states: Arc<DashMap<String, mpsc::Sender<StateCommand>>>,
    worker_transaction_txs: Arc<DashMap<String, mpsc::Sender<TrackTransaction>>>,
}

impl NeckoOrchestrator {
    pub async fn run(mut self) {
        let chains = self.db.get_chains().await.unwrap_or_else(|e| {
            warn!(error = %e, "Failed to initialize workers (get_chains)");
            vec![]
        });

        for chain in chains {
            self.init_chain(chain).await;
        }

        loop {
            tokio::select! {
                biased; // db in priority

                Some(db_event) = self.db_event_rx.recv() => {
                    self.process_db_event(db_event).await;
                }

                Some(chain_event) = self.chain_event_rx.recv() => {
                    self.process_chain_event(chain_event).await;
                }
            }
        }
    }

    async fn init_chain(&mut self, chain_data: ChainData) {
        debug!(chain_name = chain_data.name, "Initializing chain worker");

        let chain_name = chain_data.name.clone();

        let latest_blocks = match self.db.get_latest_indexed_blocks(chain_data.id, 100).await {
            Ok(blocks) => blocks,
            Err(e) => {
                warn!(error = %e, chain_id = chain_data.id,
                    "Failed to get latest indexed blocks for the chain");

                HashMap::new()
            }
        };

        let watch_addresses = chain_data.watch_addresses.clone();

        let mut tokens_map = HashMap::new();

        let tokens = self.db.get_tokens(&chain_data.name).await.unwrap_or_else(|e| {
            warn!(error = %e, chain_name = chain_data.name,
                    "Failed to get tokens for the chain");
            vec![]
        });
        for token in tokens {
            tokens_map.insert(token.contract.clone(), token);
        }

        let state: ChainState = chain_data.into();

        let (state_tx, state_rx) = mpsc::channel(1000);
        let (transaction_tx, transaction_rx) = mpsc::channel(1000);

        let worker_result = match chain_data.chain_type {
            ChainType::EVM => EvmBlockchain::build_worker(
                state, tokens_map, watch_addresses, latest_blocks,
                state_rx, transaction_rx, self.chain_event_tx.clone()),
        };
        let abort_handle = match worker_result {
            Ok(worker) => tokio::spawn(worker.run()).abort_handle(),
            Err(e) => {
                error!(error = %e, chain_name,
                    "Failed to build worker");

                return;
            }
        };

        if let Some(worker) = self.active_workers.insert(chain_name.clone(), abort_handle) {
            worker.abort();
            debug!(chain_name, "Previous worker task aborted");
        }

        self.worker_states.insert(chain_name.clone(), state_tx);
        self.worker_transaction_txs.insert(chain_name.clone(), transaction_tx);
    }

    async fn process_db_event(&mut self, event: DbEvent) {
        match event {
            DbEvent::ChainAdded { chain_data } => {
                self.init_chain(chain_data).await;
            }
            DbEvent::ChainRemoved { chain_data } => {
                if let Some((_, worker)) = self.active_workers.remove(&chain_data.name) {
                    worker.abort();
                }

                self.worker_states.remove(&chain_data.name);
                self.worker_transaction_txs.remove(&chain_data.name);
            }
            DbEvent::ChainPartialUpdated { chain_name, partial_update: update } => {
                let sender = match self.worker_states.get(&chain_name) {
                    Some(s) => s,
                    None => {
                        warn!(chain_name,
                            "Updated chain is not existing in self.worker_states");

                        return;
                    }
                };

                if let Some(x) = update.active
                    && let Err(e) = sender.send(StateCommand::ChangeActive(x)).await {
                    warn!(error = %e, chain_name, active = x,
                        "Failed to send StateCommand::ChangeActive to worker");
                }
                if let Some(x) = update.last_processed_block
                    && let Err(e) = sender.send(StateCommand::ChangeLastProcessedBlock(x)).await {
                    warn!(error = %e, chain_name, last_processed_block = x,
                        "Failed to send StateCommand::ChangeLastProcessedBlock to worker");
                }
                if let Some(x) = update.block_lag
                    && let Err(e) = sender.send(StateCommand::ChangeBlockLag(x)).await {
                    warn!(error = %e, chain_name, block_lag = x,
                        "Failed to send StateCommand::ChangeBlockLag to worker");
                }
                if let Some(x) = update.safe_lag
                    && let Err(e) = sender.send(StateCommand::ChangeSafeLag(x)).await {
                    warn!(error = %e, chain_name, safe_lag = x,
                        "Failed to send StateCommand::ChangeSafeLag to worker");
                }
                if let Some(x) = update.required_confirmations
                    && let Err(e) = sender.send(StateCommand::ChangeRequiredConfirmations(x)).await {
                    warn!(error = %e, chain_name, required_confirmations = x,
                        "Failed to send StateCommand::ChangeRequiredConfirmations to worker");
                }
                if let Some(x) = update.rpc_urls
                    && let Err(e) = sender.send(StateCommand::ChangeRpcUrls(x.clone())).await {
                    warn!(error = %e, chain_name, rpc_urls = ?x,
                        "Failed to send StateCommand::ChangeRpcUrls to worker");
                }
            }
            DbEvent::ChainActiveUpdated { chain_name, active } => {
                let sender = match self.worker_states.get(&chain_name) {
                    Some(s) => s,
                    None => {
                        warn!(chain_name,
                            "Updated chain is not existing in self.worker_states");

                        return;
                    }
                };

                if let Err(e) = sender.send(StateCommand::ChangeActive(active)).await {
                    warn!(error = %e, chain_name, active,
                        "Failed to send StateCommand::ChangeActive to worker");
                }
            }
            DbEvent::ChainBlockUpdated { .. } => {} // skip
            DbEvent::ChainWatchAddressAdded { chain_name, address } => {
                let sender = match self.worker_states.get(&chain_name) {
                    Some(s) => s,
                    None => {
                        warn!(chain_name,
                            "Updated chain is not existing in self.worker_states");

                        return;
                    }
                };

                if let Err(e) = sender.send(
                    StateCommand::AddWatchAddress(address.to_string())).await
                {
                    warn!(error = %e, chain_name, address,
                        "Failed to send StateCommand::AddWatchAddress to worker");
                }
            }
            DbEvent::ChainWatchAddressesRemoved { chain_name, addresses } => {
                let sender = match self.worker_states.get(&chain_name) {
                    Some(s) => s,
                    None => {
                        warn!(chain_name,
                            "Updated chain is not existing in self.worker_states");

                        return;
                    }
                };

                for address in addresses {
                    if let Err(e) = sender.send(
                        StateCommand::RemoveWatchAddress(address.to_string())).await
                    {
                        warn!(error = %e, chain_name, address,
                            "Failed to send StateCommand::AddWatchAddress to worker");
                    }
                }
            }

            DbEvent::IndexedBlocksUpserted { .. } => {} // skip

            DbEvent::TokenAdded { chain_name, token_data } => {
                let sender = match self.worker_states.get(&chain_name) {
                    Some(s) => s,
                    None => {
                        warn!(chain_name,
                            "Token added to the chain which is not existing in self.worker_states");

                        return;
                    }
                };

                let token_symbol = token_data.symbol.clone();

                if let Err(e) = sender.send(
                    StateCommand::AddTokenData(token_data)).await
                {
                    warn!(error = %e, chain_name, token_symbol,
                        "Failed to send StateCommand::AddTokenData to worker");
                }
            }
            DbEvent::TokenRemoved { chain_name, token_data } => {
                let sender = match self.worker_states.get(&chain_name) {
                    Some(s) => s,
                    None => {
                        warn!(chain_name,
                            "Token added to the chain which is not existing in self.worker_states");

                        return;
                    }
                };

                let token_contract = token_data.contract;

                if let Err(e) = sender.send(
                    StateCommand::RemoveToken { contract_address: token_contract.clone() }).await
                {
                    warn!(error = %e, chain_name, token_contract,
                        "Failed to send StateCommand::RemoveToken to worker");
                }
            }

            DbEvent::InvoiceAdded { .. } => {}
            DbEvent::InvoiceStatusUpdated { .. } => {}
            DbEvent::OldInvoicesExpired { .. } => {}
            DbEvent::InvoicePaymentApplied { .. } => {}

            DbEvent::PaymentUpserted { .. } => {}
            DbEvent::PaymentStatusUpdated { .. } => {}
            DbEvent::PaymentBlockUpdated { .. } => {}

            DbEvent::WebhookAdded { .. } => {}
            DbEvent::PendingWebhooksSelected { .. } => {}
            DbEvent::WebhookStatusUpdated { .. } => {}
            DbEvent::ScheduledNextWebhookRetry { .. } => {},
        }
    }

    async fn process_chain_event(&mut self, event: ChainEvent) {
        match event {
            ChainEvent::PaymentDetected { chain_name, tx_hash, from, to,
                asset, amount_raw, amount_human, block_number,
                block_hash, log_index, required_confirmations }
            => {
                let new_payment = UpsertPayment {
                    from,
                    to,
                    network: chain_name.clone(),
                    token: match &asset {
                        Asset::Native(sym) => sym.clone(),
                        Asset::Token(sym, _) => sym.clone(),
                    },
                    tx_hash: tx_hash.clone(),
                    amount_raw,
                    block_number,
                    block_hash: block_hash.clone(),
                    log_index,
                };

                let (payment_id, inserted) = match self.db.upsert_payment(&new_payment).await {
                    Ok(res) => res,
                    Err(e) => {
                        error!(error = %e, "Failed to upsert payment");
                        return
                    }
                };

                if inserted &&
                    let Err(e) = self.core_event_tx.send(CoreEvent::TransactionDetected {
                        db_transaction_id: payment_id,
                        tx_hash: tx_hash.clone(),
                        network: chain_name.clone(),
                        asset,
                        from: new_payment.from,
                        to: new_payment.to.clone(),
                        amount_raw,
                        amount_human,
                        block_number,
                        block_hash: block_hash.clone(),
                        log_index,
                    }).await {
                        warn!(error = %e, "Failed to send CoreEvent::NewTransaction event");
                    }

                if required_confirmations > 0 {
                    let transaction_tx = match self.worker_transaction_txs.get(&chain_name) {
                        Some(tx) => tx,
                        None => {
                            error!("self.worker_transaction_txs out of sync: unknown key {}", chain_name);
                            return
                        }
                    };

                    if let Err(e) = transaction_tx.send(TrackTransaction {
                        tx_hash: tx_hash.clone(),
                        block_number,
                        block_hash,
                        confirm_after: required_confirmations,
                    }).await {
                        warn!(error = %e, tx_hash, "Failed to send TrackTransaction request");
                    };
                } else {
                    if let Err(e) = self.db.update_payment_status(
                        payment_id, PaymentStatus::Confirmed).await
                    {
                        error!(error = %e, payment_id = %payment_id, tx_hash,
                            "Failed to update payment status");

                        return;
                    };

                    self.finalize_payment(PaymentDetails {
                        chain_name,
                        payment_id,
                        address_to: new_payment.to,
                        tx_hash,
                        block_number,
                        block_hash,
                        confirmed_after: 0,
                    }).await;
                }
            }
            ChainEvent::PaymentReorged { tx_hash, new_block_number, new_block_hash, .. } => {
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

                if let Err(e) = self.core_event_tx.send(CoreEvent::TransactionReorged {
                    db_transaction_id: payment.id,
                    tx_hash: tx_hash.clone(),
                }).await {
                    warn!(error = %e, payment_id = %payment.id, tx_hash,
                        "Failed to send CoreEvent::TransactionReorged event");
                }
            }
            ChainEvent::PaymentConfirmed { tx_hash, block_number, block_hash, confirmed_after } => {
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
                    chain_name: payment.network,
                    payment_id: payment.id,
                    address_to: payment.to,
                    tx_hash,
                    block_number,
                    block_hash,
                    confirmed_after,
                }).await;
            }
            ChainEvent::PaymentFailed { tx_hash } => {
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
                
                if let Err(e) = self.core_event_tx.send(CoreEvent::TransactionFailed {
                    db_transaction_id: payment.id,
                    tx_hash: tx_hash.clone(),
                }).await {
                    warn!(error = %e, payment_id = %payment.id, tx_hash,
                        "Failed to send CoreEvent::TransactionFailed event");
                }
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
                if let Err(e) = self.db.upsert_indexed_blocks_batch(chain_id, &new_blocks).await {
                    warn!(error = %e, chain_id, "Failed to upsert indexed blocks")
                }
                
                for tx_hash in pending_transactions.iter() {
                    if let Err(e) = self.core_event_tx.send(CoreEvent::TransactionLost {
                        tx_hash: tx_hash.clone(),
                    }).await {
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
    }
}

struct PaymentDetails {
    chain_name: String,
    payment_id: Uuid,
    address_to: String,
    tx_hash: String,
    block_number: u64,
    block_hash: String,
    confirmed_after: u64,
}

impl NeckoOrchestrator {
    async fn finalize_payment(&self, details: PaymentDetails) {
        let info_opt = match self.db.finalize_payment(details.payment_id).await {
            Ok(Some(info)) => Some(info),
            Ok(None) => {
                debug!(payment_id = %details.payment_id, invoice_address = details.address_to,
                    "Cannot find invoice for this payment");
                None
            }
            Err(e) => {
                warn!(error = %e, payment_id = %details.payment_id, "Failed to finalize payment");
                None
            }
        };

        if let Some(info) = info_opt {
            match info.is_fully_paid {
                true => {
                    if let Err(e) = self.db.remove_watch_address(&details.chain_name, &details.address_to).await {
                        warn!(error = %e, chain_name = details.chain_name, address = details.address_to,
                            "Failed to remove watch_address");
                    }

                    // send event (invoice fully paid)
                }
                false => {
                    // send event/webhook (invoice partially paid) 
                }
            }
        }

        if let Err(e) = self.core_event_tx.send(CoreEvent::TransactionConfirmed {
            db_transaction_id: details.payment_id,
            tx_hash: details.tx_hash.clone(),
            block_number: details.block_number,
            block_hash: details.block_hash,
            confirmed_after: details.confirmed_after,
        }).await {
            warn!(error = %e, payment_id = %details.payment_id, tx_hash = details.tx_hash,
                "Failed to send CoreEvent::TransactionConfirmed event");
        };
    }
}