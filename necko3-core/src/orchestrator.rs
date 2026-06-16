use necko3_blockchain::traits::worker::BlockchainWorker;
use std::collections::HashMap;
use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::{mpsc, watch};
use tokio::task::AbortHandle;
use tracing::{debug, error, warn};
use necko3_blockchain::backends::evm::EvmBlockchain;
use necko3_blockchain::traits::adapter::BlockchainAdapter;
use necko3_database::decorators::notifying::DbEvent;
use necko3_database::traits::DatabaseExt;
use necko3_types::blockchain::{Asset, ChainEvent, ChainState, TrackTransaction};
use necko3_types::{ChainData, ChainType, PaymentStatus, UpsertPayment};
use crate::types::CoreEvent;

pub struct NeckoOrchestrator {
    db: Arc<dyn DatabaseExt>,
    db_event_rx: mpsc::Receiver<DbEvent>,

    active_workers: Arc<DashMap<String, AbortHandle>>,

    chain_event_tx: mpsc::Sender<ChainEvent>,
    chain_event_rx: mpsc::Receiver<ChainEvent>,

    core_event_tx: mpsc::Sender<CoreEvent>,

    worker_states: Arc<DashMap<String, watch::Sender<ChainState>>>,
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

        let state = {
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

            let (static_data, dynamic_data) = chain_data.into();

            ChainState {
                static_data: Arc::new(static_data),
                dynamic_data: Arc::new(dynamic_data),
                tokens_map: Arc::new(tokens_map),
                watch_addresses: Arc::new(watch_addresses),
            }
        };

        let (state_tx, state_rx) = watch::channel(state);
        let (transaction_tx, transaction_rx) = mpsc::channel(1000);

        let worker_result = match chain_data.chain_type {
            ChainType::EVM => EvmBlockchain::build_worker(latest_blocks, state_rx, transaction_rx, self.chain_event_tx.clone()),
        };
        let abort_handle = match worker_result {
            Ok(worker) => tokio::spawn(worker.run()).abort_handle(),
            Err(e) => {
                error!(error = %e, chain_name,
                    "Failed to build worker");

                return;
            }
        };

        self.active_workers.insert(chain_name.clone(), abort_handle);
        self.worker_states.insert(chain_name.clone(), state_tx);
        self.worker_transaction_txs.insert(chain_name.clone(), transaction_tx);
    }

    async fn process_db_event(&mut self, event: DbEvent) {
        match event {
            DbEvent::ChainAdded { chain_data } => {
                self.init_chain(chain_data).await;
            }
            DbEvent::ChainRemoved { chain_data } => {
                self.active_workers.remove(&chain_data.name);
                
                // graceful shutdown
                self.worker_states.remove(&chain_data.name);
                self.worker_transaction_txs.remove(&chain_data.name);
            }
            DbEvent::ChainPartialUpdated { .. } => {}
            DbEvent::ChainActiveUpdated { .. } => {}
            DbEvent::ChainBlockUpdated { .. } => {}
            DbEvent::ChainWatchAddressAdded { .. } => {}
            DbEvent::ChainWatchAddressesRemoved { .. } => {}

            DbEvent::IndexedBlocksUpserted { .. } => {}
            
            DbEvent::TokenAdded { .. } => {}
            DbEvent::TokenRemoved { .. } => {}

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
                    let Err(e) = self.core_event_tx.send(CoreEvent::NewTransaction {
                        db_transaction_id: payment_id,
                        tx_hash: tx_hash.clone(),
                        network: chain_name.clone(),
                        asset,
                        from: new_payment.from,
                        to: new_payment.to,
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
                        error!(error = %e, payment_id = %payment_id,
                            "Failed to update payment status");

                        return;
                    };

                    if let Err(e) = self.core_event_tx.send(CoreEvent::TransactionConfirmed {
                        db_transaction_id: payment_id,
                        tx_hash: tx_hash.clone(),
                        block_number,
                        block_hash,
                        confirmed_after: 0,
                    }).await {
                        warn!(error = %e, payment_id = %payment_id, tx_hash,
                            "Failed to send CoreEvent::TransactionConfirmed event");
                    };
                }
            }
            ChainEvent::PaymentReorged { .. } => {}
            ChainEvent::PaymentConfirmed { .. } => {}
            ChainEvent::PaymentFailed { .. } => {}
            ChainEvent::BlockProcessed { .. } => {}
            ChainEvent::BlocksReorged { .. } => {}
        }
    }
}