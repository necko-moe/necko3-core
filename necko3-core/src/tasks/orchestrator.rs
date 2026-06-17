pub mod db_events;
pub mod chain_events;

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
use necko3_types::blockchain::{ChainEvent, ChainState, StateCommand, TrackTransaction};
use necko3_types::{ChainData, ChainType, InvoiceStatus, WebhookEvent};
use crate::types::{CoreEvent, ExternalEvent, NeckoEvent};

pub struct NeckoOrchestrator<D> {
    db: Arc<D>,
    db_event_rx: mpsc::Receiver<DbEvent>,

    active_workers: Arc<DashMap<String, AbortHandle>>,

    chain_event_tx: mpsc::Sender<ChainEvent>,
    chain_event_rx: mpsc::Receiver<ChainEvent>,

    core_event_tx: mpsc::Sender<NeckoEvent>,

    worker_states: Arc<DashMap<String, mpsc::Sender<StateCommand>>>,
    worker_transaction_txs: Arc<DashMap<String, mpsc::Sender<TrackTransaction>>>,
}

impl<D: DatabaseExt> NeckoOrchestrator<D> {
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

    fn get_worker_state_tx(&self, chain_name: &str) -> Option<mpsc::Sender<StateCommand>> {
        let tx = self.worker_states.get(chain_name)?;
        Some(tx.clone())
    }
}

struct PaymentDetails {
    payment_id: Uuid,
    address_to: String,
    tx_hash: String,
    block_number: u64,
    block_hash: String,
    confirmed_after: u64,
}

impl<D: DatabaseExt> NeckoOrchestrator<D> {
    async fn finalize_payment(&self, details: PaymentDetails) {
        let _info_opt = match self.db.finalize_payment(details.payment_id).await {
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

        if let Err(e) = self.core_event_tx.send(
            NeckoEvent::Core(CoreEvent::TransactionConfirmed {
            db_transaction_id: details.payment_id,
            tx_hash: details.tx_hash.clone(),
            block_number: details.block_number,
            block_hash: details.block_hash,
            confirmed_after: details.confirmed_after,
        })).await {
            warn!(error = %e, payment_id = %details.payment_id, tx_hash = details.tx_hash,
                "Failed to send CoreEvent::TransactionConfirmed event");
        };
    }
    
    async fn handle_invoice_status_change(
        &self, 
        invoice_id: Uuid, 
        paid_amount: String, 
        new_status: InvoiceStatus
    ) {
        let (webhook, core_event) = match new_status {
            InvoiceStatus::Paid => {
                let webhook = WebhookEvent::InvoicePaid {
                    invoice_id,
                    paid_amount,
                };

                let core_event = ExternalEvent::InvoicePaid { invoice_id };

                (webhook, core_event)
            }
            InvoiceStatus::Expired => {
                let webhook = WebhookEvent::InvoiceExpired { invoice_id, };
                let core_event = ExternalEvent::InvoiceExpired { invoice_id, };

                (webhook, core_event)
            }
            InvoiceStatus::Cancelled => {
                let webhook = WebhookEvent::InvoiceCancelled { invoice_id, };
                let core_event = ExternalEvent::InvoiceCancelled { invoice_id, };

                (webhook, core_event)
            }
            InvoiceStatus::Pending => { return; }  // :D
        };

        if let Err(e) = self.db.create_webhook_job(invoice_id, &webhook).await {
            warn!(error = %e, "Failed to create webhook job");
        }

        if let Err(e) = self.core_event_tx.send(
            NeckoEvent::Ext(core_event)).await {
            warn!(error = %e, "Failed to send ExternalEvent::Invoice event");
        }
    }
}