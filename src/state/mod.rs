pub mod watcher;
pub mod janitor;
pub mod confirmator;
mod webhook;

use crate::chain::BlockchainAdapter;
use crate::db::{Database, DatabaseAdapter};
use crate::model::PaymentEvent;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

use tracing::{debug, error, info, instrument, warn, Instrument};

pub struct AppState {
    pub api_key: String,
    pub tx: Sender<PaymentEvent>,

    pub db: Arc<Database>,
    pub active_chains: RwLock<HashMap<String, JoinHandle<()>>>,
}

impl AppState {
    #[instrument(skip(db, api_key))]
    pub fn new(db: Database, api_key: &str) -> (Self, Receiver<PaymentEvent>) {
        debug!("Creating new AppState channels for the watcher");
        let (tx, rx): (Sender<PaymentEvent>, Receiver<PaymentEvent>) = mpsc::channel(100);

        let state = Self {
            api_key: api_key.to_owned(),
            tx,
            db: Arc::new(db),
            active_chains: RwLock::new(HashMap::new()),
        };

        (state, rx)
    }

    #[instrument(skip(db, api_key), err)]
    pub async fn init(
        db: Database,
        api_key: &str,
        janitor_timeout: Duration,
        confirmator_timeout: Duration
    ) -> anyhow::Result<Arc<AppState>> {
        info!("Initializing AppState and starting background services");

        let (state, rx) = Self::new(db, api_key);
        let state_arc = Arc::new(state);

        debug!("Starting invoice watcher...");
        watcher::start_invoice_watcher(state_arc.clone(), rx);

        debug!(?janitor_timeout, "Starting janitor...");
        janitor::start_janitor(state_arc.clone(), janitor_timeout);

        debug!(?confirmator_timeout, "Starting confirmator...");
        confirmator::start_confirmator(state_arc.clone(), confirmator_timeout);

        debug!("Starting webhook dispatcher...");
        webhook::start_webhook_dispatcher(state_arc.clone());

        debug!("Firing up chain listeners...");
        state_arc.clone().listen_all().await?;

        info!("AppState initialization completed successfully");
        Ok(state_arc)
    }

    #[instrument(skip(self))]
    pub async fn get_free_slot(&self, chain_name: &str) -> Option<u32> {
        debug!("Requesting free slot");
        let busy_indexes = match self.db.get_busy_indexes(chain_name).await {
            Ok(indexes) => indexes,
            Err(e) => {
                error!(chain = chain_name, error = %e, "Failed to get busy indexes from DB");
                return None
            }
        };

        for i in 0..=busy_indexes.len() as u32 {
            if !busy_indexes.contains(&(i)) {
                debug!(slot = i, "Found free slot");
                return Some(i);
            }
        }

        warn!("Could not find a free slot (unreachable spot is actually reachable?)");
        None
    }
}

impl AppState {
    #[instrument(skip(self), err)]
    pub async fn listen_all(self: Arc<Self>) -> anyhow::Result<()> {
        info!("Starting to listen to all configured blockchains");

        for blockchain in self.db.get_chains().await? {
            let chain_name = blockchain.config().read().unwrap().name.clone();

            debug!(chain = chain_name, "Spawning listener for chain");

            let db = self.db.clone();
            let tx = self.tx.clone();

            let span = tracing::info_span!(parent: None, "chain_listener");

            let listener = tokio::spawn(async move {
                if let Err(e) = blockchain.listen(db, tx).await {
                    error!(error = %e, "Blockchain listener task died");
                }
            }.instrument(span));

            self.active_chains.write().await.insert(chain_name, listener);
        }

        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn start_listening(self: Arc<Self>, chain: &str) -> anyhow::Result<()> {
        info!("Trying to start listener for a specific chain");

        if self.active_chains.read().await.contains_key(chain) {
            anyhow::bail!("Chain {} is already listening", chain);
        }

        let maybe_blockchain = match self.db.get_chain(chain).await {
            Ok(chain) => chain,
            Err(e) => {
                anyhow::bail!("Failed to get chain '{}': {}", chain, e);
            }
        };

        let Some(blockchain) = maybe_blockchain else {
            anyhow::bail!("Chain '{}' does not exist", chain)
        };

        let chain_name = blockchain.config().read().unwrap().name.clone();
        debug!(chain = chain_name, "Chain found, spawning task");

        let db = self.db.clone();
        let tx = self.tx.clone();

        let span = tracing::info_span!(parent: None, "chain_listener");

        let listener = tokio::spawn(async move {
            if let Err(e) = blockchain.listen(db, tx).await {
                error!(error = %e, "Blockchain listener task died");
            }
        }.instrument(span));

        self.active_chains.write().await.insert(chain_name, listener);

        info!("Successfully started listening");
        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn stop_listening(&self, chain_name: &str) -> anyhow::Result<()> {
        info!("Trying to stop chain listener");

        let mut active_chains = self.active_chains.write().await;

        if let Some(handle) = active_chains.remove(chain_name) {
            handle.abort();
            debug!("Task handle aborted successfully");
        } else {
            anyhow::bail!("Chain {} is not listening", chain_name);
        }

        Ok(())
    }
}