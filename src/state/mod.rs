pub mod watcher;
pub mod janitor;
pub mod confirmator;

use crate::chain::BlockchainAdapter;
use crate::db::{Database, DatabaseAdapter};
use crate::model::PaymentEvent;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

pub struct AppState {
    pub api_key: String,
    pub tx: Sender<PaymentEvent>,

    pub db: Arc<Database>,
    pub active_chains: RwLock<HashMap<String, JoinHandle<()>>>,
}

impl AppState {
    pub fn new(db: Database, api_key: &str) -> (Self, Receiver<PaymentEvent>) {
        let (tx, rx): (Sender<PaymentEvent>, Receiver<PaymentEvent>) = mpsc::channel(100);

        let state = Self {
            api_key: api_key.to_owned(),
            tx,
            db: Arc::new(db),
            active_chains: RwLock::new(HashMap::new()),
        };

        (state, rx)
    }

    pub async fn init(
        db: Database,
        api_key: &str,
        janitor_timeout: Duration,
        confirmator_timeout: Duration
    ) -> anyhow::Result<Arc<AppState>> {
        let (state, rx) = Self::new(db, api_key);
        let state_arc = Arc::new(state);

        watcher::start_invoice_watcher(state_arc.clone(), rx);
        janitor::start_janitor(state_arc.clone(), janitor_timeout);
        confirmator::start_confirmator(state_arc.clone(), confirmator_timeout);
        state_arc.clone().listen_all().await?;

        Ok(state_arc)
    }

    pub async fn get_free_slot(&self, chain_name: &str) -> Option<u32> {
        let busy_indexes = match self.db.get_busy_indexes(chain_name).await {
            Ok(indexes) => indexes,
            Err(e) => {
                eprintln!("failed to get busy indexes for '{}' chain: {}", chain_name, e);
                return None
            }
        };

        for i in 0..=busy_indexes.len() {
            if !busy_indexes.contains(&(i as u32)) { return Some(i as u32); }
        }

        None // actually unreachable, but who knows
    }
}

impl AppState {
    pub async fn listen_all(self: Arc<Self>) -> anyhow::Result<()> {
        for blockchain in self.db.get_chains().await? {
            let chain_name = blockchain.config().read().unwrap().name.clone();
            let chain_name_clone = chain_name.clone();

            let db = self.db.clone();
            let tx = self.tx.clone();
            let listener = tokio::spawn(async move {
                if let Err(e) = blockchain.listen(db, tx).await {
                    eprintln!("{} listener died: {}", chain_name, e);
                }
            });

            self.active_chains.write().await.insert(chain_name_clone, listener);
        }

        Ok(())
    }
    pub async fn start_listening(self: Arc<Self>, chain: &str) -> anyhow::Result<()> {
        if self.active_chains.read().await.contains_key(chain) {
            anyhow::bail!("chain {} is already listening", chain);
        }

        let maybe_blockchain = match self.db.get_chain(chain).await {
            Ok(chain) => chain,
            Err(e) => {
                anyhow::bail!("failed to get chain '{}': {}", chain, e);
            }
        };

        let Some(blockchain) = maybe_blockchain else {
            anyhow::bail!("chain '{}' does not exist", chain)
        };

        let chain_name = blockchain.config().read().unwrap().name.clone();

        let db = self.db.clone();
        let tx = self.tx.clone();
        let listener = tokio::spawn(async move {
            if let Err(e) = blockchain.listen(db, tx).await {
                eprintln!("{} listener died: {}", chain_name, e);
            }
        });

        self.active_chains.write().await.insert(chain.to_owned(), listener);

        Ok(())
    }

    pub async fn stop_listening(&self, chain_name: &str) -> anyhow::Result<()> {
        let mut active_chains = self.active_chains.write().await;

        if let Some(handle) = active_chains.remove(chain_name) {
            handle.abort();
        } else {
            anyhow::bail!("chain {} is not listening", chain_name);
        }

        Ok(())
    }
}