use crate::chain::{Blockchain, BlockchainAdapter};
use crate::db::{Database, DatabaseAdapter};
use crate::model::{InvoiceStatus, PaymentEvent};
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

    pub async fn init(db: Database, api_key: &str, janitor_timeout: Duration) -> anyhow::Result<Arc<AppState>> {
        let (state, rx) = Self::new(db, api_key);
        let state_arc = Arc::new(state);

        state_arc.clone().start_invoice_watcher(rx);
        state_arc.clone().start_janitor(janitor_timeout);
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
    pub fn start_invoice_watcher(self: Arc<Self>, mut rx: Receiver<PaymentEvent>) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let Ok(Some(invoice)) = self.db.get_pending_invoice_by_address(
                    &event.network, &event.to).await
                else {
                    eprintln!("(???) unable to find invoice for {}", event.to);
                    continue;
                };

                if event.network != invoice.network || event.token != invoice.token {
                    continue;
                }

                let now = chrono::Utc::now();
                if invoice.expires_at < now {
                    println!("detected transaction on EXPIRED invoice. skipping \
                        (you lost your tokens idiot)");
                    continue
                }

                if invoice.status == InvoiceStatus::Paid {
                    println!("(unreachable) invoice is already paid but {} received something",
                             invoice.address)
                }

                let (paid_raw, paid) = match self.db.add_payment(&invoice.id, event.amount_raw).await {
                    Ok((pr, p)) => (pr, p),
                    Err(e) => {
                        eprintln!("failed to add payment for {}: {}. skipping...", invoice.id, e);
                        continue;
                    }
                };

                println!("\npaid {}/{} {} on {} (index {})",
                         paid,
                         invoice.amount,
                         invoice.token,
                         invoice.address,
                         invoice.address_index);

                if paid_raw >= invoice.amount_raw {
                    println!("invoice {} is fully paid!", invoice.id);

                    if let Err(e) = self.db.set_invoice_status(&invoice.id,
                                                               InvoiceStatus::Paid).await {
                        eprintln!("failed to mark invoice status as paid: {}", e)
                    }

                    if let Err(e) = self.db.remove_watch_address(&invoice.network,
                                                                 &invoice.address).await {
                        eprintln!("FATAL: failed to remove address {} from watcher: {}",
                                  invoice.address, e);
                    }
                }
            }
        })
    }

    pub fn start_janitor(self: Arc<Self>, interval: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);

            loop {
                interval.tick().await;
                println!("checking for expired invoices...");

                let expired_addresses = self.db.expire_old_invoices().await
                    .unwrap_or_else(|e| {
                        eprintln!("failed to get expired invoices: {}", e);
                        vec![]
                    });


                let mut to_remove: HashMap<String, Vec<String>> = HashMap::new();

                for (address, network, invoice_id) in expired_addresses {
                    println!("marking invoice {} (address {}) as expired", invoice_id, address);

                    to_remove.entry(network)
                        .or_insert_with(Vec::new)
                        .push(address);
                }

                for (network, addresses) in to_remove {
                    if let Err(e) = self.db.remove_watch_addresses_bulk(&network, &addresses).await {
                        println!("failed to remove addresses {:?} (chain '{}') from watcher: {}",
                                 addresses, network, e);
                    }
                }
            }
        })
    }
}

impl AppState {
    pub async fn listen_all(self: Arc<Self>) -> anyhow::Result<()> {
        for chain in self.db.get_chains().await? {
            let chain_name = chain.name.clone();
            let blockchain = Blockchain::new(self.clone(), chain.chain_type,
                                             &chain_name, Some(self.tx.clone()));
            let listener = tokio::spawn(async move {
                if let Err(e) = blockchain.listen().await {
                    eprintln!("{} listener died: {}", chain_name, e);
                }
            });

            self.active_chains.write().await.insert(chain.name.clone(), listener);
        }

        Ok(())
    }
    pub async fn start_listening(self: Arc<Self>, chain: &str) -> anyhow::Result<()> {
        if self.active_chains.read().await.contains_key(chain) {
            anyhow::bail!("chain {} is already listening", chain);
        }

        let maybe_chain_config = match self.db.get_chain(chain).await {
            Ok(chain) => chain,
            Err(e) => {
                anyhow::bail!("failed to get chain '{}': {}", chain, e);
            }
        };

        let Some(chain_config) = maybe_chain_config else {
            anyhow::bail!("chain '{}' does not exist", chain)
        };

        let chain_name = chain_config.name.clone();
        let blockchain = Blockchain::new(self.clone(), chain_config.chain_type,
                                         &chain_name, Some(self.tx.clone()));
        let listener = tokio::spawn(async move {
            if let Err(e) = blockchain.listen().await {
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