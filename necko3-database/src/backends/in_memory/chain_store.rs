use crate::backends::in_memory::InMemoryAdapter;
use crate::error::{DbError, DbResult};
use crate::traits::ChainStore;
use async_trait::async_trait;
use necko3_types::{ChainData, PartialChainUpdate};
use std::sync::atomic::Ordering;

#[async_trait]
impl ChainStore for InMemoryAdapter {
    async fn get_chains(&self) -> DbResult<Vec<ChainData>> {
        Ok(self.chains.read().values().cloned().collect())
    }

    async fn get_chain(&self, chain_name: &str) -> DbResult<Option<ChainData>> {
        Ok(self.chains.read().get(chain_name).cloned())
    }

    async fn get_chain_by_id(&self, id: i32) -> DbResult<Option<ChainData>> {
        Ok(self.chains.read().values()
            .find(|c| c.id == id)
            .cloned())
    }

    async fn add_chain(&self, mut chain_config: ChainData) -> DbResult<ChainData> {
        let next_id = self.chains_last_id.fetch_add(1, Ordering::SeqCst);

        chain_config.id = next_id;
        
        self.chains.write()
            .insert(chain_config.name.clone(), chain_config.clone());

        Ok(chain_config)
    }

    async fn remove_chain(&self, chain_name: &str) -> DbResult<ChainData> {
        let chain_opt = self.chains.write()
            .remove(chain_name);

        let Some(chain) = chain_opt else {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })
        };

        Ok(chain)
    }

    async fn chain_exists(&self, chain_name: &str) -> DbResult<bool> {
        Ok(self.chains.read().contains_key(chain_name))
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: PartialChainUpdate) -> DbResult<ChainData> {
        let mut guard = self.chains.write();

        let chain = guard
            .get_mut(chain_name)
            .ok_or_else(|| DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })?;

        chain.patch(chain_update);

        Ok(chain.clone())
    }

    async fn update_chain_active(&self, chain_name: &str, active: bool) -> DbResult<()> {
        if !self.chains.read().contains_key(chain_name) {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })
        }

        if let Some(chain) = self.chains.write()
            .get_mut(chain_name) {

            chain.active = active;
        }

        Ok(())
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> DbResult<()> {
        if !self.chains.read().contains_key(chain_name) {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })
        }

        if let Some(chain) = self.chains.write()
            .get_mut(chain_name) {

            chain.last_processed_block = block_num;
        }

        Ok(())
    }

    async fn add_watch_address(&self, chain_name: &str, address: &str) -> DbResult<bool> {
        if !self.chains.read().contains_key(chain_name) {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })
        }

        if let Some(chain) = self.chains.write()
            .get_mut(chain_name) {

            let added = chain.watch_addresses.insert(address.to_string());
            Ok(added)
        } else { Ok(false) }
    }

    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> DbResult<bool> {
        if !self.chains.read().contains_key(chain_name) {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })
        }

        if let Some(chain) = self.chains.write()
            .get_mut(chain_name) {

            Ok(chain.watch_addresses.remove(address))
        } else { Ok(false) }
    }

    async fn remove_watch_addresses(&self, chain_name: &str, addresses: &[String]) -> DbResult<Vec<String>> {
        if !self.chains.read().contains_key(chain_name) {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })
        }

        if let Some(chain) = self.chains.write()
            .get_mut(chain_name) {

            let mut removed = Vec::new();

            for address in addresses {
                if chain.watch_addresses.remove(address) {
                    removed.push(address.clone());
                }
            }

            Ok(removed)
        } else { Ok(vec![]) }
    }
}