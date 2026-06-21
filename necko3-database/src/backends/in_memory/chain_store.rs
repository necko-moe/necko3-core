use std::sync::atomic::Ordering;
use async_trait::async_trait;
use necko3_types::{ChainData, PartialChainUpdate};
use crate::backends::in_memory::InMemoryAdapter;
use crate::traits::ChainStore;

#[async_trait]
impl ChainStore for InMemoryAdapter {
    async fn get_chains(&self) -> anyhow::Result<Vec<ChainData>> {
        Ok(self.chains.read().values().cloned().collect())
    }

    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainData>> {
        Ok(self.chains.read().get(chain_name).cloned())
    }

    async fn get_chain_by_id(&self, id: i32) -> anyhow::Result<Option<ChainData>> {
        Ok(self.chains.read().values()
            .find(|c| c.id == id)
            .cloned())
    }

    async fn add_chain(&self, chain_config: &ChainData) -> anyhow::Result<()> {
        let mut chain_config = chain_config.clone();
        chain_config.id = self.chains_last_id.fetch_add(1, Ordering::SeqCst) ;
        
        self.chains.write()
            .insert(chain_config.name.clone(), chain_config);

        Ok(())
    }

    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainData>> {
        let deleted = self.chains.write()
            .remove(chain_name);

        Ok(deleted)
    }

    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool> {
        Ok(self.chains.read().contains_key(chain_name))
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()> {
        let mut guard = self.chains.write();

        let chain = guard
            .get_mut(chain_name)
            .ok_or_else(|| anyhow::anyhow!("Chain {} not found in DB", chain_name))?;

        chain.patch(chain_update);

        Ok(())
    }

    async fn update_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()> {
        if let Some(chain) = self.chains.write()
            .get_mut(chain_name)
        {
            chain.active = active;
        }

        Ok(())
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
        if let Some(chain) = self.chains.write()
            .get_mut(chain_name)
        {
            chain.last_processed_block = block_num;
        }

        Ok(())
    }

    async fn add_watch_address(&self, chain_name: &str, address: String) -> anyhow::Result<bool> {
        if let Some(chain) = self.chains.write()
            .get_mut(chain_name)
        {
            let added = chain.watch_addresses.insert(address);

            Ok(added)
        } else { Ok(false) }
    }

    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<bool> {
        if let Some(chain) = self.chains.write()
            .get_mut(chain_name)
        {
            Ok(chain.watch_addresses.remove(address))
        } else { Ok(false) }
    }

    async fn remove_watch_addresses(&self, chain_name: &str, addresses: &[String]) -> anyhow::Result<Vec<String>> {
        if let Some(chain) = self.chains.write()
            .get_mut(chain_name)
        {
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