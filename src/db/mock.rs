use crate::config::{ChainConfig, TokenConfig};
use crate::db::DatabaseAdapter;
use crate::model::{Invoice, InvoiceStatus, PartialChainUpdate};
use alloy::primitives::utils::format_units;
use alloy::primitives::U256;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

pub struct MockDatabase {
    chains: RwLock<HashMap<String, ChainConfig>>, // key = chain name
    invoices: DashMap<String, Invoice>, // key = id/uuid
    token_decimals: RwLock<HashMap<String, HashMap<String, u8>>> // (chain_name, (token_symbol, decimals))
}

impl MockDatabase {
    pub fn new() -> Self {
        Self {
            chains: RwLock::new(HashMap::new()),
            invoices: DashMap::new(),
            token_decimals: RwLock::new(HashMap::new()),
        }
    }
}

impl DatabaseAdapter for MockDatabase {
    async fn get_chains_map(&self) -> anyhow::Result<HashMap<String, ChainConfig>> {
        Ok(self.chains.read().unwrap().clone())
    }

    async fn get_chains(&self) -> anyhow::Result<Vec<ChainConfig>> {
        Ok(self.chains.read().unwrap()
            .iter()
            .map(|x| x.1.clone())
            .collect::<Vec<_>>())
    }

    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainConfig>> {
        Ok(self.chains.read().unwrap().get(chain_name).cloned())
    }

    async fn get_chain_by_id(&self, id: u32) -> anyhow::Result<Option<ChainConfig>> {
        unimplemented!("mock database does not have ids")
    }

    async fn add_chain(&self, chain_config: &ChainConfig) -> anyhow::Result<()> {
        match self.chains.read().unwrap().contains_key(&chain_config.name) {
            true => anyhow::bail!("chain with name {} already exists", chain_config.name),
            false => {},
        };

        self.chains.write().unwrap()
            .insert(chain_config.name.clone(), chain_config.clone());

        Ok(())
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
        match self.chains.write().unwrap().get_mut(chain_name) {
            Some(c) => c.last_processed_block = block_num,
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        }

        Ok(())
    }

    async fn get_latest_block(&self, chain_name: &str) -> anyhow::Result<Option<u64>> {
        match self.chains.write().unwrap().get(chain_name) {
            Some(c) => Ok(Some(c.last_processed_block)),
            None => Ok(None),
        }
    }

    async fn get_chains_with_token(&self, token_symbol: &str) -> anyhow::Result<Vec<ChainConfig>> {
        Ok(self.chains.read().unwrap().iter()
            .map(|x| x.1)
            .filter(|x| x.tokens.read().unwrap().iter()
                .any(|y| y.symbol == token_symbol))
            .cloned()
            .collect()
        )
    }

    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<()> {
        self.chains.write().unwrap().remove(chain_name);
        Ok(())
    }

    async fn remove_chain_by_id(&self, id: u32) -> anyhow::Result<()> {
        unimplemented!("mock database does not have ids")
    }

    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool> {
        Ok(self.chains.read().unwrap().contains_key(chain_name))
    }

    async fn update_chain_partial(&self, chain_name: &str, update_chain_req: &PartialChainUpdate) -> anyhow::Result<()> {
        let mut guard = self.chains.write().unwrap();
        let chain = guard.get_mut(chain_name)
            .ok_or_else(|| anyhow::anyhow!("chain '{}' does not exist", chain_name))?;

        if let Some(xpub) = &update_chain_req.xpub {
            chain.xpub = xpub.to_owned();
        }

        if let Some(rpc_url) = &update_chain_req.rpc_url {
            chain.rpc_url = rpc_url.to_owned();
        }

        if let Some(last_processed_block) = update_chain_req.last_processed_block {
            chain.last_processed_block = last_processed_block;
        }

        if let Some(block_lag) = update_chain_req.block_lag {
            chain.block_lag = block_lag;
        }

        Ok(())
    }

    async fn get_watch_addresses(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => Ok(Some(c.watch_addresses.read().unwrap().iter()
                .cloned()
                .collect())),
            None => Ok(None),
        }
    }

    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<()> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => {
                c.watch_addresses.write().unwrap()
                    .retain(|x| x != address);
            }
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        }

        Ok(())
    }

    async fn remove_watch_addresses_bulk(
        &self,
        chain_name: &str,
        addresses: &[String]
    ) -> anyhow::Result<()> {
        let to_remove: HashSet<&str> = addresses.iter()
            .map(|s| s.as_ref()).collect();

        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => {
                c.watch_addresses.write().unwrap()
                    .retain(|x: &String| !to_remove.contains(x.as_str()));
            }
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        }

        Ok(())
    }

    async fn add_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<()> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => {
                c.watch_addresses.write().unwrap().insert(address.to_owned());
            }
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        }

        Ok(())
    }

    async fn get_xpub(&self, chain_name: &str) -> anyhow::Result<Option<String>> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => Ok(Some(c.xpub.clone())),
            None => Ok(None),
        }
    }

    async fn get_rpc_url(&self, chain_name: &str) -> anyhow::Result<Option<String>> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => Ok(Some(c.rpc_url.clone())),
            None => Ok(None),
        }
    }

    async fn get_block_lag(&self, chain_name: &str) -> anyhow::Result<Option<u8>> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => Ok(Some(c.block_lag)),
            None => Ok(None),
        }
    }

    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Option<Vec<TokenConfig>>> {
        // actually I can do all of them with mappings, but I won't hehe (not now)
        Ok(self.chains.read().unwrap().get(chain_name)
            .map(|c| c.tokens.read().unwrap().iter()
                .cloned()
                .collect()))
    }

    async fn get_token_contracts(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => Ok(Some(c.tokens.read().unwrap().iter()
                .map(|tc| tc.contract.clone())
                .collect())),
            None => Ok(None),
        }
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenConfig>> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => Ok(c.tokens.read().unwrap().iter()
                .find(|tc| tc.symbol == token_symbol)
                .cloned()),
            None => Ok(None),
        }
    }

    async fn get_token_by_id(&self, chain_name: &str, id: u32) -> anyhow::Result<Option<TokenConfig>> {
        unimplemented!("mock database does not have ids")
    }

    async fn get_token_by_contract(&self, chain_name: &str, contract_address: &str) -> anyhow::Result<Option<TokenConfig>> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => Ok(c.tokens.read().unwrap().iter()
                .find(|tc| tc.contract == contract_address)
                .cloned()),
            None => Ok(None),
        }
    }

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<()> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => c.tokens.write().unwrap()
                .retain(|tc| tc.symbol != token_symbol),
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        }

        Ok(())
    }

    async fn remove_token_by_id(&self, chain_name: &str, id: u32) -> anyhow::Result<()> {
        unimplemented!("mock database does not have ids")
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenConfig) -> anyhow::Result<()> {
        let guard = self.chains.read().unwrap();

        let chain = match guard.get(chain_name) {
            Some(c) => c,
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        };

        let contains = chain.tokens.read().unwrap().iter()
            .any(|tc| tc.symbol == token_config.symbol);

        match contains {
            true => anyhow::bail!("token '{}' already exists", token_config.symbol),
            false => chain.tokens.write().unwrap().insert(token_config.clone()),
        };

        Ok(())
    }

    async fn get_invoices(&self) -> anyhow::Result<Vec<Invoice>> {
        Ok(self.invoices.iter()
            .map(|x| x.value().clone())
            .collect())
    }

    async fn get_invoices_by_chain(&self, chain_name: &str) -> anyhow::Result<Vec<Invoice>> {
        Ok(self.invoices.iter()
            .map(|x| x.value().clone())
            .filter(|inv| inv.network == chain_name)
            .collect())
    }

    async fn get_invoices_by_token(&self, token_symbol: &str) -> anyhow::Result<Vec<Invoice>> {
        Ok(self.invoices.iter()
            .map(|x| x.value().clone())
            .filter(|inv| inv.token == token_symbol)
            .collect())
    }

    async fn get_invoices_by_address(&self, address: &str) -> anyhow::Result<Vec<Invoice>> {
        Ok(self.invoices.iter()
            .map(|x| x.value().clone())
            .filter(|inv| inv.address == address)
            .collect())
    }

    async fn get_invoice(&self, uuid: &str) -> anyhow::Result<Option<Invoice>> {
        Ok(self.invoices.get(uuid).map(|x| x.value().clone()))
    }

    async fn get_invoices_by_status(&self, status: InvoiceStatus) -> anyhow::Result<Vec<Invoice>> {
        Ok(self.invoices.iter()
            .map(|x| x.value().clone())
            .filter(|inv| inv.status == status)
            .collect())
    }

    async fn get_invoices_by_chain_and_status(&self, chain_name: &str, status: InvoiceStatus) -> anyhow::Result<Vec<Invoice>> {
        Ok(self.invoices.iter()
            .map(|x| x.value().clone())
            .filter(|inv| inv.network == chain_name && inv.status == status)
            .collect())
    }

    async fn get_invoices_by_address_and_status(&self, address: &str, status: InvoiceStatus) -> anyhow::Result<Vec<Invoice>> {
        Ok(self.invoices.iter()
            .map(|x| x.value().clone())
            .filter(|inv| inv.address == address && inv.status == status)
            .collect())
    }

    async fn get_busy_indexes(&self, chain_name: &str) -> anyhow::Result<Vec<u32>> {
        Ok(self.invoices.iter()
            .filter(|i| i.status == InvoiceStatus::Pending
                && i.network == chain_name)
            .map(|i| i.value().address_index)
            .collect())
    }

    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()> {
        if self.invoices.contains_key(&invoice.id) {
            anyhow::bail!("invoice '{}' already exists", invoice.id);
        }

        self.invoices.insert(invoice.id.clone(), invoice.clone());

        Ok(())
    }

    async fn set_invoice_status(&self, uuid: &str, status: InvoiceStatus) -> anyhow::Result<()> {
        match self.invoices.get_mut(uuid) {
            Some(mut inv) => inv.status = status,
            None => anyhow::bail!("invoice '{}' does not exist", uuid),
        }

        Ok(())
    }

    async fn add_payment(&self, uuid: &str, amount_raw: U256) -> anyhow::Result<(U256, String)> {
        let mut inv = match self.invoices.get_mut(uuid) {
            Some(inv) => inv,
            None => anyhow::bail!("invoice '{}' does not exist", uuid),
        };

        inv.paid_raw += amount_raw;

        let amount_human = format_units(inv.paid_raw, inv.decimals)?;
        inv.paid = amount_human;

        Ok((inv.paid_raw, inv.paid.clone()))
    }

    async fn get_pending_invoice_by_address(&self, chain_name: &str, address: &str) -> anyhow::Result<Option<Invoice>> {
        Ok(self.invoices.iter()
            .map(|x| x.value().clone())
            .find(|inv| inv.network == chain_name
                && inv.address == address
                && inv.status == InvoiceStatus::Pending))
    }

    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        let now = chrono::Utc::now();

        let mut old_invoices: Vec<(String, String, String)> = vec![];

        self.invoices.iter_mut()
            .filter(|inv| inv.status == InvoiceStatus::Pending
                && inv.expires_at <= now)
            .for_each(|mut inv| {
                inv.status = InvoiceStatus::Expired;
                old_invoices.push((inv.id.clone(), inv.network.clone(), inv.address.clone()))
            });

        Ok(old_invoices)
    }

    async fn is_invoice_expired(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        Ok(self.invoices.iter()
            .find(|inv| inv.id == uuid)
            .map(|inv| inv.status == InvoiceStatus::Expired))
    }

    async fn is_invoice_paid(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        Ok(self.invoices.iter()
            .find(|inv| inv.id == uuid)
            .map(|inv| inv.status == InvoiceStatus::Paid))
    }

    async fn is_invoice_pending(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        Ok(self.invoices.iter()
            .find(|inv| inv.id == uuid)
            .map(|inv| inv.status == InvoiceStatus::Pending))
    }

    async fn remove_invoice(&self, uuid: &str) -> anyhow::Result<()> {
        self.invoices.remove(uuid);

        Ok(())
    }

    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        if let Some(decimals) = self._get_token_decimals(chain_name, token_symbol)?
        {
            return Ok(Some(decimals))
        }

        let chain_config = {
            let guard = self.chains.read().unwrap();

            let Some(cc) = guard.get(chain_name).cloned() else {
                return Ok(None)
            };

            cc
        };

        if token_symbol == chain_config.native_symbol {
            self._insert_token_decimals(chain_name, token_symbol, chain_config.decimals)?;

            return Ok(Some(chain_config.decimals));
        }

        match chain_config.tokens.read().unwrap().iter()
            .find(|tc| tc.symbol == token_symbol) {
            Some(tc) => {
                self._insert_token_decimals(chain_name, token_symbol, tc.decimals)?;

                Ok(Some(tc.decimals))
            },
            None => Ok(None),
        }
    }
}

impl MockDatabase {
    fn _insert_token_decimals(&self, chain_name: &str, token_symbol: &str, decimals: u8) -> anyhow::Result<()> {
        let mut write_guard = self.token_decimals.write().unwrap();
        let inner_map = write_guard
            .entry(chain_name.to_string())
            .or_insert_with(HashMap::new);

        inner_map.insert(token_symbol.to_string(), decimals);

        Ok(())
    }

    fn _get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        Ok(self.token_decimals.read().unwrap()
            .get(chain_name)
            .and_then(|c| c.get(token_symbol)
                .cloned()))
    }
}