use crate::chain::{Blockchain, BlockchainAdapter};
use crate::db::DatabaseAdapter;
use crate::model::{ChainConfig, ChainType, Invoice, InvoiceStatus, PartialChainUpdate, Payment, PaymentStatus, TokenConfig, WebhookEvent, WebhookJob, WebhookStatus};
use alloy::primitives::utils::format_units;
use alloy::primitives::U256;
use sqlx::postgres::PgRow;
use sqlx::types::BigDecimal;
use sqlx::{PgPool, Row};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

pub struct Postgres {
    pool: PgPool,

    // cache
    chains_cache: RwLock<HashMap<String, Arc<Blockchain>>>, // key = chain name
    token_decimals: RwLock<HashMap<String, HashMap<String, u8>>> // (chain_name, (token_symbol, decimals))
}

impl Postgres {
    pub async fn init(pool: PgPool) -> anyhow::Result<Self> {
        let mut chains_map: HashMap<String, Arc<Blockchain>> = HashMap::new();
        let mut decimals_map: HashMap<String, HashMap<String, u8>> = HashMap::new();

        // cache this shit for tokens
        let mut chain_id_to_name: HashMap<i32, String> = HashMap::new();

        for row in sqlx::query(
            r#"SELECT id, name, rpc_url, chain_type, xpub, native_symbol, decimals,
       last_processed_block, block_lag, required_confirmations FROM chains"#
        )
            .fetch_all(&pool)
            .await?
        {
            let id: i32 = row.get("id");
            let name: String = row.get("name");

            let chain_str: String = row.get("chain_type");
            let chain_type: ChainType = chain_str.parse()
                .map_err(|e| anyhow::anyhow!("Invalid chain type: {}", e))?;

            let config = ChainConfig {
                name: name.clone(),
                rpc_url: row.get("rpc_url"),
                chain_type,
                xpub: row.get("xpub"),
                native_symbol: row.get("native_symbol"),
                decimals: row.get::<i16, _>("decimals") as u8,
                last_processed_block: row.get::<i64, _>("last_processed_block") as u64,
                block_lag: row.get::<i16, _>("block_lag") as u8,
                required_confirmations: row.get::<i64, _>("required_confirmations") as u64,
                watch_addresses: Arc::new(RwLock::new(HashSet::new())),
                tokens: Arc::new(RwLock::new(HashSet::new())),
            };

            // decimals for native token
            decimals_map
                .entry(name.clone())
                .or_insert_with(HashMap::new)
                .insert(config.native_symbol.clone(), config.decimals);

            let blockchain = Blockchain::new(config)?;

            chains_map.insert(name.clone(), Arc::new(blockchain));
            chain_id_to_name.insert(id, name);
        }

        for row in sqlx::query(
            r#"SELECT chain_id, symbol, contract_address, decimals FROM tokens"#
        )
            .fetch_all(&pool)
            .await?
        {
            let chain_id: i32 = row.get("chain_id");

            let chain_name = match chain_id_to_name.get(&chain_id) {
                Some(cname) => cname,
                None => continue, // unreachable because deleting chain causes token demolish
            };

            let blockchain = chains_map.get(chain_name).unwrap(); // scary!

            let symbol: String = row.get("symbol");
            let decimals = row.get::<i16, _>("decimals") as u8;

            let token = TokenConfig {
                symbol: symbol.clone(),
                contract: row.get("contract_address"),
                decimals,
            };

            blockchain.config().read().unwrap()
                .tokens.write().unwrap().insert(token);

            decimals_map
                .entry(chain_name.clone())
                .or_insert_with(HashMap::new)
                .insert(symbol, decimals);
        }

        for row in sqlx::query(
            r#"SELECT address, network FROM invoices WHERE status = 'Pending'"#
        )
            .fetch_all(&pool)
            .await?
        {
            let network: String = row.get("network");
            let address: String = row.get("address");

            if let Some(blockchain) = chains_map.get(&network) {
                blockchain.config().read().unwrap()
                    .watch_addresses.write().unwrap().insert(address);
            }
        }
        
        sqlx::query(
            "UPDATE webhooks SET status = 'Pending' WHERE status = 'Processing'"
        )
            .execute(&pool)
            .await?;

        Ok(Self {
            pool,
            chains_cache: RwLock::new(chains_map),
            token_decimals: RwLock::new(decimals_map)
        })
    }

    fn map_row_to_invoice(
        row: PgRow
    ) -> anyhow::Result<Invoice> {
        let status_str: String = row.get("status");
        let status = match status_str.as_str() {
            "Pending" => InvoiceStatus::Pending,
            "Paid" => InvoiceStatus::Paid,
            "Expired" => InvoiceStatus::Expired,
            _ => anyhow::bail!("Unknown invoice status in DB: {}", status_str),
        };

        let amount_str: String = row.get("amount_raw");
        let paid_str: String = row.get("paid_raw");

        let amount_raw = U256::from_str(&amount_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse amount_raw: {}", e))?;
        let paid_raw = U256::from_str(&paid_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse paid_raw: {}", e))?;

        let network: String = row.get("network");
        let token: String = row.get("token");

        let decimals = row.get::<i16, _>("decimals") as u8;

        let amount_human = format_units(amount_raw, decimals)?;
        let paid_human = format_units(paid_raw, decimals)?;

        Ok(Invoice {
            id: row.get::<uuid::Uuid, _>("id").to_string(),
            address: row.get("address"),
            address_index: row.get::<i32, _>("address_index") as u32,
            network,
            token,
            amount_raw,
            paid_raw,
            amount: amount_human,
            paid: paid_human,
            status,
            decimals,
            webhook_url: row.get("webhook_url"),
            webhook_secret: row.get("webhook_secret"),
            created_at: row.get("created_at"),
            expires_at: row.get("expires_at"),
        })
    }

    fn map_row_to_payment(
        row: PgRow
    ) -> anyhow::Result<Payment> {
        let status_str: String = row.get("status");
        let status = match status_str.as_str() {
            "Confirming" => PaymentStatus::Confirming,
            "Confirmed" => PaymentStatus::Confirmed,
            _ => anyhow::bail!("Unknown payment status in DB: {}", status_str),
        };

        let amount_bd: String = row.get("amount_raw");
        let amount_raw = U256::from_str(&amount_bd)
            .map_err(|e| anyhow::anyhow!("Failed to parse amount_raw: {}", e))?;

        Ok(Payment {
            id: row.get::<uuid::Uuid, _>("id").to_string(),
            invoice_id: row.get::<uuid::Uuid, _>("invoice_id").to_string(),
            from: row.get("from"),
            to: row.get("to"),
            network: row.get("network"),
            tx_hash: row.get("tx_hash"),
            amount_raw,
            block_number: row.get::<i64, _>("block_number") as u64,
            status,
            created_at: row.get("created_at"),
        })
    }
}

impl DatabaseAdapter for Postgres {
    async fn get_chains_map(&self) -> anyhow::Result<HashMap<String, Arc<Blockchain>>> {
        Ok(self.chains_cache.read().unwrap().clone())
    }

    async fn get_chains(&self) -> anyhow::Result<Vec<Arc<Blockchain>>> {
        Ok(self.chains_cache.read().unwrap().values().cloned().collect())
    }

    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<Arc<Blockchain>>> {
        Ok(self.chains_cache.read().unwrap().get(chain_name).cloned())
    }

    async fn get_chain_by_id(&self, id: u32) -> anyhow::Result<Option<Arc<Blockchain>>> {
        let row = sqlx::query("SELECT name FROM chains WHERE id = $1")
            .bind(id as i32)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(r) = row {
            let name: String = r.get("name");
            self.get_chain(&name).await
        } else {
            Ok(None)
        }
    }

    async fn add_chain(&self, chain_config: &ChainConfig) -> anyhow::Result<()> {
        sqlx::query(
            r#"INSERT INTO chains (name, rpc_url, chain_type, xpub, native_symbol, decimals,
                    last_processed_block, block_lag, required_confirmations)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
        )
            .bind(&chain_config.name)
            .bind(&chain_config.rpc_url)
            .bind(chain_config.chain_type.to_string())
            .bind(&chain_config.xpub)
            .bind(&chain_config.native_symbol)
            .bind(chain_config.decimals as i16)
            .bind(chain_config.last_processed_block as i64)
            .bind(chain_config.block_lag as i16)
            .bind(chain_config.required_confirmations as i64)
            .execute(&self.pool)
            .await?;

        let blockchain = Blockchain::new(chain_config.clone())?;

        self.chains_cache.write().unwrap().insert(chain_config.name.clone(), Arc::new(blockchain));

        self._insert_token_decimals(&chain_config.name, &chain_config.native_symbol,
                                    chain_config.decimals)?;

        Ok(())
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
        sqlx::query("UPDATE chains SET last_processed_block = $1 WHERE name = $2")
            .bind(block_num as i64)
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get_latest_block(&self, chain_name: &str) -> anyhow::Result<Option<u64>> {
        Ok(self.chains_cache.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap().last_processed_block))
    }

    async fn get_chains_with_token(&self, token_symbol: &str) -> anyhow::Result<Vec<Arc<Blockchain>>> {
        let guard = self.chains_cache.read().unwrap();

        let result = guard.values()
            .filter(|c| {
                if c.config().read().unwrap()
                    .native_symbol == token_symbol { return true; }
                c.config().read().unwrap()
                    .tokens.read().unwrap().iter()
                    .any(|c| c.symbol == token_symbol)
            })
            .cloned()
            .collect();

        Ok(result)
    }

    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<()> {
        let result = sqlx::query("DELETE FROM chains WHERE name = $1")
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() > 0 {
            self.chains_cache.write().unwrap().remove(chain_name);
            self.token_decimals.write().unwrap().remove(chain_name);
        }

        Ok(())
    }

    async fn remove_chain_by_id(&self, id: u32) -> anyhow::Result<()> {
        let name_opt: Option<String> = sqlx::query_scalar(
            "DELETE FROM chains WHERE id = $1 RETURNING name"
        )
            .bind(id as i32)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(name) = name_opt {
            self.chains_cache.write().unwrap().remove(&name);
            self.token_decimals.write().unwrap().remove(&name);
        }

        Ok(())
    }

    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool> {
        Ok(self.chains_cache.read().unwrap().contains_key(chain_name))
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate)
                                  -> anyhow::Result<()>
    {
        sqlx::query(
            r#"UPDATE chains SET
                       rpc_url = COALESCE($1, rpc_url),
                       last_processed_block = COALESCE($2, last_processed_block),
                       xpub = COALESCE($3, xpub),
                       block_lag = COALESCE($4, block_lag),
                       required_confirmations = COALESCE($5, required_confirmations)
                   WHERE name = $6"#
        )
            .bind(chain_update.rpc_url.to_owned())
            .bind(chain_update.last_processed_block.map(|x| x as i64))
            .bind(chain_update.xpub.to_owned())
            .bind(chain_update.block_lag.map(|x| x as i16))
            .bind(chain_update.required_confirmations.map(|x| x as i16))
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        let guard = self.chains_cache.write().unwrap();
        let blockchain = guard.get(chain_name)
            .ok_or_else(|| anyhow::anyhow!("chain '{}' does not exist", chain_name))?;

        let config_lock = blockchain.config();
        let mut chain_config = config_lock.write().unwrap();

        if let Some(xpub) = &chain_update.xpub {
            chain_config.xpub = xpub.to_owned();
        }

        if let Some(rpc_url) = &chain_update.rpc_url {
            chain_config.rpc_url = rpc_url.to_owned();
        }

        if let Some(last_processed_block) = chain_update.last_processed_block {
            chain_config.last_processed_block = last_processed_block;
        }

        if let Some(block_lag) = chain_update.block_lag {
            chain_config.block_lag = block_lag;
        }

        if let Some(required_confirmations) = chain_update.required_confirmations {
            chain_config.required_confirmations = required_confirmations;
        }

        Ok(())
    }

    async fn get_watch_addresses(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        Ok(self.chains_cache.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap()
                .watch_addresses.read().unwrap().iter()
                .cloned()
                .collect()))
    }

    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<()> {
        match self.chains_cache.read().unwrap().get(chain_name) {
            Some(c) => {
                c.config().read().unwrap()
                    .watch_addresses.write().unwrap().remove(address);
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
        match self.chains_cache.read().unwrap().get(chain_name) {
            Some(c) => {
                let config_lock = c.config();
                let guard = config_lock.read().unwrap();
                let mut watch_addresses = guard.watch_addresses.write().unwrap();

                for addr in addresses {
                    watch_addresses.remove::<String>(addr);
                }
            }
            None => anyhow::bail!("chain '{}' does not exist", chain_name)
        }

        Ok(())
    }

    async fn add_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<()> {
        match self.chains_cache.read().unwrap().get(chain_name) {
            Some(c) => {
                c.config().read().unwrap()
                    .watch_addresses.write().unwrap().insert(address.to_owned());
            }
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        }

        Ok(())
    }

    async fn get_xpub(&self, chain_name: &str) -> anyhow::Result<Option<String>> {
        Ok(self.chains_cache.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap().xpub.clone()))
    }

    async fn get_rpc_url(&self, chain_name: &str) -> anyhow::Result<Option<String>> {
        Ok(self.chains_cache.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap()
                .rpc_url.clone()))
    }

    async fn get_block_lag(&self, chain_name: &str) -> anyhow::Result<Option<u8>> {
        Ok(self.chains_cache.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap()
                .block_lag))
    }

    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Option<Vec<TokenConfig>>> {
        Ok(self.chains_cache.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap()
                .tokens.read().unwrap().iter()
                .cloned()
                .collect()))
    }

    async fn get_token_contracts(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        Ok(self.chains_cache.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap()
                .tokens.read().unwrap().iter()
                .map(|tc| tc.contract.clone())
                .collect()))
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str)
        -> anyhow::Result<Option<TokenConfig>>
    {
        match self.chains_cache.read().unwrap().get(chain_name) {
            Some(c) => Ok(c.config().read().unwrap()
                .tokens.read().unwrap().iter()
                .find(|tc| tc.symbol == token_symbol)
                .cloned()),
            None => Ok(None),
        }
    }

    async fn get_token_by_id(&self, chain_name: &str, id: u32)
        -> anyhow::Result<Option<TokenConfig>>
    {
        let row = sqlx::query(
            r#"SELECT symbol, contract_address, tokens.decimals FROM tokens
                   JOIN chains ON tokens.chain_id = chains.id
                   WHERE chains.name = $1 AND tokens.id = $2"#
        )
            .bind(chain_name)
            .bind(id as i32)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(r) = row {
            Ok(Some(TokenConfig {
                symbol: r.get("symbol"),
                contract: r.get("contract_address"),
                decimals: r.get::<i16, _>("decimals") as u8
            }))
        } else { Ok(None) }
    }

    async fn get_token_by_contract(&self, chain_name: &str, contract_address: &str)
        -> anyhow::Result<Option<TokenConfig>>
    {
        match self.chains_cache.read().unwrap().get(chain_name) {
            Some(c) => Ok(c.config().read().unwrap()
                .tokens.read().unwrap().iter()
                .find(|tc| tc.contract == contract_address)
                .cloned()),
            None => Ok(None),
        }
    }

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"DELETE FROM tokens
                   WHERE symbol = $1 AND chain_id = (SELECT id FROM chains WHERE name = $2)"#
        )
            .bind(token_symbol)
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        if let Some(c) = self.chains_cache.read().unwrap().get(chain_name) {
            c.config().read().unwrap()
                .tokens.write().unwrap().retain(|t| t.symbol != token_symbol);
        }

        if let Some(chain_decimals) = self.token_decimals.write().unwrap()
            .get_mut(chain_name)
        {
            chain_decimals.remove(token_symbol);
        }

        Ok(())
    }

    async fn remove_token_by_id(&self, chain_name: &str, id: u32) -> anyhow::Result<()> {
        let symbol_opt: Option<String> = sqlx::query_scalar(
            "DELETE FROM tokens WHERE id = &1 RETURNING symbol"
        )
            .bind(id as i32)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(symbol) = symbol_opt {
            if let Some(c) = self.chains_cache.read().unwrap().get(chain_name) {
                c.config().read().unwrap()
                    .tokens.write().unwrap().retain(|t| t.symbol != symbol);
            }

            if let Some(chain_decimals) = self.token_decimals.write().unwrap()
                .get_mut(chain_name)
            {
                chain_decimals.remove(&symbol);
            }
        }

        Ok(())
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenConfig) -> anyhow::Result<()> {
        let chain_id: i32 = sqlx::query_scalar("SELECT id FROM chains WHERE name = $1")
            .bind(chain_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|_| anyhow::anyhow!("Chain {} not found in DB", chain_name))?;

        sqlx::query(
            r#"INSERT INTO tokens (chain_id, symbol, contract_address, decimals)
                   VALUES ($1, $2, $3, $4)"#
        )
            .bind(chain_id)
            .bind(&token_config.symbol)
            .bind(&token_config.contract)
            .bind(token_config.decimals as i16)
            .execute(&self.pool)
            .await?;

        if let Some(c) = self.chains_cache.read().unwrap().get(chain_name) {
            c.config().read().unwrap()
                .tokens.write().unwrap().insert(token_config.clone());
        }
        self._insert_token_decimals(chain_name, &token_config.symbol, token_config.decimals)?;

        Ok(())
    }

    async fn get_invoices(&self) -> anyhow::Result<Vec<Invoice>> {
        let rows = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, webhook_url, webhook_secret, created_at, expires_at
                   FROM invoices"#
        )
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(Self::map_row_to_invoice).collect()
    }

    async fn get_invoices_by_chain(&self, chain_name: &str) -> anyhow::Result<Vec<Invoice>> {
        let rows = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, webhook_url, webhook_secret, created_at, expires_at
                   FROM invoices WHERE network = $1"#
        )
            .bind(chain_name)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(Self::map_row_to_invoice).collect()
    }

    async fn get_invoices_by_token(&self, token_symbol: &str) -> anyhow::Result<Vec<Invoice>> {
        let rows = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, webhook_url, webhook_secret, created_at, expires_at
                   FROM invoices WHERE token = $1"#
        )
            .bind(token_symbol)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(Self::map_row_to_invoice).collect()
    }

    async fn get_invoices_by_address(&self, address: &str) -> anyhow::Result<Vec<Invoice>> {
        let rows = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, webhook_url, webhook_secret, created_at, expires_at
                   FROM invoices WHERE address = $1"#
        )
            .bind(address)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(Self::map_row_to_invoice).collect()
    }

    async fn get_invoice(&self, uuid: &str) -> anyhow::Result<Option<Invoice>> {
        let uuid_parsed = uuid::Uuid::parse_str(uuid)?;

        let row = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, webhook_url, webhook_secret, created_at, expires_at
                   FROM invoices WHERE id = $1"#
        )
            .bind(uuid_parsed)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(r) => Ok(Some(Self::map_row_to_invoice(r)?)),
            None => Ok(None)
        }
    }

    async fn get_invoices_by_status(&self, status: InvoiceStatus) -> anyhow::Result<Vec<Invoice>> {
        let rows = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, webhook_url, webhook_secret, created_at, expires_at
                   FROM invoices WHERE status = $1"#
        )
            .bind(status.to_string())
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(Self::map_row_to_invoice).collect()
    }

    async fn get_invoices_by_chain_and_status(&self, chain_name: &str, status: InvoiceStatus)
        -> anyhow::Result<Vec<Invoice>>
    {
        let rows = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, webhook_url, webhook_secret, created_at, expires_at
                   FROM invoices WHERE network = $1 AND status = $2"#
        )
            .bind(chain_name)
            .bind(status.to_string())
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(Self::map_row_to_invoice).collect()
    }

    async fn get_invoices_by_address_and_status(&self, address: &str, status: InvoiceStatus)
        -> anyhow::Result<Vec<Invoice>>
    {
        let rows = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, webhook_url, webhook_secret, created_at, expires_at
                   FROM invoices WHERE address = $1 AND status = $1"#
        )
            .bind(address)
            .bind(status.to_string())
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(Self::map_row_to_invoice).collect()
    }

    async fn get_busy_indexes(&self, chain_name: &str) -> anyhow::Result<Vec<u32>> {
        let rows = sqlx::query(
            "SELECT address_index FROM invoices WHERE network = $1 AND status = 'Pending'"
        )
            .bind(chain_name)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.iter()
            .map(|r| r.get::<i32, _>("address_index") as u32)
            .collect())
    }

    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()> {
        let uuid = uuid::Uuid::parse_str(&invoice.id)?;
        let amount_bd = BigDecimal::from_str(&invoice.amount_raw.to_string())?;
        let paid_bd = BigDecimal::from_str(&invoice.paid_raw.to_string())?;

        sqlx::query(
            r#"INSERT INTO invoices
                   (id, address, address_index, network, token, amount_raw, paid_raw, status,
                    created_at, expires_at, decimals, webhook_url, webhook_secret)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"#
        )
            .bind(uuid)
            .bind(&invoice.address)
            .bind(invoice.address_index as i32)
            .bind(&invoice.network)
            .bind(&invoice.token)
            .bind(&amount_bd)
            .bind(&paid_bd)
            .bind(invoice.status.to_string())
            .bind(invoice.created_at)
            .bind(invoice.expires_at)
            .bind(invoice.decimals as i16)
            .bind(&invoice.webhook_url)
            .bind(&invoice.webhook_secret)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn set_invoice_status(&self, uuid: &str, status: InvoiceStatus) -> anyhow::Result<()> {
        let uuid_parsed = uuid::Uuid::parse_str(uuid)?;

        let result = sqlx::query("UPDATE invoices SET status = $1 WHERE id = $2")
            .bind(status.to_string())
            .bind(uuid_parsed)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            anyhow::bail!("Invoice {} not found", uuid)
        }

        Ok(())
    }

    // async fn add_payment(&self, uuid: &str, amount_raw: U256) -> anyhow::Result<(U256, String)> {
    //     let uuid_parsed = uuid::Uuid::parse_str(&uuid)?;
    //     let added_amount_bd = BigDecimal::from_str(&amount_raw.to_string())?;
    //
    //     let row = sqlx::query(
    //         r#"UPDATE invoices
    //                SET paid_raw = paid_raw + $1
    //                WHERE id = $2
    //                RETURNING paid_raw::TEXT, decimals"#
    //     )
    //         .bind(added_amount_bd)
    //         .bind(uuid_parsed)
    //         .fetch_optional(&self.pool)
    //         .await?;
    //
    //     let row = row.ok_or_else(|| anyhow::anyhow!("Invoice {} not found", uuid))?;
    //
    //     let new_paid_u256 = {
    //         let np_bd: String = row.get("paid_raw");
    //         U256::from_str(&np_bd)
    //             .context("Failed to parse result paid_raw")?
    //     };
    //     let decimals = row.get::<i16, _>("decimals") as u8;
    //
    //     let paid_human = format_units(new_paid_u256, decimals)?;
    //
    //     Ok((new_paid_u256, paid_human))
    // }

    async fn get_pending_invoice_by_address(&self, chain_name: &str, address: &str)
        -> anyhow::Result<Option<Invoice>>
    {
        let row = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, created_at, expires_at, webhook_url, webhook_secret
                   FROM invoices WHERE network = $1 AND address = $2 AND status = 'Pending'"#
        )
            .bind(chain_name)
            .bind(address)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(r) => Ok(Some(Self::map_row_to_invoice(r)?)),
            None => Ok(None)
        }
    }

    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        let rows = sqlx::query(
            r#"UPDATE invoices
                   SET status = 'Expired'
                   WHERE status = 'Pending' AND expires_at <= now()
                   RETURNING id, network, address"#
        )
            .fetch_all(&self.pool)
            .await?;

        let mut expired = Vec::new();
        for row in rows {
            let id: uuid::Uuid = row.get("id");
            let network: String = row.get("network");
            let address: String = row.get("address");

            expired.push((id.to_string(), network, address));
        }

        Ok(expired)
    }

    async fn is_invoice_expired(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        let uuid_parsed = uuid::Uuid::parse_str(&uuid)?;

        let status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM invoices WHERE id = $1"
        )
            .bind(uuid_parsed)
            .fetch_optional(&self.pool)
            .await?;

        Ok(status.map(|s| s == InvoiceStatus::Expired.to_string()))
    }

    async fn is_invoice_paid(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        let uuid_parsed = uuid::Uuid::parse_str(&uuid)?;

        let status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM invoices WHERE id = $1"
        )
            .bind(uuid_parsed)
            .fetch_optional(&self.pool)
            .await?;

        Ok(status.map(|s| s == InvoiceStatus::Paid.to_string()))
    }

    async fn is_invoice_pending(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        let uuid_parsed = uuid::Uuid::parse_str(&uuid)?;

        let status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM invoices WHERE id = $1"
        )
            .bind(uuid_parsed)
            .fetch_optional(&self.pool)
            .await?;

        Ok(status.map(|s| s == InvoiceStatus::Pending.to_string()))
    }

    async fn remove_invoice(&self, uuid: &str) -> anyhow::Result<()> {
        let uuid_parsed = uuid::Uuid::parse_str(&uuid)?;

        sqlx::query("DELETE FROM invoices WHERE id = $1")
            .bind(uuid_parsed)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn add_payment_attempt(&self, invoice_id: &str, from: &str, to: &str, tx_hash: &str,
                                 amount_raw: U256, block_number: u64, network: &str) -> anyhow::Result<()> {
        let invoice_uuid_parsed = uuid::Uuid::parse_str(invoice_id)?;
        let amount_bd = BigDecimal::from_str(&amount_raw.to_string())?;

        sqlx::query(
            r#"INSERT INTO payments (invoice_id, "from", "to", network, tx_hash, amount_raw,
                      block_number, status)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, 'Confirming')
                   ON CONFLICT (invoice_id, tx_hash)
                   DO UPDATE SET block_number = excluded.block_number"#
        )
            .bind(invoice_uuid_parsed)
            .bind(from)
            .bind(to)
            .bind(network)
            .bind(tx_hash)
            .bind(amount_bd)
            .bind(block_number as i64)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get_confirming_payments(&self) -> anyhow::Result<Vec<Payment>> {
        let rows = sqlx::query(
            r#"SELECT id, invoice_id, "from", "to", network, tx_hash,
                       amount_raw::TEXT, block_number, status, created_at
                   FROM payments WHERE status = 'Confirming'"#)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(Self::map_row_to_payment).collect()
    }

    async fn finalize_payment(&self, payment_id: &str) -> anyhow::Result<bool> {
        let pay_uuid_parsed = uuid::Uuid::parse_str(&payment_id)?;

        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            "UPDATE payments SET status = 'Confirmed' WHERE id = $1
                                         RETURNING invoice_id, amount_raw::TEXT"
        )
            .bind(pay_uuid_parsed)
            .fetch_one(&mut *tx)
            .await?;

        let inv_id: uuid::Uuid = row.get("invoice_id");

        let pay_amount_str: String = row.get("amount_raw");
        let pay_amount_bd = BigDecimal::from_str(&pay_amount_str)?;

        let inv = sqlx::query(
            r#"UPDATE invoices SET paid_raw = paid_raw + $1 WHERE id = $2
                   RETURNING paid_raw::TEXT, amount_raw::TEXT"#
        )
            .bind(pay_amount_bd)
            .bind(inv_id)
            .fetch_one(&mut *tx)
            .await?;

        let inv_paid_str: String = inv.get("paid_raw");
        let inv_amount_str: String = inv.get("amount_raw");

        let inv_paid_raw = U256::from_str(&inv_paid_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse paid_raw: {}", e))?;
        let inv_amount_raw = U256::from_str(&inv_amount_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse amount_raw: {}", e))?;

        let is_fully_paid = inv_paid_raw >= inv_amount_raw;
        if is_fully_paid {
            sqlx::query("UPDATE invoices SET status = 'Paid' WHERE id = $1")
                .bind(inv_id)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        Ok(is_fully_paid)
    }

    async fn update_payment_block(&self, payment_id: &str, block_num: u64) -> anyhow::Result<()> {
        let uuid_parsed = uuid::Uuid::parse_str(payment_id)?;

        sqlx::query("UPDATE payments SET block_number = $1 WHERE id = $2")
            .bind(block_num as i64)
            .bind(uuid_parsed)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn select_webhooks_job(&self) -> anyhow::Result<Vec<WebhookJob>> {
        let jobs: Vec<WebhookJob> = sqlx::query_as(
            r#"UPDATE webhooks w
                       SET status = 'Processing'
                       FROM invoices i
                       WHERE w.invoice_id = i.id
                           AND w.id IN (
                               SELECT id FROM webhooks
                               WHERE status = 'Pending' AND next_retry <= NOW()
                               LIMIT 50
                               FOR UPDATE SKIP LOCKED
                           )
                       RETURNING w.id, w.url, w.payload, w.max_retries, w.attempts, i.webhook_secret as "secret_key!"
                           "#
        )
            .fetch_all(&self.pool)
            .await?;

        Ok(jobs)
    }

    async fn set_webhook_status(&self, id: &str, status: WebhookStatus) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE webhooks SET status = $1 WHERE id = $2"
        )
            .bind(status.to_string())
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn schedule_webhook_retry(&self, id: &str, attempts: i32, next_retry_in_secs: f64) -> anyhow::Result<()> {
        sqlx::query(
            r#"UPDATE webhooks SET status = 'Pending', attempts = $1,
                       next_retry = NOW() + (interval '1 second' * $2) WHERE id = $3"#
        )
            .bind(attempts)
            .bind(next_retry_in_secs)
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn add_webhook_job(&self, invoice_id: &str, event: &WebhookEvent) -> anyhow::Result<()> {
        let uuid_parsed = uuid::Uuid::parse_str(&invoice_id)?;

        let url_opt: Option<String> = sqlx::query_scalar(
            "SELECT webhook_url FROM invoices WHERE id = $1"
        )
            .bind(uuid_parsed)
            .fetch_optional(&self.pool)
            .await?;

        let Some(url) = url_opt else {
            anyhow::bail!("Invoice {} not found", invoice_id);
        };

        let event_type = event.as_ref();
        let payload = serde_json::to_value(event)?;

        sqlx::query(
            r#"INSERT INTO webhooks (invoice_id, event_type, url, payload)
                       VALUES ($1, $2, $3, $4)"#
        )
            .bind(uuid_parsed)
            .bind(event_type)
            .bind(url)
            .bind(payload)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        if let Some(d) = self._get_token_decimals_cached(chain_name, token_symbol) {
            return Ok(Some(d));
        }

        if let Some(bc) = self.chains_cache.read().unwrap().get(chain_name) {
            let lock = bc.config();
            let c = lock.read().unwrap();
            if c.native_symbol == token_symbol {
                self._insert_token_decimals(chain_name, token_symbol, c.decimals)?;
                return Ok(Some(c.decimals));
            }

            if let Some(tc) = c.tokens.read().unwrap().iter()
                .find(|tc| tc.symbol == token_symbol)
            {
                self._insert_token_decimals(chain_name, token_symbol, tc.decimals)?;
                return Ok(Some(tc.decimals));
            }
        }

        Ok(None)
    }

}

impl Postgres {
    fn _insert_token_decimals(&self, chain_name: &str, token_symbol: &str, decimals: u8) -> anyhow::Result<()> {
        let mut write_guard = self.token_decimals.write().unwrap();
        let inner_map = write_guard
            .entry(chain_name.to_string())
            .or_insert_with(HashMap::new);

        inner_map.insert(token_symbol.to_string(), decimals);

        Ok(())
    }

    fn _get_token_decimals_cached(&self, chain_name: &str, token_symbol: &str) -> Option<u8> {
        self.token_decimals.read().unwrap()
            .get(chain_name)
            .and_then(|c| c.get(token_symbol).cloned())
    }
}