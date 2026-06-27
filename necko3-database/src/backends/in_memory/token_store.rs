use crate::backends::in_memory::InMemoryAdapter;
use crate::error::{DbError, DbResult};
use crate::traits::TokenStore;
use async_trait::async_trait;
use necko3_types::TokenData;
use std::sync::atomic::Ordering;

#[async_trait]
impl TokenStore for InMemoryAdapter {
    async fn get_tokens(&self, chain_name: &str) -> DbResult<Vec<TokenData>> {
        Ok(self.tokens.read().get(chain_name)
            .map(|c| c.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> DbResult<Option<TokenData>> {
        Ok(self.tokens.read().get(chain_name)
            .and_then(|c|
                c.get(token_symbol).cloned()))
    }

    async fn get_token_by_id(&self, id: i32) -> DbResult<Option<TokenData>> {
        Ok(self.tokens.read()
            .values()
            .flat_map(|c| c.values())
            .find(|t| t.id == id)
            .cloned())
    }

    async fn get_token_by_contract(&self, contract_address: &str) -> DbResult<Option<TokenData>> {
        Ok(self.tokens.read()
            .values()
            .flat_map(|c| c.values())
            .find(|t| t.contract == contract_address)
            .cloned())
    }

    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> DbResult<Vec<TokenData>> {
        Ok(self.tokens.read()
            .values()
            .flat_map(|c| c.values())
            .filter(|t| t.symbol == token_symbol)
            .cloned()
            .collect())
    }

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> DbResult<TokenData> {
        if !self.chains.read().contains_key(chain_name) {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })
        }

        let token_opt = self.tokens.write()
            .get_mut(chain_name)
            .and_then(|c| c.remove(token_symbol));

        let Some(token) = token_opt else {
            return Err(DbError::NotFound {
                entity: "Token",
                id: token_symbol.to_string(),
            })
        };

        Ok(token)
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenData) -> DbResult<TokenData> {
        if !self.chains.read().contains_key(chain_name) {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })
        }

        let next_id = self.tokens_last_id.fetch_add(1, Ordering::SeqCst);

        let mut token_config = token_config.clone();
        token_config.id = next_id;

        self.tokens.write()
            .entry(chain_name.to_string())
            .or_default()
            .insert(token_config.symbol.clone(), token_config.clone());

        Ok(token_config)
    }
}