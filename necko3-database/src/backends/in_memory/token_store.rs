use async_trait::async_trait;
use necko3_types::TokenData;
use crate::backends::in_memory::InMemoryAdapter;
use crate::traits::TokenStore;

#[async_trait]
impl TokenStore for InMemoryAdapter {
    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Vec<TokenData>> {
        Ok(self.tokens.read().get(chain_name)
            .map(|c| c.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenData>> {
        Ok(self.tokens.read().get(chain_name)
            .and_then(|c|
                c.get(token_symbol).cloned()))
    }

    async fn get_token_by_id(&self, id: i32) -> anyhow::Result<Option<TokenData>> {
        Ok(self.tokens.read()
            .values()
            .flat_map(|c| c.values())
            .find(|t| t.id == id)
            .cloned())
    }

    async fn get_token_by_contract(&self, contract_address: &str) -> anyhow::Result<Option<TokenData>> {
        Ok(self.tokens.read()
            .values()
            .flat_map(|c| c.values())
            .find(|t| t.contract == contract_address)
            .cloned())
    }

    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> anyhow::Result<Vec<TokenData>> {
        Ok(self.tokens.read()
            .values()
            .flat_map(|c| c.values())
            .filter(|t| t.symbol == token_symbol)
            .cloned()
            .collect())
    }

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenData>> {
        let deleted = self.tokens.write()
            .get_mut(chain_name)
            .and_then(|c| c.remove(token_symbol));

        Ok(deleted)
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenData) -> anyhow::Result<()> {
        if !self.chains.read().contains_key(chain_name) {
            anyhow::bail!("Chain {} not found in DB", chain_name)
        }

        self.tokens.write()
            .entry(chain_name.to_string())
            .or_default()
            .insert(token_config.symbol.clone(), token_config.clone());

        Ok(())
    }

    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        if let Some(chain) = self.chains.read()
            .get(chain_name)
        {
            if chain.native_symbol == token_symbol {
                return Ok(Some(chain.decimals));
            }
        }

        let decimals = self.tokens.read()
            .get(chain_name)
            .and_then(|c| c.get(token_symbol))
            .map(|token| token.decimals);

        Ok(decimals)
    }
}