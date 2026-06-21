use std::sync::atomic::{AtomicU64, Ordering};
use async_trait::async_trait;
use crate::backends::in_memory::InMemoryAdapter;
use crate::error::DbResult;
use crate::traits::XPubStore;

#[async_trait]
impl XPubStore for InMemoryAdapter {
    async fn next_derivation_index(&self, xpub: &str) -> DbResult<u64> {
        if let Some(last_used_index) = self.xpub_states.get(xpub) {
            return Ok(last_used_index.value()
                .fetch_add(1, Ordering::SeqCst))
        }

        let entry = self.xpub_states
            .entry(xpub.to_string())
            .or_insert_with(|| AtomicU64::new(0));

        Ok(entry.value().fetch_add(1, Ordering::SeqCst))
    }
}