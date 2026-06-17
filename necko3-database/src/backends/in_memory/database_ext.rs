use async_trait::async_trait;
use crate::backends::in_memory::InMemoryAdapter;
use crate::traits::DatabaseExt;

#[async_trait]
impl DatabaseExt for InMemoryAdapter {}