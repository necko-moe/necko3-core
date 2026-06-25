pub mod webhook_config;
pub mod chain_config;
pub mod token_config;
pub mod invoice_config;

use std::sync::Arc;
use std::time::Duration;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use necko3_database::backends::in_memory::InMemoryAdapter;
use necko3_database::decorators::notifying::NotifyingDb;
use necko3_database::error::DbResult;
use necko3_database::traits::{ChainStore, DatabaseExt, TokenStore};
use crate::builder::chain_config::ChainConfig;
use crate::builder::webhook_config::WebhookDispatcherConfig;
use crate::core::NeckoCore;
use crate::tasks::janitor::NeckoJanitor;
use crate::tasks::orchestrator::NeckoOrchestrator;
use crate::tasks::webhook_dispatcher::NeckoWebhookDispatcher;
use crate::types::{CoreEvent, NeckoEvent};

pub struct NeckoCoreBuilder<D, E = CoreEvent> {
    db: D,

    _event_type: std::marker::PhantomData<E>,

    channel_buffer: usize,
    janitor_interval: Duration,
    webhook_config: WebhookDispatcherConfig,

    webhook_dispatcher_enabled: bool,
    janitor_service_enabled: bool,
}

impl Default for NeckoCoreBuilder<InMemoryAdapter, CoreEvent> {
    fn default() -> Self {
        Self {
            db: InMemoryAdapter::default(),

            _event_type: std::marker::PhantomData,

            channel_buffer: 1000,
            janitor_interval: Duration::from_secs(30),
            webhook_config: WebhookDispatcherConfig::default(),

            webhook_dispatcher_enabled: true,
            janitor_service_enabled: true,
        }
    }
}

impl<E> NeckoCoreBuilder<InMemoryAdapter, E>
where
    E: Clone + Send + Sync + 'static,
    NeckoEvent: TryInto<E>,
{
    pub async fn add_chain(self, chain_config: ChainConfig) -> DbResult<Self> {
        let (tokens, chain_data) = chain_config.into();

        self.db.add_chain(&chain_data).await?;

        for token in tokens {
            self.db.add_token(&chain_data.name, &token).await?;
        }

        Ok(self)
    }
}

impl<D: DatabaseExt, E> NeckoCoreBuilder<D, E>
where
    E: Clone + Send + Sync + 'static,
    NeckoEvent: TryInto<E>,
{
    pub fn with_storage<ND: DatabaseExt>(self, db: ND) -> NeckoCoreBuilder<ND, E> {
        NeckoCoreBuilder {
            db,
            _event_type: self._event_type,
            channel_buffer: self.channel_buffer,
            janitor_interval: self.janitor_interval,
            webhook_config: self.webhook_config,
            webhook_dispatcher_enabled: self.webhook_dispatcher_enabled,
            janitor_service_enabled: self.janitor_service_enabled,
        }
    }

    pub fn with_event_type<NE>(self) -> NeckoCoreBuilder<D, NE> {
        NeckoCoreBuilder {
            db: self.db,
            _event_type: std::marker::PhantomData,
            channel_buffer: self.channel_buffer,
            janitor_interval: self.janitor_interval,
            webhook_config: self.webhook_config,
            webhook_dispatcher_enabled: self.webhook_dispatcher_enabled,
            janitor_service_enabled: self.janitor_service_enabled,
        }
    }

    pub fn with_janitor_interval(mut self, interval: Duration) -> Self {
        self.janitor_interval = interval;
        self
    }

    pub fn with_event_channel_buffer(mut self, buffer: usize) -> Self {
        self.channel_buffer = buffer;
        self
    }

    pub fn with_webhook_config(mut self, webhook_config: WebhookDispatcherConfig) -> Self {
        self.webhook_config = webhook_config;
        self
    }

    pub fn enable_webhook_dispatcher(mut self, enabled: bool) -> Self {
        self.webhook_dispatcher_enabled = enabled;
        self
    }

    pub fn enable_janitor_service(mut self, enabled: bool) -> Self {
        self.janitor_service_enabled = enabled;
        self
    }
}

impl<D: DatabaseExt + 'static, E> NeckoCoreBuilder<D, E>
where
    E: Clone + Send + Sync + 'static,
    NeckoEvent: TryInto<E>,
{
    pub async fn build(self) -> NeckoCore<NotifyingDb<D>, E> {
        let (db_event_tx, db_event_rx) = mpsc::channel(5000);
        let db = Arc::new(NotifyingDb::new(self.db, db_event_tx));

        let (core_event_tx, core_event_rx) = mpsc::channel::<NeckoEvent>(self.channel_buffer);
        let (chain_event_tx, chain_event_rx) = mpsc::channel(10000);

        let workers = Arc::new(DashMap::new());

        let core = NeckoCore::new(db.clone(), workers.clone());

        let orchestrator = NeckoOrchestrator::new(
            db.clone(), db_event_rx, chain_event_tx, chain_event_rx, core_event_tx, workers);

        if self.janitor_service_enabled {
            let janitor = NeckoJanitor::new(db.clone(), self.janitor_interval);

            tokio::spawn(async move { janitor.run().await; });
        }

        if self.webhook_dispatcher_enabled {
            let webhook_dispatcher = NeckoWebhookDispatcher::new(
                db, self.webhook_config.interval, self.webhook_config.webhooks_selection_limit,
                self.webhook_config.concurrency_limit);

            tokio::spawn(async move { webhook_dispatcher.run().await; });
        }

        let (ready_tx, ready_rx) = oneshot::channel();

        tokio::spawn(async move { orchestrator.run(ready_tx).await; });

        let core_listener = core.clone();
        tokio::spawn(async move {
            core_listener.listen_channels(core_event_rx).await;
        });

        let _ = ready_rx.await;

        core
    }
}