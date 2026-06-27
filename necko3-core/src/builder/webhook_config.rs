use std::time::Duration;

pub struct WebhookDispatcherConfig {
    pub interval: Duration,
    pub webhooks_selection_limit: usize,
    pub concurrency_limit: usize,
}

impl Default for WebhookDispatcherConfig {
    fn default() -> Self {
        WebhookDispatcherConfig {
            interval: Duration::from_secs(5),
            webhooks_selection_limit: 50,
            concurrency_limit: 100,
        }
    }
}

impl WebhookDispatcherConfig {
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_selection_limit(mut self, limit: usize) -> Self {
        self.webhooks_selection_limit = limit;
        self
    }

    pub fn with_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit = limit;
        self
    }
}