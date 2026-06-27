use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, error, info, instrument, trace};
use necko3_database::traits::InvoiceStore;

pub struct NeckoJanitor<D> {
    db: Arc<D>,

    interval: Interval,
}

impl<D: InvoiceStore> NeckoJanitor<D> {
    pub fn new(db: Arc<D>, interval_duration: Duration) -> Self {
        let mut interval = tokio::time::interval(interval_duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Self { db, interval }
    }

    #[instrument(skip(self), name = "janitor_service")]
    pub async fn run(mut self) {
        info!(interval = ?self.interval, "Starting janitor service");
        self.interval.tick().await; // skip first tick

        loop {
            self.interval.tick().await;

            debug!("Checking for expired invoices...");

            let expired_invoices = match self.db.expire_old_invoices().await {
                Ok(addrs) => addrs,
                Err(e) => {
                    error!(error = %e, "Failed to fetch/expire old invoices");
                    continue;
                }
            };

            if expired_invoices.is_empty() {
                trace!("No expired invoices found");
                continue;
            }

            info!(count = expired_invoices.len(), "Expired invoices.");
        }
    }
}