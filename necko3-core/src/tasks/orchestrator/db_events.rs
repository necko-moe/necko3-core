use std::collections::HashMap;
use alloy_primitives::U256;
use tracing::{debug, error, warn};
use uuid::Uuid;
use necko3_database::decorators::notifying::DbEvent;
use necko3_database::model::ExpiredInvoiceInfo;
use necko3_database::traits::DatabaseExt;
use necko3_types::blockchain::{StateCommand, TrackTransaction};
use necko3_types::{Invoice, InvoiceStatus, PartialChainUpdate, PaymentStatus, TokenData, UpsertPayment, WebhookEvent, WebhookStatus};
use crate::tasks::orchestrator::{NeckoOrchestrator, PaymentDetails};
use crate::types::{CoreEvent, ExternalEvent, NeckoEvent, TransactionDetectedData};

impl<D: DatabaseExt> NeckoOrchestrator<D> {
    pub(super) async fn process_db_event(&mut self, event: DbEvent) {
        match event {
            DbEvent::ChainAdded { chain_data } => {
                self.init_chain(chain_data).await;
            }
            DbEvent::ChainRemoved { chain_data } => {
                if let Some(worker) = self.workers.get(&chain_data.name) {
                    worker.abort_handle.abort();
                }

                self.workers.remove(&chain_data.name);
            }
            DbEvent::ChainPartialUpdated { chain_name, partial_update } => {
                self.handle_chain_partial_updated(chain_name, partial_update).await;
            }
            DbEvent::ChainActiveUpdated { chain_name, active } => {
                self.handle_chain_active_updated(chain_name, active).await;
            }
            DbEvent::ChainBlockUpdated { .. } => {} // skip
            DbEvent::ChainWatchAddressAdded { chain_name, address } => {
                self.handle_chain_watch_address_added(chain_name, address).await;
            }
            DbEvent::ChainWatchAddressesRemoved { chain_name, addresses } => {
                self.handle_chain_watch_addresses_removed(chain_name, addresses).await;
            }

            DbEvent::IndexedBlocksUpserted { .. } => {} // skip

            DbEvent::TokenAdded { chain_name, token_data } => {
                self.handle_token_added(chain_name, token_data).await;
            }
            DbEvent::TokenRemoved { chain_name, token_data } => {
                self.handle_token_removed(chain_name, token_data).await;
            }

            DbEvent::InvoiceAdded { invoice } => {
                self.handle_invoice_added(invoice).await;
            }
            DbEvent::InvoiceStatusUpdated { invoice_id, new_status } => {
                self.handle_invoice_status_updated(invoice_id, new_status).await;
            }
            DbEvent::OldInvoicesExpired { invoices_info } => {
                self.handle_old_invoices_expired(invoices_info).await;
            }
            DbEvent::InvoicePaymentApplied { invoice_id, payment_id, paid_raw_before,
                paid_raw_after, old_status, new_status }
            => {
                self.handle_invoice_payment_applied(invoice_id, payment_id, paid_raw_before,
                                                    paid_raw_after, old_status, new_status).await;
            }

            DbEvent::PaymentUpserted { payment_id, payment, is_new_payment } => {
                self.handle_payment_upsert(payment_id, payment, is_new_payment).await;
            }
            DbEvent::PaymentStatusUpdated { payment_id, new_status } => {
                self.handle_payment_status_updated(payment_id, new_status).await;
            }
            DbEvent::PaymentBlockUpdated { .. } => {} // skip

            // let webhook dispatcher handle this
            DbEvent::WebhookAdded { .. } => {}
            DbEvent::PendingWebhooksSelected { .. } => {}
            DbEvent::WebhookStatusUpdated { webhook_id, new_status } => {
                self.handle_webhook_status_updated(webhook_id, new_status).await;
            }
            DbEvent::ScheduledNextWebhookRetry { .. } => {},
        }
    }

    async fn handle_chain_partial_updated(
        &self,
        chain_name: String,
        update: PartialChainUpdate,
    ) {
        let Some(sender) = self.get_worker_state_tx(&chain_name) else {
            warn!(chain_name, "Updated chain is not existing in self.worker_states");
            return;
        };

        if let Some(x) = update.active
            && let Err(e) = sender.send(StateCommand::ChangeActive(x)).await {
            warn!(error = %e, chain_name, active = x,
                        "Failed to send StateCommand::ChangeActive to worker");
        }
        if let Some(x) = update.last_processed_block
            && let Err(e) = sender.send(StateCommand::ChangeLastProcessedBlock(x)).await {
            warn!(error = %e, chain_name, last_processed_block = x,
                        "Failed to send StateCommand::ChangeLastProcessedBlock to worker");
        }
        if let Some(x) = update.block_lag
            && let Err(e) = sender.send(StateCommand::ChangeBlockLag(x)).await {
            warn!(error = %e, chain_name, block_lag = x,
                        "Failed to send StateCommand::ChangeBlockLag to worker");
        }
        if let Some(x) = update.safe_lag
            && let Err(e) = sender.send(StateCommand::ChangeSafeLag(x)).await {
            warn!(error = %e, chain_name, safe_lag = x,
                        "Failed to send StateCommand::ChangeSafeLag to worker");
        }
        if let Some(x) = update.required_confirmations
            && let Err(e) = sender.send(StateCommand::ChangeRequiredConfirmations(x)).await {
            warn!(error = %e, chain_name, required_confirmations = x,
                        "Failed to send StateCommand::ChangeRequiredConfirmations to worker");
        }
        if let Some(x) = update.rpc_urls
            && let Err(e) = sender.send(StateCommand::ChangeRpcUrls(x.clone())).await {
            warn!(error = %e, chain_name, rpc_urls = ?x,
                        "Failed to send StateCommand::ChangeRpcUrls to worker");
        }
    }

    async fn handle_chain_active_updated(
        &self,
        chain_name: String,
        active: bool,
    ) {
        let Some(sender) = self.get_worker_state_tx(&chain_name) else {
            warn!(chain_name, "Updated chain is not existing in self.worker_states");
            return;
        };

        if let Err(e) = sender.send(StateCommand::ChangeActive(active)).await {
            warn!(error = %e, chain_name, active,
                        "Failed to send StateCommand::ChangeActive to worker");
        }
    }

    async fn handle_chain_watch_address_added(
        &self,
        chain_name: String,
        address: String,
    ) {
        let Some(sender) = self.get_worker_state_tx(&chain_name) else {
            warn!(chain_name, "Updated chain is not existing in self.worker_states");
            return;
        };

        if let Err(e) = sender.send(
            StateCommand::AddWatchAddress(address.to_string())).await
        {
            warn!(error = %e, chain_name, address,
                        "Failed to send StateCommand::AddWatchAddress to worker");
        }
    }

    async fn handle_chain_watch_addresses_removed(
        &self,
        chain_name: String,
        addresses: Vec<String>,
    ) {
        let Some(sender) = self.get_worker_state_tx(&chain_name) else {
            warn!(chain_name, "Updated chain is not existing in self.worker_states");
            return;
        };

        for address in addresses {
            if let Err(e) = sender.send(
                StateCommand::RemoveWatchAddress(address.to_string())).await
            {
                warn!(error = %e, chain_name, address,
                            "Failed to send StateCommand::AddWatchAddress to worker");
            }
        }
    }

    async fn handle_token_added(
        &self,
        chain_name: String,
        token_data: TokenData,
    ) {
        let Some(sender) = self.get_worker_state_tx(&chain_name) else {
            warn!(chain_name, "Token added to the chain which is not existing in self.worker_states");
            return;
        };

        let token_symbol = token_data.symbol.clone();

        if let Err(e) = sender.send(
            StateCommand::AddTokenData(token_data)).await
        {
            warn!(error = %e, chain_name, token_symbol,
                        "Failed to send StateCommand::AddTokenData to worker");
        }
    }

    async fn handle_token_removed(
        &self,
        chain_name: String,
        token_data: TokenData,
    ) {
        let Some(sender) = self.get_worker_state_tx(&chain_name) else {
            warn!(chain_name, "Token removed from the chain which is not existing in self.worker_states");
            return;
        };

        let token_contract = token_data.contract;

        if let Err(e) = sender.send(
            StateCommand::RemoveToken { contract_address: token_contract.clone() }).await
        {
            warn!(error = %e, chain_name, token_contract,
                        "Failed to send StateCommand::RemoveToken to worker");
        }
    }

    async fn handle_invoice_added(
        &self,
        invoice: Invoice,
    ) {
        match self.db.add_watch_address(&invoice.network, invoice.address).await {
            Ok(true) => {}
            Ok(false) => {
                warn!("Invoice added, but address to watch is already existing");
            }
            Err(e) => {
                warn!(error = %e, invoice_id = %invoice.id,
                            "Failed to add watch address");
            }
        }
    }

    async fn handle_invoice_status_updated(
        &self,
        invoice_id: Uuid,
        new_status: InvoiceStatus,
    ) {
        let invoice = match self.db.get_invoice(invoice_id).await {
            Ok(Some(inv)) => inv,
            Ok(None) => {
                error!(invoice_id = %invoice_id,
                    "Received DbEvent::InvoicePaymentApplied, \
                    but invoice_id is not existing in the storage");
                return;
            }
            Err(e) => {
                error!(error = %e, invoice_id = %invoice_id,
                    "Failed to get invoice");
                return;
            }
        };

        if new_status != InvoiceStatus::Pending {
            if let Err(e) = self.db.remove_watch_address(&invoice.network, &invoice.address).await {
                warn!(error = %e, chain_name = invoice.network, address = invoice.address,
                    "Failed to remove watch_address");
            }

            self.handle_invoice_status_change(invoice_id, invoice.paid, new_status).await;
        } else {
            match self.db.add_watch_address(&invoice.network, invoice.address).await {
                Ok(true) => {}
                Ok(false) => {
                    warn!("Invoice status updated to Pending, but address to watch is already existing");
                }
                Err(e) => {
                    warn!(error = %e, invoice_id = %invoice.id,
                        "Failed to add watch address");
                }
            }
        }
    }

    async fn handle_old_invoices_expired(
        &self,
        invoices_info: Vec<ExpiredInvoiceInfo>,
    ) {
        for invoice in &invoices_info {
            let webhook_job = WebhookEvent::InvoiceExpired {
                invoice_id: invoice.id,
            };

            if let Err(e) = self.db.create_webhook_job(invoice.id, &webhook_job).await {
                warn!(error = %e, invoice_id = %invoice.id,
                            "Failed to create InvoiceExpired webhook job");
            }

            if let Err(e) = self.core_event_tx.send(
                NeckoEvent::Ext(ExternalEvent::InvoiceExpired {
                invoice_id: invoice.id,
            })).await {
                warn!(error = %e, "Failed to send ExternalEvent::InvoiceExpired event");
            };
        }

        let mut to_remove: HashMap<String, Vec<String>> = HashMap::new();

        for invoice in invoices_info {
            to_remove.entry(invoice.network)
                .or_default()
                .push(invoice.address);
        }

        for (network, addresses) in to_remove {
            match self.db.remove_watch_addresses(&network, &addresses).await {
                Ok(removed) => {
                    debug!(network = %network,
                                "Removed {}/{} addresses from address watching",
                            removed.len(), addresses.len());
                }
                Err(e) => {
                    warn!(error = %e, "Failed to remove watch addresses");
                }
            }
        }
    }

    async fn handle_invoice_payment_applied(
        &self,
        invoice_id: Uuid,
        payment_id: Uuid,
        paid_raw_before: U256,
        paid_raw_after: U256,
        old_status: InvoiceStatus,
        new_status: InvoiceStatus,
    ) {
        if let Err(e) = self.core_event_tx.send(
            NeckoEvent::Ext(ExternalEvent::InvoicePaymentApplied {
                invoice_id,
                payment_id,
                paid_raw_before,
                paid_raw_after,
                old_status,
                new_status,
            })).await {
            warn!(error = %e, "Failed to send ExternalEvent::InvoicePaymentApplied event");
        }

        if new_status != InvoiceStatus::Pending {
            let invoice = match self.db.get_invoice(invoice_id).await {
                Ok(Some(inv)) => inv,
                Ok(None) => {
                    error!(invoice_id = %invoice_id,
                        "Received DbEvent::InvoicePaymentApplied, \
                        but invoice_id is not existing in the storage");
                    return;
                }
                Err(e) => {
                    error!(error = %e, invoice_id = %invoice_id,
                        "Failed to get invoice");
                    return;
                }
            };

            if let Err(e) = self.db.remove_watch_address(&invoice.network, &invoice.address).await {
                warn!(error = %e, chain_name = invoice.network, address = invoice.address,
                    "Failed to remove watch_address");
            }

            self.handle_invoice_status_change(invoice_id, invoice.paid, new_status).await;
        }
    }

    async fn handle_payment_upsert(
        &self,
        payment_id: Uuid,
        payment: UpsertPayment,
        is_new_payment: bool,
    ) {
        if is_new_payment {
            let event_data = Box::new(TransactionDetectedData {
                db_transaction_id: payment_id,
                tx_hash: payment.tx_hash.clone(),
                network: payment.network.clone(),
                asset: payment.asset,
                from: payment.from,
                to: payment.to.clone(),
                amount_raw: payment.amount_raw,
                amount_human: payment.amount_human,
                block_number: payment.block_number,
                block_hash: payment.block_hash.clone(),
                log_index: payment.log_index,
            });

            if let Err(e) = self.core_event_tx.send(
                NeckoEvent::Core(CoreEvent::TransactionDetected(event_data))).await {
                warn!(error = %e, "Failed to send CoreEvent::NewTransaction event");
            }
        }

        if payment.required_confirmations > 0 {
            let transaction_tx = match self.workers.get(&payment.network) {
                Some(worker) => worker.value().transaction_tx.clone(),
                None => {
                    error!("self.workers out of sync: unknown key '{}'", payment.network);
                    return
                }
            };

            if let Err(e) = transaction_tx.send(TrackTransaction {
                tx_hash: payment.tx_hash.clone(),
                block_number: payment.block_number,
                block_hash: payment.block_hash,
                confirm_after: payment.required_confirmations,
            }).await {
                warn!(error = %e, tx_hash = payment.tx_hash, "Failed to send TrackTransaction request");
            };
        } else {
            if let Err(e) = self.db.update_payment_status(
                payment_id, PaymentStatus::Confirmed).await
            {
                error!(error = %e, payment_id = %payment_id, tx_hash = payment.tx_hash,
                            "Failed to update payment status");

                return;
            };

            self.finalize_payment(PaymentDetails {
                payment_id,
                address_to: payment.to,
                tx_hash: payment.tx_hash,
                block_number: payment.block_number,
                block_hash: payment.block_hash,
                confirmed_after: 0,
            }).await;
        }
    }

    async fn handle_payment_status_updated(
        &self,
        payment_id: Uuid,
        new_status: PaymentStatus,
    ) {
        let PaymentStatus::Cancelled = new_status else { return; };

        if let Err(e) = self.core_event_tx.send(
            NeckoEvent::Ext(ExternalEvent::PaymentCancelled {
                payment_id,
            })).await {
            warn!(error = %e, "Failed to send ExternalEvent::PaymentCancelled event");
        }
    }

    async fn handle_webhook_status_updated(
        &self,
        webhook_id: Uuid,
        new_status: WebhookStatus,
    ) {
        if new_status == WebhookStatus::Pending
            || new_status == WebhookStatus::Processing { return; };

        let webhook_data = match self.db.get_webhook(webhook_id).await {
            Ok(Some(webhook_data)) => webhook_data,
            Ok(None) => {
                error!(webhook_id = %webhook_id,
                            "Received DbEvent::WebhookStatusUpdated, \
                            but webhook_id is not existing in the storage");

                return;
            }
            Err(e) => {
                error!(error = %e, webhook_id = %webhook_id,
                            "Failed to get webhook");

                return;
            }
        };

        let event = match new_status {
            WebhookStatus::Sent => {
                ExternalEvent::WebhookDelivered {
                    webhook_id,
                    invoice_id: webhook_data.invoice_id,
                    attempt: webhook_data.attempts,
                    url: webhook_data.url,
                }
            }
            WebhookStatus::Failed => {
                ExternalEvent::WebhookFailed {
                    webhook_id,
                    invoice_id: webhook_data.invoice_id,
                    attempt: webhook_data.attempts,
                    max_attempts: webhook_data.max_retries,
                    url: webhook_data.url,
                }
            }
            WebhookStatus::Cancelled => {
                ExternalEvent::WebhookCancelled {
                    webhook_id
                }
            }
            _ => { return; } // :D
        };

        if let Err(e) = self.core_event_tx.send(
            NeckoEvent::Ext(event)).await {
            warn!(error = %e, "Failed to send ExternalEvent::Webhook event");
        }
    }
}