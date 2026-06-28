use async_trait::async_trait;
use uuid::Uuid;
use necko3_types::{Payment, PaymentStatus, UpsertPayment};
use crate::backends::in_memory::InMemoryAdapter;
use crate::error::{DbError, DbResult};
use crate::model::{PaginatedVec, PaymentFilter};
use crate::traits::PaymentStore;

#[async_trait]
impl PaymentStore for InMemoryAdapter {
    async fn get_payments(&self, filter: PaymentFilter) -> DbResult<PaginatedVec<Payment>> {
        let mut filtered: Vec<Payment> = self.payments.iter()
            .filter(|kv| {
                let pay = kv.value();

                filter.from.as_ref().map_or(true, |f| pay.from == *f)
                    && filter.to.as_ref().map_or(true, |t| pay.to == *t)
                    && filter.network.as_ref().map_or(true, |n| pay.network == *n)
                    && filter.token.as_ref().map_or(true, |t| pay.token == *t)
                    && filter.block_number.as_ref().map_or(true, |b| pay.block_number == *b)
                    && filter.block_hash.as_ref().map_or(true, |b| pay.block_hash == *b)
                    && filter.status.as_ref().map_or(true, |s| pay.status == *s)
            })
            .map(|x| x.value().clone())
            .collect();

        let total = filtered.len() as u64;

        filtered.sort_unstable_by(|a, b| b.created_at.cmp(&a.created_at));

        let payments: Vec<Payment> = filtered
            .into_iter()
            .skip(filter.pagination.offset as usize)
            .take(filter.pagination.limit as usize)
            .collect();

        Ok(PaginatedVec::new(
            payments,
            total,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_payment(&self, payment_id: Uuid) -> DbResult<Option<Payment>> {
        Ok(self.payments.get(&payment_id)
            .map(|x| x.value().clone()))
    }

    async fn get_payment_by_tx_hash(&self, tx_hash: &str) -> DbResult<Option<Payment>> {
        Ok(self.payments.iter()
            .find(|payment|
                payment.value().tx_hash == tx_hash)
            .map(|x| x.value().clone()))
    }

    async fn get_payments_by_status(&self, status: PaymentStatus) -> DbResult<Vec<Payment>> {
        Ok(self.payments.iter()
            .filter(|payment|
                payment.value().status == status)
            .map(|x| x.value().clone())
            .collect())
    }

    async fn upsert_payment(&self, upsert: UpsertPayment) -> DbResult<(Uuid, bool)> {
        if let Some(mut existing) = self.payments.iter_mut()
            .find(|x| {
                let pay = x.value();

                pay.tx_hash == upsert.tx_hash
                    && pay.log_index == upsert.log_index
                    && pay.network == upsert.network
            })
        {
            existing.block_number = upsert.block_number;
            existing.block_hash = upsert.block_hash.clone();
            existing.status = PaymentStatus::Confirming;

            return Ok((existing.id, false));
        }

        let new_payment: Payment = upsert.clone().into();
        let payment_id = new_payment.id;
        self.payments.insert(new_payment.id, new_payment);

        Ok((payment_id, true))
    }

    async fn update_payment_status(&self, payment_id: Uuid, status: PaymentStatus) -> DbResult<()> {
        if !self.payments.contains_key(&payment_id) {
            return Err(DbError::NotFound {
                entity: "Payment",
                id: payment_id.to_string(),
            })
        }
        
        if let Some(mut payment) = self.payments
            .get_mut(&payment_id) {
            
            payment.status = status;
        }

        Ok(())
    }

    async fn update_payment_block(&self, payment_id: Uuid, block_num: u64, block_hash: &str) -> DbResult<()> {
        if !self.payments.contains_key(&payment_id) {
            return Err(DbError::NotFound {
                entity: "Payment",
                id: payment_id.to_string(),
            })
        }
        
        if let Some(mut payment) = self.payments
            .get_mut(&payment_id) {
            
            payment.block_number = block_num;
            payment.block_hash = block_hash.to_string();
        }

        Ok(())
    }
}