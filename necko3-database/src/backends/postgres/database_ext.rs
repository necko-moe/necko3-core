use std::collections::HashSet;
use std::str::FromStr;
use alloy_primitives::U256;
use async_trait::async_trait;
use sqlx::Row;
use sqlx::types::BigDecimal;
use uuid::Uuid;
use necko3_types::InvoiceStatus;
use crate::backends::postgres::PostgresAdapter;
use crate::model::FinalizedPaymentInfo;
use crate::traits::DatabaseExt;

#[async_trait]
impl DatabaseExt for PostgresAdapter {
    async fn finalize_payment(&self, payment_id: Uuid) -> anyhow::Result<Option<FinalizedPaymentInfo>> {
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"UPDATE payments SET status = 'Confirmed' WHERE id = $1
                                         RETURNING "to", amount_raw::TEXT, network, token"#
        )
            .bind(payment_id)
            .fetch_one(&mut *tx)
            .await?;

        let to_address: String = row.get("to");

        let pay_amount_str: String = row.get("amount_raw");
        let pay_amount_bd = BigDecimal::from_str(&pay_amount_str)?;
        let pay_amount_u256 = U256::from_str(&pay_amount_str)?;

        let pay_network: String = row.get("network");
        let pay_token: String = row.get("token");

        let inv_opt = sqlx::query(
            r#"SELECT paid_raw::TEXT as old_paid_raw,
                       paid_raw::TEXT as new_paid_raw,
                       amount_raw::TEXT,
                       status, id, network, token
                   FROM invoices WHERE address = $1 FOR UPDATE"#
        )
            .bind(to_address)
            .fetch_optional(&mut *tx)
            .await?;

        if let Some(inv) = inv_opt {
            let inv_id: Uuid = inv.get("id");
            let inv_network: String = inv.get("network");
            let inv_token: String = inv.get("token");

            if inv_network != pay_network || inv_token != pay_token {
                tx.commit().await?;

                anyhow::bail!(
                    "Asset mismatch for invoice {}: expected {} ({}), got {} ({})",
                    inv_id, inv_token, inv_network, pay_token, pay_network
                );
            }

            let inv_paid_before = U256::from_str(&inv.get::<String, _>("old_paid_raw"))
                .map_err(|e| anyhow::anyhow!("Failed to parse old_paid_raw: {}", e))?;
            let inv_amount = U256::from_str(&inv.get::<String, _>("amount_raw"))
                .map_err(|e| anyhow::anyhow!("Failed to parse amount_raw: {}", e))?;

            let inv_paid_after = inv_paid_before + pay_amount_u256;

            let old_status_str: String = inv.get("status");
            let old_status: InvoiceStatus = old_status_str.parse()
                .map_err(|e| anyhow::anyhow!("Unknown invoice status '{}' from DB: {}", old_status_str, e))?;

            let is_fully_paid = inv_paid_after >= inv_amount;
            let new_status = if is_fully_paid {
                InvoiceStatus::Paid
            } else { old_status };

            sqlx::query(
                r#"UPDATE invoices
                       SET paid_raw = paid_raw + $1, status = $2
                       WHERE id = $3"#
            )
                .bind(&pay_amount_bd)
                .bind(new_status.as_ref())
                .bind(inv_id)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            Ok(Some(FinalizedPaymentInfo {
                is_fully_paid,
                invoice_id: inv_id,
                paid_raw_before: inv_paid_before,
                paid_raw_after: inv_paid_after,
                old_invoice_status: old_status,
                new_invoice_status: new_status,
            }))
        } else {
            tx.commit().await?;

            Ok(None)
        }
    }

    async fn mark_txs_as_pending(&self, tx_hashes: &[String]) -> anyhow::Result<Vec<String>> {
        if tx_hashes.is_empty() {
            return Ok(Vec::new());
        }

        let updated: Vec<String> = sqlx::query_scalar(
            r#"UPDATE payments SET status = 'Pending'
                   WHERE tx_hash = ANY($1)
                   RETURNING tx_hash"#
        )
            .bind(tx_hashes)
            .fetch_all(&self.pool)
            .await?;

        let updated_set = updated.iter()
            .map(|s| s.as_str())
            .collect::<HashSet<_>>();

        let skipped = tx_hashes
            .iter()
            .filter(|tx_hash| !updated_set.contains(tx_hash.as_str()))
            .cloned()
            .collect();

        Ok(skipped)
    }
}