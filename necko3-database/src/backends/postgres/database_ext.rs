use std::collections::HashSet;
use std::str::FromStr;
use alloy_primitives::U256;
use async_trait::async_trait;
use sqlx::Row;
use sqlx::types::BigDecimal;
use uuid::Uuid;
use necko3_types::InvoiceStatus;
use crate::backends::postgres::PostgresAdapter;
use crate::error::{DbError, DbExtError, DbExtResult, DbResult};
use crate::model::FinalizedPaymentInfo;
use crate::traits::DatabaseExt;

#[async_trait]
impl DatabaseExt for PostgresAdapter {

    async fn get_symbol_decimals(&self, chain_name: &str, symbol: &str) -> DbResult<Option<u8>> {
        // native
        let native: Option<(i32, String, i16)> = sqlx::query_as(
            r#"SELECT id, chains.native_symbol, chains.decimals FROM chains WHERE name = $1"#
        )
            .bind(chain_name)
            .fetch_optional(&self.pool)
            .await?;

        let (chain_id, native_symbol, native_decimals) = match native {
            Some((ci, ns, nd)) => (ci, ns, nd as u8),
            None => { return Ok(None) }
        };

        if native_symbol == symbol {
            return Ok(Some(native_decimals));
        }

        // token
        let token_decimals: Option<i16> = sqlx::query_scalar(
            r#"SELECT tokens.decimals FROM tokens WHERE symbol = $1 AND chain_id = $2"#
        )
            .bind(symbol)
            .bind(chain_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(token_decimals.map(|dec| dec as u8))
    }
    
    async fn finalize_payment(&self, payment_id: Uuid) -> DbExtResult<Option<FinalizedPaymentInfo>> {
        let mut tx = self.pool.begin().await?;

        let row_opt = sqlx::query(
            r#"UPDATE payments SET status = 'Confirmed' WHERE id = $1
                                         RETURNING "to", amount_raw::TEXT, network, token"#
        )
            .bind(payment_id)
            .fetch_optional(&mut *tx)
            .await?;

        let row = row_opt.ok_or_else(|| DbError::NotFound {
            entity: "Payment",
            id: payment_id.to_string(),
        })?;

        let to_address: String = row.get("to");

        let pay_amount_str: String = row.get("amount_raw");
        let pay_amount_bd = BigDecimal::from_str(&pay_amount_str)
            .map_err(|e| DbError::DataCorruption(
                format!("Failed to parse amount_raw ({}) as BigDecimal: {}", pay_amount_str, e)))?;
        let pay_amount_raw = U256::from_str(&pay_amount_str)
            .map_err(|e| DbError::DataCorruption(
                format!("Failed to parse amount_raw from '{}': {}", pay_amount_str, e)))?;

        let pay_network: String = row.get("network");
        let pay_token: String = row.get("token");

        let inv_opt = sqlx::query(
            r#"SELECT paid_raw::TEXT as old_paid_raw,
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
                
                return Err(DbExtError::AssetMismatch {
                    invoice_id: inv_id,
                    expected_token: inv_token,
                    expected_network: inv_network,
                    got_token: pay_token,
                    got_network: pay_network,
                });
            }

            let inv_paid_before_str: String = inv.get("old_paid_raw");
            let inv_amount_str: String = inv.get("amount_raw");
            let inv_paid_before = U256::from_str(&inv_paid_before_str)
                .map_err(|e| DbError::DataCorruption(
                    format!("Failed to parse old_paid_raw from '{}': {}", inv_paid_before_str, e)))?;
            let inv_amount = U256::from_str(&inv_amount_str)
                .map_err(|e| DbError::DataCorruption(
                    format!("Failed to parse amount_raw from '{}': {}", inv_amount_str, e)))?;

            let inv_paid_after = inv_paid_before + pay_amount_raw;

            let old_status_str: String = inv.get("status");
            let old_status: InvoiceStatus = old_status_str.parse()
                .map_err(|e| DbError::DataCorruption(
                    format!("Unknown invoice status '{}' from DB: {}", old_status_str, e)))?;

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

    async fn mark_txs_as_pending(&self, tx_hashes: &[String]) -> DbResult<Vec<String>> {
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