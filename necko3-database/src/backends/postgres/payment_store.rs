use std::str::FromStr;
use async_trait::async_trait;
use sqlx::{QueryBuilder, Row};
use sqlx::types::BigDecimal;
use uuid::Uuid;
use necko3_types::{Payment, PaymentStatus, UpsertPayment};
use crate::backends::postgres::PostgresAdapter;
use crate::error::{DbError, DbResult};
use crate::model::{PaginatedVec, PaymentFilter};
use crate::traits::PaymentStore;

#[async_trait]
impl PaymentStore for PostgresAdapter {
    async fn get_payments(&self, filter: PaymentFilter) -> DbResult<PaginatedVec<Payment>> {
        fn apply_filters(
            builder: &mut QueryBuilder<sqlx::Postgres>,
            filter: &PaymentFilter
        ) {
            if let Some(ref from) = filter.from {
                builder.push(r#" AND "from" = "#);
                builder.push_bind(from);
            }

            if let Some(ref to) = filter.to {
                builder.push(r#" AND "to" = "#);
                builder.push_bind(to);
            }

            if let Some(ref network) = filter.network {
                builder.push(" AND network = ");
                builder.push_bind(network);
            }

            if let Some(ref token) = filter.token {
                builder.push(" AND token = ");
                builder.push_bind(token);
            }

            if let Some(ref block_number) = filter.block_number {
                builder.push(" AND block_number = ");
                builder.push_bind(*block_number as i64);
            }

            if let Some(ref block_hash) = filter.block_hash {
                builder.push(" AND block_hash = ");
                builder.push_bind(block_hash.to_string());
            }

            if let Some(ref status) = filter.status {
                builder.push(" AND status = ");
                builder.push_bind(status.to_string());
            }
        }

        let mut count_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            "SELECT count(*) FROM payments WHERE TRUE");

        apply_filters(&mut count_builder, &filter);

        let total: i64 = count_builder
            .build_query_as::<(i64,)>()
            .fetch_one(&self.pool)
            .await?
            .0;

        let mut data_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                    id, "from", "to", network, tx_hash, token, amount_raw::TEXT,
                    block_number, block_hash::TEXT, status, created_at, log_index
                FROM payments WHERE TRUE"#
        );

        apply_filters(&mut data_builder, &filter);

        data_builder.push(" ORDER BY created_at DESC LIMIT ");
        data_builder.push_bind(filter.pagination.limit as i64);
        data_builder.push(" OFFSET ");
        data_builder.push_bind(filter.pagination.offset as i64);

        let rows = data_builder
            .build()
            .fetch_all(&self.pool)
            .await?;

        let payments: Vec<Payment> = rows
            .into_iter()
            .map(Self::map_row_to_payment)
            .collect::<DbResult<_>>()?;

        Ok(PaginatedVec::new(
            payments,
            total as u64,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_payment(&self, payment_id: Uuid) -> DbResult<Option<Payment>> {
        sqlx::query(
            r#"SELECT id, "from", "to", network, tx_hash, token, amount_raw::TEXT,
                       block_number, block_hash::TEXT, status, created_at, log_index
                   FROM payments WHERE id = $1"#
        )
            .bind(payment_id)
            .fetch_optional(&self.pool)
            .await?
            .map(Self::map_row_to_payment)
            .transpose()
    }

    async fn get_payment_by_tx_hash(&self, tx_hash: String) -> DbResult<Option<Payment>> {
        sqlx::query(
            r#"SELECT id, "from", "to", network, tx_hash, token, amount_raw::TEXT,
                       block_number, block_hash::TEXT, status, created_at, log_index
                   FROM payments WHERE tx_hash = $1"#
        )
            .bind(tx_hash)
            .fetch_optional(&self.pool)
            .await?
            .map(Self::map_row_to_payment)
            .transpose()
    }

    async fn get_payments_by_status(&self, status: PaymentStatus) -> DbResult<Vec<Payment>> {
        let rows = sqlx::query(
            r#"SELECT id, "from", "to", network, tx_hash, token, amount_raw::TEXT,
                       block_number, block_hash::TEXT, status, created_at, log_index
                   FROM payments WHERE status = $1"#)
            .bind(status.as_ref())
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(Self::map_row_to_payment).collect()
    }

    async fn upsert_payment(&self, payment: &UpsertPayment) -> DbResult<(Uuid, bool)> {
        let amount_bd = BigDecimal::from_str(&payment.amount_raw.to_string())
            .map_err(|e| DbError::DataCorruption(
                format!("Failed to parse amount_raw ({}) as BigDecimal: {}", payment.amount_raw, e)))?;

        let token = payment.asset.as_symbol();
        
        let row = sqlx::query(
            r#"INSERT INTO payments ("from", "to", network, tx_hash, amount_raw,
                      block_number, block_hash, status, log_index, token)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, 'Confirming', $9, $10)
                   ON CONFLICT (tx_hash, log_index, network)
                   DO UPDATE SET block_number = excluded.block_number,
                                 block_hash = excluded.block_hash,
                                 status = 'Confirming'
                   RETURNING id, (xmax = 0) AS inserted"#
        )
            .bind(&payment.from)
            .bind(&payment.to)
            .bind(&payment.network)
            .bind(&payment.tx_hash)
            .bind(amount_bd)
            .bind(payment.block_number as i64)
            .bind(payment.block_hash.to_string())
            .bind(payment.log_index.map(|index| index as i64))
            .bind(token)
            .fetch_one(&self.pool)
            .await?;

        let id: Uuid = row.get("id");
        let inserted: bool = row.get("inserted");

        Ok((id, inserted))
    }

    async fn update_payment_status(&self, payment_id: Uuid, status: PaymentStatus) -> DbResult<()> {
        let res = sqlx::query(
            "UPDATE payments SET status = $1 WHERE id = $2"
        )
            .bind(status.to_string())
            .bind(payment_id)
            .execute(&self.pool)
            .await?;

        if res.rows_affected() == 0 {
            return Err(DbError::NotFound {
                entity: "Payment",
                id: payment_id.to_string(),
            });
        }

        Ok(())
    }

    async fn update_payment_block(&self, payment_id: Uuid, block_num: u64, block_hash: String) -> DbResult<()> {
        let res = sqlx::query(
            "UPDATE payments SET block_number = $1, block_hash = $2 WHERE id = $3"
        )
            .bind(block_num as i64)
            .bind(block_hash)
            .bind(payment_id)
            .execute(&self.pool)
            .await?;

        if res.rows_affected() == 0 {
            return Err(DbError::NotFound {
                entity: "Payment",
                id: payment_id.to_string(),
            });
        }

        Ok(())
    }
}