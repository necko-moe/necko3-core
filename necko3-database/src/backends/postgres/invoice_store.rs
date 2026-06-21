use std::str::FromStr;
use alloy_primitives::U256;
use async_trait::async_trait;
use sqlx::QueryBuilder;
use sqlx::types::BigDecimal;
use uuid::Uuid;
use necko3_types::{Invoice, InvoiceStatus};
use crate::backends::postgres::PostgresAdapter;
use crate::model::{ExpiredInvoiceInfo, InvoiceFilter, PaginatedVec};
use crate::traits::InvoiceStore;

#[async_trait]
impl InvoiceStore for PostgresAdapter {
    async fn get_invoices(&self, filter: InvoiceFilter) -> anyhow::Result<PaginatedVec<Invoice>> {
        fn apply_filters(
            builder: &mut QueryBuilder<sqlx::Postgres>,
            filter: &InvoiceFilter
        ) {
            if let Some(ref status) = filter.status {
                builder.push(" AND status = ");
                builder.push_bind(status.to_string());
            }

            if let Some(ref address) = filter.address {
                builder.push(" AND address = ");
                builder.push_bind(address);
            }

            if let Some(ref network) = filter.network {
                builder.push(" AND network = ");
                builder.push_bind(network);
            }

            if let Some(ref token) = filter.token {
                builder.push(" AND token = ");
                builder.push_bind(token);
            }
        }

        let mut count_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            "SELECT count(*) FROM invoices WHERE TRUE");

        apply_filters(&mut count_builder, &filter);

        let total: i64 = count_builder
            .build_query_as::<(i64,)>()
            .fetch_one(&self.pool)
            .await?
            .0;

        let mut data_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                    id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                    status, decimals, webhook_url, webhook_secret, webhook_max_retries, created_at, expires_at
                FROM invoices WHERE TRUE"# // WHERE TRUE so that arguments don't have to think that they can be first
        );

        apply_filters(&mut data_builder, &filter);

        data_builder.push(" ORDER BY created_at DESC ");
        data_builder.push(" LIMIT ");
        data_builder.push_bind(filter.pagination.limit as i64);
        data_builder.push(" OFFSET ");
        data_builder.push_bind(filter.pagination.offset as i64);

        let rows = data_builder
            .build()
            .fetch_all(&self.pool)
            .await?;

        let invoices: Vec<Invoice> = rows
            .into_iter()
            .map(Self::map_row_to_invoice)
            .collect::<anyhow::Result<_>>()?;

        Ok(PaginatedVec::new(
            invoices,
            total as u64,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_invoice(&self, invoice_id: Uuid) -> anyhow::Result<Option<Invoice>> {
        sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, webhook_url, webhook_secret, webhook_max_retries, created_at, expires_at
                   FROM invoices WHERE id = $1"#
        )
            .bind(invoice_id)
            .fetch_optional(&self.pool)
            .await?
            .map(Self::map_row_to_invoice)
            .transpose()
    }

    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()> {
        let amount_bd = BigDecimal::from_str(&invoice.amount_raw.to_string())?;
        let paid_bd = BigDecimal::from_str(&invoice.paid_raw.to_string())?;

        sqlx::query(
            r#"INSERT INTO invoices
                   (id, address, address_index, network, token, amount_raw, paid_raw, status,
                    created_at, expires_at, decimals, webhook_url, webhook_secret, webhook_max_retries)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"#
        )
            .bind(invoice.id)
            .bind(&invoice.address)
            .bind(invoice.address_index as i32)
            .bind(&invoice.network)
            .bind(&invoice.token)
            .bind(&amount_bd)
            .bind(&paid_bd)
            .bind(invoice.status.to_string())
            .bind(invoice.created_at)
            .bind(invoice.expires_at)
            .bind(invoice.decimals as i16)
            .bind(&invoice.webhook_url)
            .bind(&invoice.webhook_secret)
            .bind(invoice.webhook_max_retries.map(|x| x as i32))
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn update_invoice_status(&self, invoice_id: Uuid, status: InvoiceStatus) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE invoices SET status = $1 WHERE id = $2"
        )
            .bind(status.to_string())
            .bind(invoice_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get_invoice_by_address(&self, address: &str) -> anyhow::Result<Option<Invoice>> {
        let row = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, created_at, expires_at, webhook_url, webhook_secret, webhook_max_retries
                   FROM invoices WHERE address = $1"#
        )
            .bind(address)
            .fetch_optional(&self.pool)
            .await?;

        row.map(Self::map_row_to_invoice).transpose()
    }

    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<ExpiredInvoiceInfo>> {
        let rows = sqlx::query_as::<_, ExpiredInvoiceInfo>(
            r#"UPDATE invoices
                   SET status = 'Expired'
                   WHERE status = 'Pending' AND expires_at <= now()
                   RETURNING id, network, address"#
        )
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    async fn update_invoice_paid(&self, invoice_id: Uuid, _payment_id: Uuid, paid_raw: U256, new_status: Option<InvoiceStatus>) -> anyhow::Result<()> {
        let paid_bd = BigDecimal::from_str(&paid_raw.to_string())?;

        sqlx::query(
            r#"UPDATE invoices SET paid_raw = $1,
                    status = COALESCE($2, status)
                WHERE id = $3"#
        )
            .bind(paid_bd)
            .bind(new_status.map(|x| x.to_string()))
            .bind(invoice_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}