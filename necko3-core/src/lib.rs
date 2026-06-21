pub mod core;
pub mod builder;
pub mod types;
pub mod tasks;

pub mod prelude {
    pub use crate::builder::NeckoCoreBuilder;
    pub use crate::core::NeckoCore;
    pub use necko3_database::traits::DatabaseExt;
}