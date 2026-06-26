pub mod core;
pub mod builder;
pub mod types;
pub mod tasks;
pub mod error;

pub mod prelude {
    pub mod db {
        pub mod backends {
            pub use necko3_database::backends::in_memory::InMemoryAdapter;
            pub use necko3_database::backends::postgres::PostgresAdapter;
        }

        pub use necko3_database::decorators::*;

        pub use necko3_database::traits;
        pub use necko3_database::error::*;
    }
}