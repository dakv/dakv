#![allow(clippy::module_inception)]
#![allow(unused_doc_comments)]
extern crate lazy_static;
#[macro_use(defer)]
extern crate scopeguard;
#[macro_use]
extern crate quick_error;
extern crate env_logger;
extern crate num_traits;
extern crate rb_tree;
extern crate varint;

pub use crate::db::{Database, Options, ReadOptions, WriteOptions, DB};
pub use crate::env::TestEnv;

// export the macros first.
#[macro_use]
mod macros;
mod db;
mod env;
mod skiplist;
mod table;
mod utils;

pub fn version_info() -> String {
    let fallback = "Unknown env";
    format!(
        "\nRelease Version:   {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}",
        option_env!("DAKV_RELEASE").unwrap_or(fallback),
        option_env!("GIT_COMMIT").unwrap_or(fallback),
        option_env!("GIT_BRANCH").unwrap_or(fallback),
    )
}
