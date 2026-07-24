use std::sync::Arc;

use dbt_common::io_args::FsCommand;
pub use dbt_tasks_core::CompiledSqlCache;

/// Common configuration for compilation pipeline
#[derive(Clone)]
pub struct CompilationConfig {
    /// Whether to use the build cache to determine the schedule
    pub use_build_cache_for_scheduling: bool,
    /// Commands that support caching
    pub cacheable_commands: Vec<FsCommand>,
    /// Disables local compute checks
    pub disable_local_compute_checks: bool,
    /// When hydrating schemas, use the resolver state's view of the world
    pub use_resolver_state_deps: bool,
    /// When true, disables checking versions
    pub no_version_check: bool,
    /// When true, the schedule used when initializing
    /// a schema store is for all nodes in the project.
    pub use_full_schema_store: bool,
    /// Cache for compiled (rendered) SQL. When `None`, the default
    /// disk-backed cache is used, which writes compiled SQL under the
    /// project's target directory (`<out_dir>/compiled/...`).
    /// Embedding hosts can supply an in-memory implementation so compilation
    /// never writes into the user's target directory.
    ///
    /// Only consulted when there is no previous cache state - once tasks have run,
    /// the resolved cache is carried forward via the compilation cache state.
    pub compiled_sql_cache: Option<Arc<dyn CompiledSqlCache>>,
}

impl Default for CompilationConfig {
    fn default() -> Self {
        Self {
            use_build_cache_for_scheduling: true,
            cacheable_commands: vec![
                FsCommand::Parse,
                FsCommand::Compile,
                FsCommand::Run,
                FsCommand::Test,
                FsCommand::Extension("lineage"),
                FsCommand::Seed,
            ],
            disable_local_compute_checks: false,
            use_resolver_state_deps: false,
            no_version_check: false,
            use_full_schema_store: false,
            compiled_sql_cache: None,
        }
    }
}
