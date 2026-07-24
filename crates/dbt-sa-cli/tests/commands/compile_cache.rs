//! Tests for injecting a custom `CompiledSqlCache`.
//!
//! All tests run `compile --no-introspect` against the fixture's default
//! target: with introspection disabled dbt builds a mock adapter and never
//! opens a connection, so the render phase (which is what reads and writes
//! the compiled-SQL cache) runs without any network access.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use dbt_clap_core::{Cli, CliParserFactory as _};
use dbt_common::io_args::{EvalArgs, IoArgs};
use dbt_common::tracing::FsTraceConfig;
use dbt_common::tracing::dbt_init::init_tracing;
use dbt_common::tracing::invocation::create_invocation_attributes;
use dbt_common::{CompiledSpans, FsResult, MacroSpan, create_root_info_span};
use dbt_features::cli::DefaultCliParserFactory;
use dbt_features::feature_stack::FeatureStack;
use dbt_features::feature_stack_builder::FeatureStackBuilder;
use dbt_features::tracing::TracingFeature;
use dbt_frontend_common::span::ReclassifySpan;
use dbt_main::CompiledSqlCache;
use dbt_main::compilation::{DbtProjectCompilation, DbtScheduleDescription};
use dbt_schemas::schemas::CommonAttributes;
use dbt_test_utils::task::ProjectEnv;
use tracing::Instrument as _;

const MODEL_UID: &str = "model.hello_world.hello_world";

/// One cached entry, mirroring `CompiledSqlCache::try_get_compiled_sql`'s
/// return tuple.
type CachedEntry = (String, Vec<MacroSpan>, Vec<ReclassifySpan>);

/// HashMap-backed `CompiledSqlCache` that records every call and never
/// touches disk.
#[derive(Default)]
struct RecordingMemoryCache {
    entries: Mutex<HashMap<String, CachedEntry>>,
    set_calls: Mutex<Vec<String>>,
    get_calls: Mutex<Vec<String>>,
}

impl RecordingMemoryCache {
    fn set_calls(&self) -> Vec<String> {
        self.set_calls.lock().unwrap().clone()
    }

    fn get_calls(&self) -> Vec<String> {
        self.get_calls.lock().unwrap().clone()
    }

    fn total_calls(&self) -> usize {
        self.set_calls.lock().unwrap().len() + self.get_calls.lock().unwrap().len()
    }

    fn sql_for(&self, unique_id: &str) -> Option<String> {
        self.entries
            .lock()
            .unwrap()
            .get(unique_id)
            .map(|(sql, ..)| sql.clone())
    }
}

impl CompiledSqlCache for RecordingMemoryCache {
    fn get_compiled_sql_path(&self, io: &IoArgs, common: &CommonAttributes) -> PathBuf {
        // Synthetic path: only used for display; this impl never writes it.
        io.out_dir.join("in-memory").join(&common.unique_id)
    }

    fn try_get_compiled_sql(
        &self,
        _io: &IoArgs,
        common: &CommonAttributes,
    ) -> Option<(String, Vec<MacroSpan>, Vec<ReclassifySpan>)> {
        self.get_calls
            .lock()
            .unwrap()
            .push(common.unique_id.clone());
        self.entries.lock().unwrap().get(&common.unique_id).cloned()
    }

    fn set_compiled_sql(
        &self,
        _io: &IoArgs,
        common: &CommonAttributes,
        rendered_sql_maybe_with_cte: &str,
        spans: &dyn CompiledSpans,
    ) -> FsResult<()> {
        self.set_calls
            .lock()
            .unwrap()
            .push(common.unique_id.clone());
        self.entries.lock().unwrap().insert(
            common.unique_id.clone(),
            (
                rendered_sql_maybe_with_cte.to_string(),
                spans.macro_spans().to_vec(),
                spans.reclassify_spans().unwrap_or_default().to_vec(),
            ),
        );
        Ok(())
    }

    fn clear(&self, unique_id: &str) {
        self.entries.lock().unwrap().remove(unique_id);
    }
}

fn feature_stack() -> Arc<FeatureStack> {
    FeatureStackBuilder::new(TracingFeature::default())
        .send_anonymous_usage_stats(false)
        .build()
        .into()
}

/// dbt's task runner requires fully initialized tracing: its span manager
/// needs spans with real IDs, and invocation metrics live in span extensions
/// populated by dbt's telemetry data layer. Initialize the same global
/// pipeline the CLI uses; the `Err` on re-entry (several runs in one
/// process) means the existing pipeline is reused.
fn install_test_tracing(arg: &EvalArgs) {
    let config = FsTraceConfig::new_from_io_args(
        arg.command,
        Some(&arg.io.in_dir),
        Some(&arg.io.out_dir),
        &arg.io,
        None,
        "dbt-sa-cli-tests",
    );
    if let Ok((handle, _)) = init_tracing(config) {
        // Keep the global pipeline alive for the rest of the test process.
        std::mem::forget(handle);
    }
}

fn compile_cli_and_args(project_dir: &Path, target_dir: &Path) -> (Box<Cli>, EvalArgs) {
    let parser = DefaultCliParserFactory.create("dbt-core", env!("CARGO_PKG_VERSION"));
    let cli = parser.parse_from(vec![
        "dbt".to_string(),
        "compile".to_string(),
        "--no-introspect".to_string(),
        "--no-send-anonymous-usage-stats".to_string(),
        "--no-version-check".to_string(),
        format!("--project-dir={}", project_dir.display()),
        // The TaskSeq harness finds the fixture's profiles.yml by chdir-ing
        // into the project; these tests run in-process and in parallel, so
        // pass the directory explicitly instead.
        format!("--profiles-dir={}", project_dir.display()),
        format!("--target-path={}", target_dir.display()),
        format!(
            "--internal-packages-install-path={}",
            target_dir.join("dbt_internal_packages").display()
        ),
        format!("--log-path={}", target_dir.join("logs").display()),
    ]);
    let arg = cli
        .to_eval_args(dbt_main::from_lib(&cli))
        .expect("failed to build EvalArgs from the compile Cli");
    (cli, arg)
}

/// Full embedder flow with a given config-supplied cache; returns the cache
/// state produced by `run_tasks` so callers can thread it into a second run.
async fn compile_once(
    stack: &Arc<FeatureStack>,
    cli: &Cli,
    arg: &EvalArgs,
    configured_cache: Option<Arc<dyn CompiledSqlCache>>,
    previous_cache_state: Option<Arc<dbt_main::compilation::DbtProjectCompilationCacheState>>,
) -> FsResult<Arc<dbt_main::compilation::DbtProjectCompilationCacheState>> {
    install_test_tracing(arg);
    let token = stack.cli.cancellation_token_source.token();
    let listeners = stack.jinja.factory.create_type_checking_listener_factory();

    // dbt requires all work to happen under a root invocation span (its span
    // manager reads Span::current()), mirroring what run_cli sets up.
    let invocation_span = create_root_info_span(create_invocation_attributes("dbt-sa-cli", arg));

    async {
        let (compilation, jinja_env, changes) = DbtProjectCompilation::initialize_server(
            stack,
            arg,
            cli,
            listeners.clone(),
            None,
            configured_cache,
            &token,
        )
        .await?;

        let schedule = compilation
            .create_schedule(
                cli,
                arg,
                DbtScheduleDescription::Default,
                Default::default(),
                &token,
            )
            .await?;

        let (.., cache_state) = compilation
            .run_tasks(
                arg,
                cli,
                SystemTime::now(),
                jinja_env,
                stack.clone(),
                schedule,
                changes.as_ref(),
                previous_cache_state,
                listeners,
                stack.task_runner.hooks_factory.as_ref(),
                &token,
                Default::default(),
            )
            .await?;

        Ok(cache_state)
    }
    .instrument(invocation_span)
    .await
}

/// Default path: with no cache supplied, compile writes rendered SQL under
/// `<out_dir>/compiled/` exactly as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compile_default_cache_writes_compiled_dir() -> FsResult<()> {
    let env = ProjectEnv::immutable_sa("tests/data/hello_world")?;
    let target_dir = env.absolute_project_dir.join("target");
    let (cli, arg) = compile_cli_and_args(&env.absolute_project_dir, &target_dir);
    let stack = feature_stack();

    compile_once(&stack, &cli, &arg, None, None).await?;

    let compiled_dir = arg.io.out_dir.join("compiled");
    assert!(
        compiled_dir.exists(),
        "default disk cache should write under {}",
        compiled_dir.display()
    );
    Ok(())
}

/// An injected in-memory cache receives the rendered SQL and nothing is
/// written under `<out_dir>/compiled/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compile_with_injected_cache_writes_nothing_to_compiled_dir() -> FsResult<()> {
    let env = ProjectEnv::immutable_sa("tests/data/hello_world")?;
    let target_dir = env.absolute_project_dir.join("target");
    let (cli, arg) = compile_cli_and_args(&env.absolute_project_dir, &target_dir);
    let stack = feature_stack();

    let cache = Arc::new(RecordingMemoryCache::default());
    compile_once(
        &stack,
        &cli,
        &arg,
        Some(cache.clone() as Arc<dyn CompiledSqlCache>),
        None,
    )
    .await?;

    assert!(
        !cache.get_calls().is_empty(),
        "render path never consulted the injected cache"
    );
    assert_eq!(cache.set_calls(), vec![MODEL_UID.to_string()]);
    assert!(cache.sql_for(MODEL_UID).unwrap().contains("Hello World"));
    assert!(
        !arg.io.out_dir.join("compiled").exists(),
        "in-memory cache must prevent any writes under <out_dir>/compiled/"
    );
    Ok(())
}

/// `previous_cache_state` wins over a config-supplied cache, so incremental
/// reuse keeps using the cache from the first run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn previous_cache_state_wins_over_configured_cache() -> FsResult<()> {
    let env = ProjectEnv::immutable_sa("tests/data/hello_world")?;
    let target_dir = env.absolute_project_dir.join("target");
    let (cli, arg) = compile_cli_and_args(&env.absolute_project_dir, &target_dir);
    let stack = feature_stack();

    let cache_a = Arc::new(RecordingMemoryCache::default());
    let cache_b = Arc::new(RecordingMemoryCache::default());

    // Run 1: server configured with cache A.
    let cache_state = compile_once(
        &stack,
        &cli,
        &arg,
        Some(cache_a.clone() as Arc<dyn CompiledSqlCache>),
        None,
    )
    .await?;
    assert_eq!(cache_a.set_calls(), vec![MODEL_UID.to_string()]);

    // Run 2: fresh compilation configured with decoy cache B, but carrying the
    // cache state from run 1 -- the embedder incremental-reuse flow.
    let a_calls_before_run2 = cache_a.total_calls();
    compile_once(
        &stack,
        &cli,
        &arg,
        Some(cache_b.clone() as Arc<dyn CompiledSqlCache>),
        Some(cache_state),
    )
    .await?;

    assert_eq!(
        cache_b.total_calls(),
        0,
        "config-supplied cache must lose to previous_cache_state"
    );
    assert!(
        cache_a.total_calls() > a_calls_before_run2,
        "previous cache saw no traffic in run 2"
    );
    assert!(!arg.io.out_dir.join("compiled").exists());
    Ok(())
}
