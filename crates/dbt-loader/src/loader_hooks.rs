//! Extension points for customizing loader behavior.
use std::path::Path;

use async_trait::async_trait;
use dbt_cloud_config::ResolvedCloudConfig;
use dbt_common::{FsResult, io_args::IoArgs};
use dbt_schemas::schemas::packages::UpstreamProject;
use dbt_schemas::state::{DbtPackage, DbtProfile};

use crate::args::LoadArgs;

/// Hooks called during the load phase.
#[async_trait]
pub trait LoaderHooks: Send + Sync {
    async fn download_artifacts(
        &self,
        _upstream_projects: &[UpstreamProject],
        _cloud_config: &Option<ResolvedCloudConfig>,
        _io: &IoArgs,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn will_load_internal_packages(
        &self,
        _arg: &LoadArgs,
        _pkgs: &mut Vec<DbtPackage>,
    ) -> FsResult<()> {
        Ok(())
    }

    fn did_persist_packages(
        &self,
        _arg: &LoadArgs,
        _internal_packages_install_path: &Path,
    ) -> FsResult<()> {
        Ok(())
    }

    /// Called once the selected profile target has been rendered and its
    /// thread count resolved, before any adapter is built from it. An
    /// embedder can adjust connection settings here (for example, resolve a
    /// relative DuckDB `path` against the project directory rather than the
    /// process working directory) so that every adapter the invocation
    /// builds sees the same configuration.
    fn did_load_profile(&self, _arg: &LoadArgs, _profile: &mut DbtProfile) -> FsResult<()> {
        Ok(())
    }
}

pub struct NoOpLoaderHooks;

#[async_trait]
impl LoaderHooks for NoOpLoaderHooks {}
