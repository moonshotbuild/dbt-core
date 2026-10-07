//! `dbt deps` must never install a package outside `dbt_packages`.
//!
//! A package's `dbt_project.yml` `name:` becomes its install directory, and
//! `Path::join` discards the base for an absolute name (or walks out of it for
//! `..`). These drive the real `get_or_install_packages` entry point against a
//! `local:` package and assert the escape is refused and nothing lands outside
//! the project.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::{FsCommand, IoArgs};
use dbt_jinja_utils::phases::load::init::initialize_load_profile_jinja_environment;
use fs_deps::get_or_install_packages;
use fs_deps::private_package::LocalPrivatePackageResolver;
use tempfile::TempDir;

const ROOT_PROJECT: &str = "name: root\nversion: '1.0.0'\nconfig-version: 2\n";

struct TestProject {
    _tmp: TempDir,
    root: PathBuf,
}

impl TestProject {
    fn new() -> Self {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("dbt_project.yml"), ROOT_PROJECT).unwrap();
        Self { _tmp: tmp, root }
    }

    /// A sibling directory, outside the project, that must stay untouched.
    fn outside(&self) -> PathBuf {
        self.root.parent().unwrap().join("outside")
    }

    fn with_local_package(&self, dir: &str, declared_name: &str) {
        let package_root = self.root.parent().unwrap().join(dir);
        fs::create_dir_all(package_root.join("models")).unwrap();
        fs::write(package_root.join("models").join("m.sql"), "select 1").unwrap();
        fs::write(
            package_root.join("dbt_project.yml"),
            format!("name: \"{declared_name}\"\nversion: '1.0.0'\nconfig-version: 2\n"),
        )
        .unwrap();
        fs::write(
            self.root.join("packages.yml"),
            format!("packages:\n  - local: \"../{dir}\"\n"),
        )
        .unwrap();
    }

    async fn try_deps(&self) -> Result<(), Box<dbt_common::FsError>> {
        self.try_deps_into(&self.root.join("dbt_packages")).await
    }

    async fn try_deps_into(&self, install_path: &Path) -> Result<(), Box<dbt_common::FsError>> {
        let io = IoArgs {
            in_dir: self.root.clone(),
            out_dir: self.root.join("target"),
            ..Default::default()
        };
        let env = initialize_load_profile_jinja_environment();
        get_or_install_packages(
            &io,
            FsCommand::Deps,
            &env,
            install_path,
            true,
            None,
            false,
            false,
            Default::default(),
            false,
            false,
            None,
            &CancellationToken::never_cancels(),
            false,
            false,
            Arc::new(LocalPrivatePackageResolver),
            None,
            None,
        )
        .await
        .map(|_| ())
    }
}

#[tokio::test]
async fn an_honest_package_name_installs_under_dbt_packages() {
    let project = TestProject::new();
    project.with_local_package("pkg", "honest_pkg");

    project.try_deps().await.expect("deps should succeed");

    assert!(
        project
            .root
            .join("dbt_packages")
            .join("honest_pkg")
            .exists()
    );
}

#[tokio::test]
async fn an_absolute_package_name_is_refused() {
    let project = TestProject::new();
    let escape = project.outside().join("pwned");
    project.with_local_package("pkg", escape.to_str().unwrap());

    let err = project.try_deps().await.expect_err("deps must fail");

    assert!(
        err.to_string().contains("Invalid package name"),
        "unexpected error: {err}"
    );
    assert!(!escape.exists(), "package landed outside the project");
}

#[tokio::test]
async fn a_parent_traversing_package_name_is_refused() {
    let project = TestProject::new();
    project.with_local_package("pkg", "../../outside/pwned");

    let err = project.try_deps().await.expect_err("deps must fail");

    assert!(err.to_string().contains("Invalid package name"), "{err}");
    assert!(!project.outside().exists());
}

/// `dbt deps` removes the install directory wholesale before reinstalling, so
/// an install path outside the project must be refused before that deletion
/// (advisory deps-packages-install-path-delete).
#[tokio::test]
async fn an_install_path_outside_the_project_is_refused_before_deletion() {
    let project = TestProject::new();
    project.with_local_package("pkg", "honest_pkg");
    let outside = project.outside();
    fs::create_dir_all(outside.join("important")).unwrap();
    let sentinel = outside.join("important").join("keep.txt");
    fs::write(&sentinel, "keep me").unwrap();

    for escape in [outside.clone(), project.root.join("../outside")] {
        let err = project
            .try_deps_into(&escape)
            .await
            .expect_err("deps must refuse an install path outside the project");
        assert!(err.to_string().contains("not inside the project"), "{err}");
        assert_eq!(fs::read_to_string(&sentinel).unwrap(), "keep me");
    }
}
