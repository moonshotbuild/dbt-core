//! A dependency's `packages.yml` is rendered without the host environment.
//!
//! The root project's `packages.yml` may use `env_var`; an installed
//! dependency's may not, because it is attacker-authored and its entries are
//! installed from (advisory deps-transitive-packages-yml). These drive the real
//! `get_or_install_packages` entry point against `local:` packages.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::{FsCommand, IoArgs};
use dbt_jinja_utils::phases::load::init::initialize_load_profile_jinja_environment;
use fs_deps::get_or_install_packages;
use fs_deps::private_package::LocalPrivatePackageResolver;
use tempfile::TempDir;

struct Tree {
    _tmp: TempDir,
    root: PathBuf,
}

impl Tree {
    fn new(root_packages_yml: &str) -> Self {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("dbt_project.yml"),
            "name: root\nversion: '1.0.0'\nconfig-version: 2\n",
        )
        .unwrap();
        fs::write(root.join("packages.yml"), root_packages_yml).unwrap();
        Self { _tmp: tmp, root }
    }

    /// A sibling package directory with its own `packages.yml`.
    fn package(&self, dir: &str, packages_yml: Option<&str>) {
        let package_root = self.root.parent().unwrap().join(dir);
        fs::create_dir_all(&package_root).unwrap();
        fs::write(
            package_root.join("dbt_project.yml"),
            format!("name: {dir}\nversion: '1.0.0'\nconfig-version: 2\n"),
        )
        .unwrap();
        if let Some(yml) = packages_yml {
            fs::write(package_root.join("packages.yml"), yml).unwrap();
        }
    }

    async fn deps(&self) -> Result<(), Box<dbt_common::FsError>> {
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
            &self.root.join("dbt_packages"),
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

/// Each test owns its own variable: the tests in this binary run in parallel.
#[tokio::test]
async fn the_root_packages_yml_may_use_env_var() {
    const ENV_NAME: &str = "DSK_TRANSITIVE_PACKAGES_ROOT_PATH";
    // SAFETY: the variable is set once, before any thread reads it, and is
    // namespaced to this test.
    unsafe { std::env::set_var(ENV_NAME, "../pkg_a") };
    let tree = Tree::new(&format!(
        "packages:\n  - local: \"{{{{ env_var('{ENV_NAME}') }}}}\"\n"
    ));
    tree.package("pkg_a", None);

    tree.deps().await.expect("the root may read env_var");

    assert!(tree.root.join("dbt_packages/pkg_a").exists());
}

#[tokio::test]
async fn a_dependencys_packages_yml_may_not_use_env_var() {
    const ENV_NAME: &str = "DSK_TRANSITIVE_PACKAGES_DEP_PATH";
    // SAFETY: as above -- set once, namespaced to this test.
    unsafe { std::env::set_var(ENV_NAME, "../pkg_b") };
    let tree = Tree::new("packages:\n  - local: \"../pkg_a\"\n");
    tree.package(
        "pkg_a",
        Some(&format!(
            "packages:\n  - local: \"{{{{ env_var('{ENV_NAME}') }}}}\"\n"
        )),
    );
    tree.package("pkg_b", None);

    let err = tree
        .deps()
        .await
        .expect_err("a dependency may not read env_var");

    assert!(
        err.to_string().contains("env_var is not available"),
        "unexpected error: {err}"
    );
    assert!(
        !tree.root.join("dbt_packages/pkg_b").exists(),
        "the transitive package the root never named was installed"
    );
}

#[tokio::test]
async fn a_dependencys_plain_packages_yml_still_installs() {
    let tree = Tree::new("packages:\n  - local: \"../pkg_a\"\n");
    tree.package("pkg_a", Some("packages:\n  - local: \"../pkg_b\"\n"));
    tree.package("pkg_b", None);

    tree.deps().await.expect("plain transitive deps install");

    assert!(tree.root.join("dbt_packages/pkg_b").exists());
}
