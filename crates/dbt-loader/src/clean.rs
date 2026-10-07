use crate::{
    args::LoadArgs,
    dbt_project_yml_loader::{collect_protected_paths, load_project_yml},
    load_for_clean,
};
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
};

use dbt_common::{
    ErrorCode, FsResult,
    cancellation::CancellationToken,
    constants::{DBT_INDEX_DIR_NAME, DBT_METADATA_DIR_NAME, DBT_PROJECT_YML, DBT_TARGET_DIR_NAME},
    err, fs_err,
    io_args::{EvalArgs, EvalArgsBuilder},
    lease,
    path::DbtPath,
    stdfs,
    tracing::{
        dbt_emit::{
            emit_error_log_from_fs_error, emit_info_progress_message, emit_trace_log_message,
        },
        dbt_metrics::error_count_checkpoint,
        event_info::store_event_attributes,
    },
};
use dbt_jinja_utils::{
    invocation_args::InvocationArgs, phases::load::init::initialize_load_jinja_environment,
};
use dbt_schemas::schemas::project::DbtProject;
use dbt_telemetry::{ExecutionPhase, PhaseExecuted, ProgressMessage};

#[tracing::instrument(
    skip_all,
    fields(
        _e = ?store_event_attributes(PhaseExecuted::start_general(ExecutionPhase::Clean)),
    )
)]
pub async fn execute_clean_command(
    arg: &EvalArgs,
    files: &[String],
    _token: &CancellationToken,
) -> FsResult<()> {
    let load_args = LoadArgs::from_eval_args(arg);
    let dbt_state = load_for_clean(&load_args).await?;
    let invocation_args = InvocationArgs::from_eval_args(arg);
    let flags: BTreeMap<String, minijinja::Value> = invocation_args.to_dict();

    let arg = EvalArgsBuilder::from_eval_args(arg)
        .with_threads(dbt_state.dbt_profile.threads)
        .build();

    let env = initialize_load_jinja_environment(
        &dbt_state.dbt_profile.profile,
        &dbt_state.dbt_profile.target,
        dbt_state.dbt_profile.default_db_config().adapter_type(),
        dbt_state.dbt_profile.default_db_config().clone(),
        dbt_state.run_started_at,
        &flags,
        invocation_args.warn_error_options.clone(),
        arg.io.clone(),
        dbt_state.catalogs,
    )?;

    let dbt_project_path = arg.io.in_dir.join(DBT_PROJECT_YML);
    let (dbt_project, _) = load_project_yml(&env, &dbt_project_path, None, arg.vars.clone())?;

    clean_project(&arg, files, &dbt_project, /* clean_targets */ true).await?;

    error_count_checkpoint()
}

pub async fn clean_project(
    arg: &EvalArgs,
    files: &[String],
    dbt_project: &DbtProject,
    clean_targets: bool,
) -> FsResult<()> {
    let target_guard = lease::acquire_lease(&arg.io.in_dir, Path::new(DBT_TARGET_DIR_NAME)).await?;

    let protected_paths = collect_protected_paths(dbt_project)
        .iter()
        .map(|p| DbtPath::absolute(arg.io.in_dir.join(p)))
        .collect::<Result<Vec<_>, _>>()?;

    let default_relative_target_dir = DbtPath::from(DBT_TARGET_DIR_NAME);
    let mut paths_to_delete = dbt_project
        .clean_targets
        .as_ref()
        .unwrap()
        .iter()
        .filter(|_| clean_targets)
        .chain(files.iter())
        .map(|path| {
            let path = Path::new(path);
            if path.is_absolute() {
                err!(
                    ErrorCode::InvalidPath,
                    "Absolute paths are not allowed: {}",
                    path.display()
                )
            } else {
                DbtPath::absolute(arg.io.in_dir.join(path)).map_err(Into::into)
            }
        })
        .collect::<Result<HashSet<_>, _>>()?;

    let out_dir = DbtPath::from(&arg.io.out_dir);
    paths_to_delete.insert(out_dir.clone());
    // Leftover public copies from before index/metadata moved under `private/`.
    // Wiping `out_dir` already removes them; keep the paths explicit so a custom
    // `clean-targets` that does not delete the whole target still hides them.
    paths_to_delete.insert(out_dir.join(DBT_INDEX_DIR_NAME));
    paths_to_delete.insert(out_dir.join(DBT_METADATA_DIR_NAME));

    let all_safe = paths_to_delete.iter().all(|path_to_delete| {
        // The clean command does not delete anything outside of the project directory
        inside_project(&arg.io.in_dir, path_to_delete)
            // The clean command does not delete protected directories ("models", "macros", etc.)
            && protected_paths
                .iter()
                .all(|protected_path| unrelated_paths(protected_path, path_to_delete))
    });

    if all_safe {
        let default_target_dir = DbtPath::from(&arg.io.in_dir).join(&default_relative_target_dir);

        let mut lease_guards = vec![];

        for path in &paths_to_delete {
            if path.exists() {
                // `inside_project` and `unrelated_paths` only collapse `..` without checking for a symlink, so a symlink in the project can route the real delete outside the project or onto a protected directory.
                // Re-check on the canonical paths before leasing, and fail closed.
                // (Checked again right before the delete: the lease waits below can be long.)
                report_canonical_refusal(&arg.io.in_dir, path, &protected_paths)?;
                if path.eq(&default_target_dir) {
                    // We have already acquired the lease for this directory at this point.
                    lease_guards.push((
                        path,
                        default_relative_target_dir.display().to_string(),
                        None,
                    ));
                } else {
                    let Some(relative_path) = path.get_relative_path(&arg.io.in_dir) else {
                        return Err(fs_err!(
                            ErrorCode::InvalidPath,
                            "Unable to get relative path for: {}",
                            path.display(),
                        ));
                    };
                    let lease_guard = lease::acquire_lease(&arg.io.in_dir, &relative_path).await?;
                    lease_guards.push((
                        path,
                        relative_path.display().to_string(),
                        Some(lease_guard),
                    ));
                }
            } else {
                emit_trace_log_message(|| {
                    format!("The target directory does not exist: {}", path.display())
                });
            }
        }

        // Sort lease guards by the display path we show in the terminal
        // for deterministic output.
        let lease_guards = {
            let mut lease_guards = lease_guards.into_iter().collect::<Vec<_>>();
            lease_guards.sort_by(|a, b| a.1.cmp(&b.1));
            lease_guards
        };

        for (path, display_path_string, _) in &lease_guards {
            // The check above ran before the lease waits, and the delete goes by
            // pathname, so re-validate immediately before it: anything that
            // changed the path's meaning in the meantime fails closed here. A
            // pathname check cannot close the window completely (only a delete
            // anchored to a validated directory handle could), but it narrows it
            // to the instant before `remove_dir_all` opens the target.
            report_canonical_refusal(&arg.io.in_dir, path, &protected_paths)?;
            emit_info_progress_message(ProgressMessage::new_from_action_and_target(
                "Removing".to_string(),
                display_path_string.to_string(),
            ));
            stdfs::remove_dir_all(path)?;
        }

        for (_, _, lease_guard) in lease_guards {
            if let Some(lease_guard) = lease_guard {
                // Explicitly release to predictably wait for the lease internally to drop.
                lease_guard.release().await;
            }
        }
    }

    // Explicitly release to predictably wait for the lease internally to drop.
    target_guard.release().await;

    // Remove the agent skills dbt installed into the provider directories. Only
    // dbt's own installs are touched; anything the user authored is left alone.
    if let Err(e) =
        dbt_skills::prune_installed_skills(&arg.io.in_dir, dbt_project, arg.ai_provider.as_deref())
    {
        // A skill-pruning problem should not fail `dbt clean`; warn and move on.
        emit_error_log_from_fs_error(*e);
    }

    Ok(())
}

/// Whether `path` (already absolute and `..`-collapsed) is strictly inside
/// the project directory.
///
/// This used to be `unrelated_paths(in_dir, path)`, which only checks that
/// `path` is not an ancestor of the project: the relative route from `path`
/// back to the project starting with `..` was taken as "safe", but a sibling
/// such as `clean-targets: ["../outside"]` satisfies that too, and
/// `DbtPath::absolute` had already collapsed the `..` -- so `dbt clean`
/// deleted a directory outside the project.
fn inside_project<P: AsRef<Path>, Q: AsRef<Path>>(in_dir: P, path: Q) -> bool {
    let contained = DbtPath::absolute(in_dir.as_ref()).is_ok_and(|root| {
        path.as_ref().starts_with(root.as_path()) && path.as_ref() != root.as_path()
    });
    if !contained {
        emit_error_log_from_fs_error(*fs_err!(
            ErrorCode::InvalidPath,
            "Refusing to clean '{}': it is not inside the project directory {}",
            path.as_ref().display(),
            in_dir.as_ref().display()
        ));
    }
    contained
}

/// [`canonical_safe_to_delete`], with the refusal emitted as an error before it
/// is returned. `clean_project`'s caller exits non-zero on a returned error but
/// never prints it, so without this the user saw `Finished 'clean'
/// successfully` and no deletion, with no explanation -- `inside_project`
/// emits its refusal the same way.
fn report_canonical_refusal<P: AsRef<Path>, Q: AsRef<Path>>(
    in_dir: P,
    path: Q,
    protected_paths: &[DbtPath],
) -> FsResult<()> {
    let path = path.as_ref();
    match canonical_safe_to_delete(in_dir, path, protected_paths) {
        Ok(()) => Ok(()),
        Err(e) => {
            emit_error_log_from_fs_error(*e);
            err!(
                ErrorCode::InvalidPath,
                "Refusing to clean '{}'",
                path.display()
            )
        }
    }
}

/// Check an existing delete target against the project root and the protected directories using canonical paths.
///
/// [`inside_project`] and [`unrelated_paths`] work on the `..`-collapsed path, so they only see the lexical shape of it.
/// A symlink inside the project gets past both. With `project/link -> /hello_world`, `clean-targets: ["link/victim"]`
/// collapses to `project/link/victim`, which starts with the project root, but `remove_dir_all` follows the link and
/// deletes `/hello_world/victim`. With `project/link -> project`, `clean-targets: ["link/models"]` looks unrelated to
/// `project/models` but is the same directory.
///
/// Resolving the path through `canonicalize` removes the symlinks before the two checks run.
///
/// An ordinary directory leaf is canonicalised along with its parent chain, so the comparison sees the
/// filesystem's own spelling of it: on a case-insensitive filesystem `project/Models` and `project/models`
/// are the same directory, and comparing the requested spelling against the canonicalised protected
/// path would miss that. Only a leaf that is itself a symlink is left unresolved (resolved parent plus
/// its own name): `remove_dir_all` unlinks a symlink leaf rather than following it, so a `target/`
/// that is a symlink to another disk is still cleanable, and what gets removed is the link.
/// [`inside_project`] has already established that the leaf is a real component and not the root.
///
/// A path that cannot be resolved is refused, not deleted.
fn canonical_safe_to_delete<P: AsRef<Path>, Q: AsRef<Path>>(
    in_dir: P,
    path: Q,
    protected_paths: &[DbtPath],
) -> FsResult<()> {
    let in_dir = in_dir.as_ref();
    let path = path.as_ref();
    let Some(parent) = path.parent() else {
        return err!(
            ErrorCode::InvalidPath,
            "Refusing to clean '{}': it has no parent directory",
            path.display()
        );
    };

    let (Ok(canonical_root), Ok(canonical_parent)) =
        (stdfs::canonicalize(in_dir), stdfs::canonicalize(parent))
    else {
        return err!(
            ErrorCode::InvalidPath,
            "Refusing to clean '{}': dbt could not resolve it to confirm it stays inside the \
             project directory {}",
            path.display(),
            in_dir.display()
        );
    };

    // The real top-level entry `remove_dir_all` operates on. A symlink leaf is
    // unlinked, not followed, so it stays as the resolved parent plus its own
    // name; anything else is resolved in full so the comparison uses the
    // filesystem's spelling of the leaf too.
    let leaf_is_symlink = std::fs::symlink_metadata(path).is_ok_and(|m| m.is_symlink());
    let canonical_target = if leaf_is_symlink {
        match path.file_name() {
            Some(name) => canonical_parent.join(name),
            None => canonical_parent,
        }
    } else {
        let Ok(resolved) = stdfs::canonicalize(path) else {
            return err!(
                ErrorCode::InvalidPath,
                "Refusing to clean '{}': dbt could not resolve it to confirm it stays inside the \
                 project directory {}",
                path.display(),
                in_dir.display()
            );
        };
        resolved
    };

    if !canonical_target.starts_with(&canonical_root) || canonical_target == canonical_root {
        return err!(
            ErrorCode::InvalidPath,
            "Refusing to clean '{}': it resolves to {}, which is not inside the project directory {}",
            path.display(),
            canonical_target.display(),
            canonical_root.display()
        );
    }

    for protected in protected_paths {
        // A protected path that does not exist cannot be the resolved target of a delete, so there is nothing to canonicalize or guard against.
        let Ok(canonical_protected) = stdfs::canonicalize(protected) else {
            continue;
        };
        // Unsafe when the target is the protected directory or an ancestor of it -- deleting the target would take the protected directory with it.
        if canonical_protected.starts_with(&canonical_target) {
            return err!(
                ErrorCode::InvalidPath,
                "Refusing to clean '{}': it resolves to {}, which would remove the protected \
                 directory {}",
                path.display(),
                canonical_target.display(),
                canonical_protected.display()
            );
        }
    }

    Ok(())
}

fn unrelated_paths<P: AsRef<Path>, Q: AsRef<Path>>(to: P, from: Q) -> bool {
    match stdfs::diff_paths(&to, &from).and_then(|diff| {
        // It is safe to delete a directory if the only way to get to a protected directory is to navigate to the parent.
        if diff.components().next() == Some(std::path::Component::ParentDir) {
            Ok(true)
        } else {
            Err(fs_err!(
                ErrorCode::InvalidPath,
                "The target directory is protected: {}",
                from.as_ref().display()
            ))
        }
    }) {
        Ok(_) => true,
        Err(e) => {
            emit_error_log_from_fs_error(*e);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `clean-targets: ["../outside"]` must not be cleanable (advisory
    /// clean-targets-delete-outside).
    #[test]
    fn inside_project_rejects_siblings_and_the_project_itself() {
        let in_dir = Path::new("/proj");
        let abs = |p: &str| DbtPath::absolute(in_dir.join(p)).unwrap();
        assert!(inside_project(in_dir, abs("target")));
        assert!(inside_project(in_dir, abs("dbt_packages")));
        assert!(inside_project(in_dir, abs("target/../logs")));
        assert!(!inside_project(in_dir, abs("../outside")));
        assert!(!inside_project(in_dir, abs("target/../../outside")));
        assert!(!inside_project(in_dir, abs("..")));
        assert!(!inside_project(in_dir, abs(".")));
        assert!(!inside_project(
            in_dir,
            DbtPath::absolute("/project2").unwrap()
        ));
    }

    /// `canonical_safe_to_delete` must resolve intermediate symlinks BEFORE deleting, refusing targets that escape the project or land on a protected directory.
    /// while leaving genuinely safe targets (including a symlinked leaf, which `remove_dir_all` only unlinks) cleanable.
    #[cfg(unix)]
    #[test]
    fn canonical_safe_to_delete_resolves_symlinks() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let project = root.join("project");
        let outside = root.join("outside");
        let models = project.join("models");
        stdfs::create_dir_all(project.join("real_sub")).unwrap();
        stdfs::create_dir_all(&models).unwrap();
        stdfs::create_dir_all(outside.join("victim")).unwrap();

        let protected = vec![DbtPath::absolute(&models).unwrap()];

        // Intermediate symlink escape: project/link -> outside, so link/victim resolves to outside/victim, outside the project.
        symlink(&outside, project.join("link")).unwrap();
        let escaping = DbtPath::absolute(project.join("link/victim")).unwrap();
        // The lexical check is fooled; the canonical one refuses it.
        assert!(inside_project(&project, &escaping));
        assert!(canonical_safe_to_delete(&project, &escaping, &protected).is_err());

        // Intermediate symlink onto a protected dir: project/self -> project, so self/models looks unrelated to models lexically but resolves onto the protected directory.
        symlink(&project, project.join("self")).unwrap();
        let onto_protected = DbtPath::absolute(project.join("self/models")).unwrap();
        assert!(canonical_safe_to_delete(&project, &onto_protected, &protected).is_err());

        // Symlinked leaf to outside: remove_dir_all unlinks the link itself and never touches the target, so this stays cleanable.
        symlink(&outside, project.join("scratch")).unwrap();
        let leaf_link = DbtPath::absolute(project.join("scratch")).unwrap();
        assert!(canonical_safe_to_delete(&project, &leaf_link, &protected).is_ok());

        // A genuine directory inside the project is cleanable.
        let inside = DbtPath::absolute(project.join("real_sub")).unwrap();
        assert!(canonical_safe_to_delete(&project, &inside, &protected).is_ok());

        // The project root reached through a symlinked parent is still the root:
        // the leaf is an ordinary directory, so it is canonicalised, not kept as
        // `<resolved parent>/<leaf name>`. With `root/alias -> root`, `alias/project`
        // resolves to the project itself and must be refused.
        symlink(root, root.join("alias")).unwrap();
        let root_via_alias = DbtPath::absolute(root.join("alias/project")).unwrap();
        assert!(canonical_safe_to_delete(&project, &root_via_alias, &protected).is_err());

        // A protected directory reached as an ordinary leaf under a symlinked
        // parent is refused on the canonical leaf, not the requested spelling.
        let models_via_alias = DbtPath::absolute(root.join("alias/project/models")).unwrap();
        assert!(canonical_safe_to_delete(&project, &models_via_alias, &protected).is_err());

        // A deleted target is refused rather than resolved to its parent.
        let gone = DbtPath::absolute(project.join("never_existed")).unwrap();
        assert!(canonical_safe_to_delete(&project, &gone, &protected).is_err());
    }

    /// On a case-insensitive filesystem a different spelling names the same
    /// directory; the canonicalised leaf makes the protected-path comparison
    /// see that. On a case-sensitive one the spelling simply does not exist and
    /// is refused, so the test holds either way.
    #[cfg(unix)]
    #[test]
    fn canonical_safe_to_delete_compares_the_filesystems_spelling_of_the_leaf() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path().join("project");
        let models = project.join("models");
        stdfs::create_dir_all(&models).unwrap();
        let protected = vec![DbtPath::absolute(&models).unwrap()];

        let respelled = DbtPath::absolute(project.join("MODELS")).unwrap();
        assert!(canonical_safe_to_delete(&project, &respelled, &protected).is_err());
    }
}
