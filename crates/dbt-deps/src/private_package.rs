use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;

use async_trait::async_trait;
use dbt_common::{ErrorCode, FsError, FsResult, err};
use dbt_schemas::schemas::ResolvedCloudConfig;

/// A private package definition, rendered and ready to hand to a
/// [`PrivatePackageResolver`].
#[derive(Debug, Clone)]
pub struct PrivatePackageRef {
    pub private_def: String,
    pub provider: Option<String>,
}

/// Extension point for resolving private package definitions to git clone
/// URLs using environment- or platform-provided provider configuration.
///
/// Implementations should return a map keyed by `private_def`; if any entry
/// cannot be resolved, the entire method should fail. If possible, the error
/// returned should have information about the failing entr(y|ies) in the case
/// of a partial failure.
#[async_trait]
pub trait PrivatePackageResolver: Send + Sync {
    async fn resolve_urls(
        &self,
        _entries: &[PrivatePackageRef],
        _cloud_config: &Option<ResolvedCloudConfig>,
    ) -> FsResult<HashMap<String, String>>;
}

/// Resolve using local SSH URLs. This package resolver relies on git being
/// able to authenticate with the provider to pull each repo directly. It
/// does not attempt to use dbt platform for any credential management.
///
/// **Supported providers:**
/// - github
/// - gitlab
/// - ado / azure_devops
///
/// **Unsupported providers:**
/// - azure_active_directory (requires dbt platform resolution)
pub struct LocalPrivatePackageResolver;

#[async_trait]
impl PrivatePackageResolver for LocalPrivatePackageResolver {
    async fn resolve_urls(
        &self,
        entries: &[PrivatePackageRef],
        _cloud_config: &Option<ResolvedCloudConfig>,
    ) -> FsResult<HashMap<String, String>> {
        entries
            .iter()
            .map(|entry| {
                self.get_resolved_url(entry)
                    .map(|url| (entry.private_def.clone(), url))
            })
            .collect()
    }
}

/// Check a `private:` slug before it is interpolated into an SSH clone URL.
///
/// The slug is a package's own value (root or transitive `packages.yml`) and
/// lands verbatim in `git@<host>:<slug>.git`. Require `org/repo` for GitHub,
/// `org/[group/...]/repo` for GitLab and `org/project/repo` for Azure DevOps,
/// every segment drawn from `[A-Za-z0-9._-]`, non-empty, not `.`/`..`, and
/// not starting with `-` (which git would read as an option).
fn validate_private_slug(slug: &str, provider: &str) -> FsResult<()> {
    let parts: Vec<&str> = slug.split('/').collect();
    let count_ok = match provider {
        "github" => parts.len() == 2,
        "gitlab" => parts.len() >= 2,
        "ado" | "azure_devops" => parts.len() == 3,
        _ => true,
    };
    let segment_ok = |seg: &str| {
        !seg.is_empty()
            && seg != "."
            && seg != ".."
            && !seg.starts_with('-')
            && seg
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    };
    if !count_ok || !parts.iter().all(|seg| segment_ok(seg)) {
        return err!(
            ErrorCode::InvalidConfig,
            "Invalid private package '{}' for provider '{}': expected org/repo-style segments of letters, digits, '.', '_' and '-'",
            slug,
            provider
        );
    }
    Ok(())
}

impl LocalPrivatePackageResolver {
    /// Resolves a private package definition to its local (SSH) Git clone URL.
    fn get_resolved_url(&self, private_package: &PrivatePackageRef) -> FsResult<String> {
        // Default to "github" when provider is unspecified, matching dbt-core's behavior
        let provider = private_package.provider.as_deref().unwrap_or("github");
        validate_private_slug(private_package.private_def.deref(), provider)?;
        match provider {
            "github" => Ok(format!(
                "git@github.com:{}.git",
                private_package.private_def.deref()
            )),
            "gitlab" => Ok(format!(
                "git@gitlab.com:{}.git",
                private_package.private_def.deref()
            )),
            "ado" | "azure_devops" => {
                // "ado"/"azure_devops" requires 3-part names: org/project/repo
                let def: PrivateDefinition = private_package.private_def.parse()?;
                if def.path_segments.is_empty() {
                    return err!(
                        ErrorCode::InvalidConfig,
                        "The '{}' provider requires org/project/repo format (3 parts), got: '{}'",
                        private_package.provider.as_deref().unwrap_or_default(),
                        private_package.private_def.deref()
                    );
                }
                Ok(format!(
                    "git@ssh.dev.azure.com:v3/{}",
                    private_package.private_def.deref()
                ))
            }
            _ => {
                err!(
                    ErrorCode::InvalidConfig,
                    r#"Invalid private package configuration: '{}' provider: '{}'. Valid providers for local resolution are: github, gitlab, ado, azure_devops"#,
                    private_package.private_def.deref(),
                    private_package.provider.as_deref().unwrap_or_default()
                )
            }
        }
    }
}

/// A private package definition rendered down to org/[segment/...]/repo,
/// used to match against [`ProviderDetail`] templates. `path_segments` is
/// whatever sits between org and repo — subgroups for GitLab, a project for
/// ADO, always empty for GitHub.
#[derive(Debug, Clone)]
pub struct PrivateDefinition {
    pub org_name: String,
    pub path_segments: Vec<String>,
    pub repo_name: String,
}

impl FromStr for PrivateDefinition {
    type Err = Box<FsError>;

    fn from_str(s: &str) -> FsResult<Self> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() < 2 {
            return err!(
                ErrorCode::InvalidConfig,
                "Private definition must have at least org/repo format, got: '{}'",
                s
            );
        }

        let org_name = parts[0].to_string();
        let repo_name = parts[parts.len() - 1].to_string();
        let path_segments = if parts.len() > 2 {
            parts[1..parts.len() - 1]
                .iter()
                .map(|s| (*s).to_string())
                .collect()
        } else {
            Vec::new()
        };

        Ok(Self {
            org_name,
            path_segments,
            repo_name,
        })
    }
}

impl PrivateDefinition {
    pub fn to_path_string(&self) -> String {
        if self.path_segments.is_empty() {
            format!("{}/{}", self.org_name, self.repo_name)
        } else {
            let segments_str = self.path_segments.join("/");
            format!("{}/{}/{}", self.org_name, segments_str, self.repo_name)
        }
    }

    pub fn is_repo_wildcard(&self) -> bool {
        self.repo_name == "{repo}"
    }
}

#[cfg(test)]
mod slug_tests {
    use super::validate_private_slug;

    #[test]
    fn validate_private_slug_accepts_slugs_and_rejects_injection() {
        assert!(validate_private_slug("dbt-labs/dbt_utils", "github").is_ok());
        assert!(validate_private_slug("org/sub.group/repo", "gitlab").is_ok());
        assert!(validate_private_slug("org/project/repo", "ado").is_ok());
        assert!(validate_private_slug("org/project/repo", "azure_devops").is_ok());

        assert!(validate_private_slug("org/project/repo", "github").is_err());
        assert!(validate_private_slug("org/repo", "ado").is_err());
        assert!(validate_private_slug("org", "github").is_err());
        assert!(validate_private_slug("org//repo", "github").is_err());
        assert!(validate_private_slug("org/../repo", "github").is_err());
        assert!(validate_private_slug("-c/repo", "github").is_err());
        assert!(validate_private_slug("org/repo.git;x", "github").is_err());
        assert!(validate_private_slug("org/repo x", "github").is_err());
        assert!(validate_private_slug("org/re:po", "github").is_err());
    }
}
