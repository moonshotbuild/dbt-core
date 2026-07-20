//! Tarball download and extraction client.
//!
//! Downloads tarballs (`.tar.gz`) from URLs and extracts them directly to disk.
//! Uses async streaming extraction (no intermediate files or memory buffering)
//! and automatic retry logic for transient failures.
//! Supports selective extraction with root directory stripping and subdirectory filtering.

use async_compression::tokio::bufread::GzipDecoder;
use dbt_common::cancellation::CancellationToken;
use dbt_common::{ErrorCode, FsResult, err, fs_err};
use futures::StreamExt;
use reqwest::StatusCode;
use reqwest_middleware::ClientWithMiddleware;
use std::io;
use std::path::{Component, Path, PathBuf};
use tokio::fs as tokiofs;
use tokio::io::AsyncBufRead;
use tokio_tar::ArchiveBuilder;
use tokio_util::io::StreamReader;

/// Maximum number of entries accepted from a single archive.
const MAX_ENTRIES: usize = 100_000;
/// Maximum declared uncompressed size of a single entry.
const MAX_ENTRY_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Maximum declared uncompressed size of a whole archive.
const MAX_TOTAL_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Client for downloading and extracting tarball archives.
#[derive(Clone)]
pub struct TarballClient {
    pub client: ClientWithMiddleware,
    cancellation: CancellationToken,
}

impl TarballClient {
    pub fn from_client(client: ClientWithMiddleware, cancellation: CancellationToken) -> Self {
        Self {
            client,
            cancellation,
        }
    }

    /// Download tarball from URL and extract to target directory with optional filtering.
    ///
    /// # Arguments
    /// * `download_url` - URL of the tarball to download
    /// * `target_path` - Directory to extract contents into. **Must already exist
    ///   and be writable; lifecycle (creation and cleanup on error) is the
    ///   caller's responsibility.**
    /// * `strip_root` - If true, strip the single root directory from archive
    /// * `subdirectory` - If provided, only extract entries from this subdirectory
    /// * `headers` - Additional HTTP request headers (e.g. `Authorization`);
    ///   pass `&[]` when none are needed
    ///
    /// Streams download directly from network through gzip decoder to tar extractor,
    /// avoiding intermediate memory buffering or file I/O.
    pub async fn download_and_extract_tarball(
        &self,
        download_url: &str,
        target_path: &Path,
        strip_root: bool,
        subdirectory: Option<&str>,
        headers: &[(&str, &str)],
    ) -> FsResult<PathBuf> {
        self.cancellation.check_cancellation()?;

        let mut req = self.client.get(download_url);
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        let res = req.send().await.map_err(|e| {
            fs_err!(
                ErrorCode::RuntimeError,
                "Failed to get tarball from {download_url}; status: {}",
                e.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            )
        })?;

        if !res.status().is_success() {
            return err!(
                ErrorCode::RuntimeError,
                "Failed to download tarball from {download_url}; status: {}",
                res.status()
            );
        }

        // Convert reqwest stream to AsyncRead
        let stream = res.bytes_stream().map(|result| {
            result.map_err(|e| io::Error::other(format!("Failed to read stream: {}", e)))
        });

        extract_tar_gz(
            StreamReader::new(stream),
            download_url,
            target_path,
            strip_root,
            subdirectory,
            &self.cancellation,
        )
        .await
    }
}

/// Extract a gzipped tar stream into `target_path`.
///
/// Split out from [`TarballClient::download_and_extract_tarball`] so extraction
/// can be exercised against in-memory archives without standing up HTTP.
/// `source` names the archive's origin, and is used only in error messages.
async fn extract_tar_gz<R>(
    reader: R,
    source: &str,
    target_path: &Path,
    strip_root: bool,
    subdirectory: Option<&str>,
    cancellation: &CancellationToken,
) -> FsResult<PathBuf>
where
    R: AsyncBufRead + Unpin + Send,
{
    let decoder = GzipDecoder::new(reader);
    // Package archives are untrusted input. Keep `preserve_permissions` at its
    // default (false) so an archive cannot set setuid/setgid or the exec bit,
    // and disable overwrite: extraction always targets a freshly created
    // directory, so no entry legitimately needs to clobber another.
    let mut archive = ArchiveBuilder::new(decoder)
        .set_allow_external_symlinks(false)
        .set_overwrite(false)
        .build();

    // Resolve the extraction root once so per-entry containment checks compare
    // fully resolved paths (the root itself is often below a symlink, e.g.
    // /var -> /private/var on macOS).
    let canonical_root = tokiofs::canonicalize(target_path).await.map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to resolve extraction root {}: {}",
            target_path.display(),
            e
        )
    })?;

    let mut entries = archive
        .entries()
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to read tar entries: {}", e))?;

    let mut root_dir: Option<String> = None;
    let mut prefix = PathBuf::new();
    let mut extracted_any = false;
    let mut entry_count: usize = 0;
    let mut total_bytes: u64 = 0;

    while let Some(entry_result) = entries.next().await {
        cancellation.check_cancellation()?;

        let mut entry = entry_result
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to read tar entry: {}", e))?;

        // Bound resource consumption before doing any work with the entry. Tar
        // is size-prefixed, so the declared header size is what gets written.
        entry_count += 1;
        if entry_count > MAX_ENTRIES {
            return err!(
                ErrorCode::InvalidConfig,
                "Tarball from {} exceeds the maximum entry count ({})",
                source,
                MAX_ENTRIES
            );
        }
        let entry_size = entry.header().size().unwrap_or(0);
        if entry_size > MAX_ENTRY_BYTES {
            return err!(
                ErrorCode::InvalidConfig,
                "Tar entry exceeds the maximum entry size ({} bytes): {}",
                MAX_ENTRY_BYTES,
                entry
                    .path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default()
            );
        }
        total_bytes = total_bytes.saturating_add(entry_size);
        if total_bytes > MAX_TOTAL_BYTES {
            return err!(
                ErrorCode::InvalidConfig,
                "Tarball from {} exceeds the maximum uncompressed size ({} bytes)",
                source,
                MAX_TOTAL_BYTES
            );
        }

        let entry_path: PathBuf = entry
            .path()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get entry path: {}", e))?
            .into_owned();

        // Tar metadata headers carry no payload of their own. GNU longname /
        // longlink and local pax extensions are consumed inside the crate, but
        // a global pax header is still yielded here (GitHub archives ship one).
        let kind = entry.header().entry_type();
        if kind.is_pax_global_extensions() || kind.is_gnu_longname() || kind.is_gnu_longlink() {
            continue;
        }

        // Determine/validate root directory
        if strip_root {
            // Skip special entries like pax_global_header and macOS resource forks
            let path_str = entry_path.to_string_lossy();
            if path_str == "pax_global_header" || path_str.starts_with("._") {
                continue;
            }

            let first = entry_path
                .components()
                .next()
                .and_then(|c| match c {
                    Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                    _ => None,
                })
                .ok_or_else(|| {
                    fs_err!(
                        ErrorCode::InvalidConfig,
                        "Invalid tar entry path: {}",
                        entry_path.display()
                    )
                })?;

            match &root_dir {
                None => {
                    // Compute prefix once when root is discovered
                    prefix = match subdirectory {
                        Some(subdir) => PathBuf::from(&first).join(subdir),
                        None => PathBuf::from(&first),
                    };
                    root_dir = Some(first);
                }
                Some(existing_root) => {
                    if *existing_root != first {
                        return err!(
                            ErrorCode::InvalidConfig,
                            "Tarball has multiple root directories: '{}' and '{}'. Expected single root directory.",
                            existing_root,
                            first
                        );
                    }
                }
            }
        } else if root_dir.is_none() && subdirectory.is_some() {
            // For non-strip-root with subdirectory, compute prefix once
            root_dir = Some(String::new()); // sentinel to avoid re-entering
            prefix = PathBuf::from(subdirectory.unwrap());
        }

        // Filter: skip entries outside the prefix
        if !prefix.as_os_str().is_empty() && !entry_path.starts_with(&prefix) {
            continue;
        }

        // Strip prefix to get relative path
        let relative_path: &Path = if !prefix.as_os_str().is_empty() {
            entry_path.strip_prefix(&prefix).unwrap_or(&entry_path)
        } else {
            &entry_path
        };

        // Skip empty paths (the prefix directory entry itself)
        if relative_path.as_os_str().is_empty() {
            continue;
        }

        // Security: a dbt package contains only regular files and directories.
        // Symlinks, hard links and device/FIFO nodes never appear in a
        // legitimate package, and allowing them re-opens link-based traversal:
        // the crate's plain `unpack()` applies no containment to link targets,
        // so a hard link can point at any existing absolute path regardless of
        // `allow_external_symlinks`. Rejecting the whole class here closes it
        // independently of the crate version.
        if !(kind.is_file() || kind.is_dir()) {
            return err!(
                ErrorCode::InvalidConfig,
                "Refusing to extract non-regular tar entry ({:?}): {}",
                kind,
                entry_path.display()
            );
        }

        // Security: reject anything but plain path components. This catches
        // ".." traversal and absolute entry paths, which would otherwise escape
        // the root entirely, since `Path::join` on an absolute path discards the
        // base.
        if relative_path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
        {
            return err!(
                ErrorCode::InvalidConfig,
                "Refusing to extract tar entry with path traversal: {}",
                entry_path.display()
            );
        }

        let target_entry_path = target_path.join(relative_path);

        // Defence in depth: resolve the entry's parent and confirm it is still
        // inside the extraction root. Catches anything the component check
        // misses, including a parent that resolves through a symlink.
        if let Some(parent) = target_entry_path.parent() {
            // Archives normally order directory entries before their children,
            // but do not rely on it: create the parent inside the root so it can
            // be resolved.
            tokiofs::create_dir_all(parent).await.map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to create directory {}: {}",
                    parent.display(),
                    e
                )
            })?;
            let canonical_parent = tokiofs::canonicalize(parent).await.map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to resolve directory {}: {}",
                    parent.display(),
                    e
                )
            })?;
            if !canonical_parent.starts_with(&canonical_root) {
                return err!(
                    ErrorCode::InvalidConfig,
                    "Refusing to extract tar entry that escapes the extraction root: {}",
                    entry_path.display()
                );
            }
        }

        entry.unpack(&target_entry_path).await.map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to unpack entry {}: {}",
                entry_path.display(),
                e
            )
        })?;

        extracted_any = true;
    }

    // Validate that we extracted something
    if !extracted_any {
        if let Some(subdir) = subdirectory {
            return err!(
                ErrorCode::InvalidConfig,
                "No entries found matching subdirectory '{}' in tarball from {}",
                subdir,
                source
            );
        } else if strip_root {
            return err!(
                ErrorCode::InvalidConfig,
                "No root directory found in tarball from {}",
                source
            );
        } else {
            return err!(
                ErrorCode::InvalidConfig,
                "No entries found in tarball from {}",
                source
            );
        }
    }

    Ok(target_path.to_path_buf())
}
