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

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::cancellation::never_cancels;

    const BLOCK: usize = 512;

    // Typeflags, per the ustar spec.
    const TYPE_FILE: u8 = b'0';
    const TYPE_HARD_LINK: u8 = b'1';
    const TYPE_SYMLINK: u8 = b'2';
    const TYPE_CHAR_DEVICE: u8 = b'3';
    const TYPE_FIFO: u8 = b'6';
    const TYPE_DIR: u8 = b'5';
    const TYPE_PAX_GLOBAL: u8 = b'g';

    /// Build a ustar header block by hand. Going through the `tar` crate would
    /// not do: it refuses to emit several of the entries these tests need.
    fn header(name: &str, size: u64, typeflag: u8, linkname: &str) -> Vec<u8> {
        let mut h = vec![0u8; BLOCK];

        let write = |h: &mut Vec<u8>, at: usize, bytes: &[u8]| {
            h[at..at + bytes.len()].copy_from_slice(bytes);
        };
        let octal = |value: u64, width: usize| format!("{:0>width$o}\0", value, width = width - 1);

        write(&mut h, 0, name.as_bytes());
        write(&mut h, 100, octal(0o644, 8).as_bytes());
        write(&mut h, 108, octal(0, 8).as_bytes());
        write(&mut h, 116, octal(0, 8).as_bytes());
        write(&mut h, 124, octal(size, 12).as_bytes());
        write(&mut h, 136, octal(0, 12).as_bytes());
        h[156] = typeflag;
        write(&mut h, 157, linkname.as_bytes());
        write(&mut h, 257, b"ustar\0");
        write(&mut h, 263, b"00");

        // Checksum is computed with the checksum field itself read as spaces.
        h[148..156].fill(b' ');
        let sum: u64 = h.iter().map(|b| *b as u64).sum();
        write(&mut h, 148, format!("{:06o}\0 ", sum).as_bytes());

        h
    }

    fn entry(name: &str, typeflag: u8, linkname: &str, data: &[u8]) -> Vec<u8> {
        let mut out = header(name, data.len() as u64, typeflag, linkname);
        if !data.is_empty() {
            out.extend_from_slice(data);
            let pad = (BLOCK - data.len() % BLOCK) % BLOCK;
            out.extend(std::iter::repeat_n(0u8, pad));
        }
        out
    }

    /// An entry declaring a size it does not carry -- for the size-cap tests,
    /// which must reject before any data is read.
    fn entry_declaring(name: &str, size: u64) -> Vec<u8> {
        header(name, size, TYPE_FILE, "")
    }

    /// A regular-file entry carrying an explicit mode, for asserting that
    /// `preserve_permissions(false)` strips it. Overwrites the mode field
    /// (`header` hardcodes 0o644) and rebuilds the checksum.
    fn entry_with_mode(name: &str, mode: u32, data: &[u8]) -> Vec<u8> {
        let mut out = entry(name, TYPE_FILE, "", data);
        let octal = |value: u64, width: usize| format!("{:0>width$o}\0", value, width = width - 1);
        out[100..108].copy_from_slice(octal(mode as u64, 8).as_bytes());
        // Recompute the checksum with its own field read as spaces.
        out[148..156].copy_from_slice(b"        ");
        let sum: u64 = out[..BLOCK].iter().map(|b| *b as u64).sum();
        out[148..156].copy_from_slice(format!("{:06o}\0 ", sum).as_bytes());
        out
    }

    async fn gzip(tar: Vec<u8>) -> Vec<u8> {
        use async_compression::tokio::write::GzipEncoder;
        use tokio::io::AsyncWriteExt;

        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(&tar).await.unwrap();
        encoder.shutdown().await.unwrap();
        encoder.into_inner()
    }

    /// Assemble the entries into a gzipped archive and extract it into a fresh
    /// temp dir, returning that dir alongside the extraction result.
    async fn extract(entries: Vec<Vec<u8>>) -> (tempfile::TempDir, FsResult<PathBuf>) {
        let mut tar: Vec<u8> = entries.concat();
        tar.extend(std::iter::repeat_n(0u8, BLOCK * 2)); // end-of-archive marker
        let gz = gzip(tar).await;

        let dir = tempfile::tempdir().unwrap();
        let result = extract_tar_gz(
            &gz[..],
            "test://archive.tar.gz",
            dir.path(),
            true,
            None,
            &never_cancels(),
        )
        .await;
        (dir, result)
    }

    /// Like [`extract`], but lets a test choose the extraction root, `strip_root`,
    /// and `subdirectory` -- the branches the plain `extract` helper leaves fixed.
    async fn extract_into(
        entries: Vec<Vec<u8>>,
        root: &Path,
        strip_root: bool,
        subdirectory: Option<&str>,
    ) -> FsResult<PathBuf> {
        let mut tar: Vec<u8> = entries.concat();
        tar.extend(std::iter::repeat_n(0u8, BLOCK * 2)); // end-of-archive marker
        let gz = gzip(tar).await;
        extract_tar_gz(
            &gz[..],
            "test://archive.tar.gz",
            root,
            strip_root,
            subdirectory,
            &never_cancels(),
        )
        .await
    }

    fn project_entry() -> Vec<u8> {
        entry("pkg/dbt_project.yml", TYPE_FILE, "", b"name: pkg\n")
    }

    #[tokio::test]
    async fn extracts_a_well_formed_package() {
        let (dir, result) = extract(vec![
            entry("pkg/", TYPE_DIR, "", b""),
            project_entry(),
            entry("pkg/models/a.sql", TYPE_FILE, "", b"select 1\n"),
        ])
        .await;

        assert!(result.is_ok(), "expected success, got {:?}", result.err());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("dbt_project.yml")).unwrap(),
            "name: pkg\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("models/a.sql")).unwrap(),
            "select 1\n"
        );
    }

    /// The reported escape: a symlink out of the extraction root, then a file
    /// written through it. Must be rejected, and the sentinel left untouched.
    #[tokio::test]
    async fn rejects_symlink_escape() {
        let outside = tempfile::tempdir().unwrap();
        let sentinel = outside.path().join("sentinel.txt");
        std::fs::write(&sentinel, "ORIGINAL").unwrap();

        let (dir, result) = extract(vec![
            project_entry(),
            entry(
                "pkg/evil",
                TYPE_SYMLINK,
                outside.path().to_str().unwrap(),
                b"",
            ),
            entry("pkg/evil/sentinel.txt", TYPE_FILE, "", b"OWNED"),
        ])
        .await;

        assert_error_mentions(&result, "non-regular tar entry");
        assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "ORIGINAL");
        assert!(!dir.path().join("evil").exists());
    }

    /// The symlink flag does not cover hard links: the crate's plain `unpack()`
    /// applies no containment to a hard-link target, so this must be rejected on
    /// entry type alone.
    #[tokio::test]
    async fn rejects_hard_link_escape() {
        let outside = tempfile::tempdir().unwrap();
        let secret = outside.path().join("secret.txt");
        std::fs::write(&secret, "SECRET").unwrap();

        let (dir, result) = extract(vec![
            project_entry(),
            entry("pkg/leak", TYPE_HARD_LINK, secret.to_str().unwrap(), b""),
        ])
        .await;

        assert_error_mentions(&result, "non-regular tar entry");
        assert!(!dir.path().join("leak").exists());
    }

    #[tokio::test]
    async fn rejects_device_and_fifo_entries() {
        for typeflag in [TYPE_CHAR_DEVICE, TYPE_FIFO] {
            let (_dir, result) =
                extract(vec![project_entry(), entry("pkg/node", typeflag, "", b"")]).await;
            assert_error_mentions(&result, "non-regular tar entry");
        }
    }

    #[tokio::test]
    async fn rejects_parent_dir_traversal() {
        let (_dir, result) = extract(vec![
            project_entry(),
            entry("pkg/../escape.txt", TYPE_FILE, "", b"OWNED"),
        ])
        .await;

        assert_error_mentions(&result, "path traversal");
    }

    /// `Path::join` on an absolute path discards the base, so an absolute entry
    /// path would write straight to that path if it were not rejected.
    #[tokio::test]
    async fn rejects_absolute_entry_path() {
        let outside = tempfile::tempdir().unwrap();
        let sentinel = outside.path().join("sentinel.txt");
        std::fs::write(&sentinel, "ORIGINAL").unwrap();

        // strip_root would reject a leading "/" before the path checks run, so
        // exercise this with strip_root disabled.
        let mut tar: Vec<u8> =
            [entry(sentinel.to_str().unwrap(), TYPE_FILE, "", b"OWNED")].concat();
        tar.extend(std::iter::repeat_n(0u8, BLOCK * 2));
        let gz = gzip(tar).await;

        let dir = tempfile::tempdir().unwrap();
        let result = extract_tar_gz(
            &gz[..],
            "test://archive.tar.gz",
            dir.path(),
            false,
            None,
            &never_cancels(),
        )
        .await;

        assert_error_mentions(&result, "path traversal");
        assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "ORIGINAL");
    }

    #[tokio::test]
    async fn rejects_entry_over_the_size_cap() {
        let (_dir, result) = extract(vec![
            project_entry(),
            entry_declaring("pkg/bomb.bin", MAX_ENTRY_BYTES + 1),
        ])
        .await;

        assert_error_mentions(&result, "maximum entry size");
    }

    // The running-total cap (MAX_TOTAL_BYTES) is deliberately not unit tested:
    // reaching it needs several entries that each sit under the per-entry cap, so
    // the earlier ones must carry real data and actually unpack -- multiple GiB
    // written to disk. It shares its accounting with the per-entry check above.

    fn assert_error_mentions(result: &FsResult<PathBuf>, needle: &str) {
        match result {
            Ok(path) => panic!("expected rejection, but extraction succeeded at {path:?}"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains(needle),
                    "expected error mentioning {needle:?}, got: {msg}"
                );
            }
        }
    }

    /// Guards the assumption the containment check rests on: nothing is written
    /// outside the extraction root even when the archive is rejected midway.
    #[tokio::test]
    async fn writes_nothing_outside_the_root_on_rejection() {
        let outside = tempfile::tempdir().unwrap();
        let before: Vec<_> = std::fs::read_dir(outside.path()).unwrap().collect();
        assert!(before.is_empty());

        let (_dir, result) = extract(vec![
            project_entry(),
            entry(
                "pkg/evil",
                TYPE_SYMLINK,
                outside.path().to_str().unwrap(),
                b"",
            ),
            entry("pkg/evil/planted.txt", TYPE_FILE, "", b"OWNED"),
        ])
        .await;

        assert!(result.is_err());
        let after: Vec<_> = std::fs::read_dir(outside.path()).unwrap().collect();
        assert!(after.is_empty(), "files were written outside the root");
    }

    /// The extraction root itself sitting under a symlink is the case the
    /// containment check canonicalizes for -- it mirrors `/var -> /private/var`
    /// on macOS and symlinked `TMPDIR`s. A legitimate package must still install:
    /// the resolved children are inside the resolved root, so nothing is flagged
    /// as an escape.
    #[cfg(unix)]
    #[tokio::test]
    async fn extracts_into_a_symlinked_root() {
        let real = tempfile::tempdir().unwrap();
        let target = real.path().join("target");
        std::fs::create_dir(&target).unwrap();
        let link = real.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let result = extract_into(
            vec![
                project_entry(),
                entry("pkg/models/a.sql", TYPE_FILE, "", b"select 1\n"),
            ],
            &link,
            true,
            None,
        )
        .await;

        assert!(result.is_ok(), "expected success, got {:?}", result.err());
        assert_eq!(
            std::fs::read_to_string(target.join("models/a.sql")).unwrap(),
            "select 1\n"
        );
    }

    /// `subdirectory` selects a sub-tree of the package root and extracts it at
    /// the destination root; everything outside it is skipped.
    #[tokio::test]
    async fn extracts_a_subdirectory() {
        let dir = tempfile::tempdir().unwrap();
        let result = extract_into(
            vec![
                project_entry(),
                entry("pkg/other/ignored.sql", TYPE_FILE, "", b"-- ignored\n"),
                entry("pkg/sub/dbt_project.yml", TYPE_FILE, "", b"name: sub\n"),
                entry("pkg/sub/models/b.sql", TYPE_FILE, "", b"select 2\n"),
            ],
            dir.path(),
            true,
            Some("sub"),
        )
        .await;

        assert!(result.is_ok(), "expected success, got {:?}", result.err());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("dbt_project.yml")).unwrap(),
            "name: sub\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("models/b.sql")).unwrap(),
            "select 2\n"
        );
        assert!(!dir.path().join("other").exists());
    }

    /// `strip_root = false` keeps every entry at its archive path. Covers the
    /// non-strip-root install path on the happy side (it is otherwise only seen
    /// through the absolute-path rejection).
    #[tokio::test]
    async fn extracts_without_strip_root() {
        let dir = tempfile::tempdir().unwrap();
        let result = extract_into(
            vec![
                entry("dbt_project.yml", TYPE_FILE, "", b"name: pkg\n"),
                entry("models/a.sql", TYPE_FILE, "", b"select 1\n"),
            ],
            dir.path(),
            false,
            None,
        )
        .await;

        assert!(result.is_ok(), "expected success, got {:?}", result.err());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("models/a.sql")).unwrap(),
            "select 1\n"
        );
    }

    /// A dbt package has a single root directory. Two distinct roots is a
    /// malformed (or crafted) archive and is rejected.
    #[tokio::test]
    async fn rejects_multiple_root_directories() {
        let (_dir, result) = extract(vec![
            project_entry(),
            entry("other/b.sql", TYPE_FILE, "", b"select 2\n"),
        ])
        .await;

        assert_error_mentions(&result, "multiple root directories");
    }

    #[tokio::test]
    async fn errors_on_empty_archive() {
        let (_dir, result) = extract(vec![]).await;
        assert_error_mentions(&result, "No root directory found");
    }

    #[tokio::test]
    async fn errors_when_subdirectory_matches_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let result = extract_into(
            vec![
                project_entry(),
                entry("pkg/models/a.sql", TYPE_FILE, "", b"select 1\n"),
            ],
            dir.path(),
            true,
            Some("nope"),
        )
        .await;

        assert_error_mentions(&result, "No entries found matching subdirectory");
    }

    /// `preserve_permissions(false)`: an archive cannot smuggle setuid/setgid or
    /// the exec bit onto extracted files.
    #[cfg(unix)]
    #[tokio::test]
    async fn strips_setuid_and_exec_bits() {
        use std::os::unix::fs::PermissionsExt;

        let (dir, result) = extract(vec![
            project_entry(),
            entry_with_mode("pkg/tool.sh", 0o4777, b"#!/bin/sh\n"),
        ])
        .await;

        assert!(result.is_ok(), "expected success, got {:?}", result.err());
        let mode = std::fs::metadata(dir.path().join("tool.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o4000, 0, "setuid bit survived: {mode:o}");
        assert_eq!(mode & 0o111, 0, "exec bits survived: {mode:o}");
    }

    /// GitHub source archives ship a pax global header; macOS `tar` adds a
    /// top-level `pax_global_header` and `._`-prefixed AppleDouble entries. All
    /// are skipped without derailing extraction of the real files.
    #[tokio::test]
    async fn skips_pax_global_and_resource_fork_entries() {
        let (dir, result) = extract(vec![
            // pax global extensions header, typeflag 'g' (real GitHub archives ship one).
            entry("pax_global_header", TYPE_PAX_GLOBAL, "", b"17 comment=hello\n"),
            project_entry(),
            entry("._pkg", TYPE_FILE, "", b"apple-double\n"),
            entry("pkg/models/a.sql", TYPE_FILE, "", b"select 1\n"),
        ])
        .await;

        assert!(result.is_ok(), "expected success, got {:?}", result.err());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("models/a.sql")).unwrap(),
            "select 1\n"
        );
        assert!(!dir.path().join("._pkg").exists());
    }
}
