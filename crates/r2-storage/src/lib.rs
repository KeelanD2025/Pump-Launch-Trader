use anyhow::{Result, anyhow};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    Client,
    config::Builder as S3ConfigBuilder,
    error::{DisplayErrorContext, ProvideErrorMetadata},
    primitives::ByteStream,
};
use common::R2Config;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
};
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct R2ClientConfig {
    pub endpoint_redacted: String,
    pub account_id_present: bool,
    pub access_key_present: bool,
    pub secret_key_present: bool,
    pub buckets_configured: BTreeMap<String, bool>,
    pub dry_run: bool,
    pub upload_enabled: bool,
    pub delete_enabled: bool,
    pub managed_prefix: String,
    pub destructive_actions_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct R2ObjectRef {
    pub bucket: String,
    pub key: String,
    pub size_bytes: u64,
    pub checksum_sha256: String,
    pub content_type: String,
    pub metadata: BTreeMap<String, String>,
    pub uploaded_at: Option<OffsetDateTime>,
    pub verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct R2UploadResult {
    pub local_path: String,
    pub bucket: String,
    pub key: String,
    pub size_bytes: u64,
    pub checksum_sha256: String,
    pub uploaded: bool,
    pub verified: bool,
    pub dry_run: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactManifest {
    pub manifest_version: u32,
    pub run_id: String,
    pub source_run_id: Option<String>,
    pub run_kind: String,
    pub run_role: String,
    pub config_hash: String,
    pub idl_hash: String,
    pub calibration_snapshot_hash: Option<String>,
    pub stream_only_enabled: bool,
    pub stream_only_passed: bool,
    pub rpc_network_calls_total: u64,
    pub rpc_credits_used_total: u64,
    pub created_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
    #[serde(default)]
    pub run_status: String,
    #[serde(default)]
    pub finalization_status: String,
    #[serde(default)]
    pub segment_summary: ArtifactManifestSegmentSummary,
    #[serde(default)]
    pub pending_segments: Vec<ArtifactManifestSegmentState>,
    #[serde(default)]
    pub failed_segments: Vec<ArtifactManifestSegmentState>,
    #[serde(default)]
    pub verified_segments: Vec<ArtifactManifestSegmentState>,
    #[serde(default)]
    pub analysis_present: bool,
    #[serde(default)]
    pub readiness_present: bool,
    #[serde(default)]
    pub r2_full_verification_status: String,
    #[serde(default)]
    pub manifest_consistency_checked: bool,
    #[serde(default)]
    pub manifest_consistency_passed: bool,
    #[serde(default)]
    pub manifest_drift_detected: bool,
    #[serde(default)]
    pub manifest_drift_artifacts: Vec<String>,
    #[serde(default)]
    pub r2_object_verification_status: String,
    #[serde(default)]
    pub r2_full_consistency_status: String,
    #[serde(default)]
    pub dataset_index_status: String,
    #[serde(default)]
    pub data_gap_summary: ArtifactManifestDataGapSummary,
    #[serde(default)]
    pub repair_warnings: Vec<String>,
    #[serde(default)]
    pub remote_manifest_key: String,
    #[serde(default)]
    pub uploaded: bool,
    #[serde(default)]
    pub verified: bool,
    #[serde(default)]
    pub uploaded_at: Option<OffsetDateTime>,
    pub artifacts: Vec<ArtifactManifestEntry>,
    pub upload_summary: ArtifactManifestUploadSummary,
    #[serde(default)]
    pub pruning_summary: Option<ArtifactManifestPruningSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArtifactManifestSegmentSummary {
    pub total_segments: usize,
    pub uploaded_segments: usize,
    pub verified_segments: usize,
    pub pending_segments: usize,
    pub failed_segments: usize,
    pub pruned_segments: usize,
    pub local_bytes_remaining: u64,
    pub remote_bytes_verified: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArtifactManifestSegmentState {
    pub segment_id: String,
    pub artifact_type: String,
    pub sequence_number: usize,
    pub local_path: String,
    pub remote_key: String,
    pub size_bytes: u64,
    pub checksum_sha256: String,
    pub uploaded: bool,
    pub verified: bool,
    pub pruned_local: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArtifactManifestDataGapSummary {
    pub source_data_gaps: u64,
    pub slot_gap_count: u64,
    pub data_gap_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArtifactManifestEntry {
    pub artifact_type: String,
    pub local_path: String,
    pub relative_path: String,
    pub bucket: String,
    pub remote_key: String,
    pub size_bytes: u64,
    pub checksum_sha256: String,
    pub compressed: bool,
    pub compression: Option<String>,
    pub uploaded: bool,
    pub verified: bool,
    pub upload_time: Option<OffsetDateTime>,
    pub content_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArtifactManifestUploadSummary {
    pub attempted_count: u64,
    pub uploaded_count: u64,
    pub verified_count: u64,
    pub failed_count: u64,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArtifactManifestPruningSummary {
    pub local_prune_attempted: bool,
    pub local_prune_succeeded: bool,
    pub deleted_paths: Vec<String>,
    pub skipped_paths: Vec<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PreparedUpload {
    pub local_path: PathBuf,
    pub bucket: String,
    pub key: String,
    pub body: Vec<u8>,
    pub content_type: String,
    pub checksum_sha256: String,
    pub metadata: BTreeMap<String, String>,
    pub compressed: bool,
    pub compression: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SegmentIntegrityValidationOptions {
    pub artifact_type: String,
    pub sequence_number: usize,
    pub expected_record_count: u64,
    pub compression: Option<String>,
    pub explicitly_empty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SegmentIntegrityValidationReport {
    pub path: String,
    pub artifact_type: String,
    pub sequence_number: usize,
    pub path_is_open: bool,
    pub file_exists: bool,
    pub size_bytes: u64,
    pub checksum_sha256: String,
    pub compression: Option<String>,
    pub zstd_decode_ok: bool,
    pub jsonl_parse_ok: bool,
    pub parse_error_count: usize,
    pub decoded_record_count: usize,
    pub expected_record_count: u64,
    pub record_count_matches: bool,
    pub final_newline_present: bool,
    pub decoded_prefix_hex: String,
    pub decoded_prefix_is_zstd: bool,
    pub valid: bool,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SegmentIntegrityValidator;

impl SegmentIntegrityValidator {
    pub fn validate_file(
        path: &Path,
        options: SegmentIntegrityValidationOptions,
    ) -> Result<SegmentIntegrityValidationReport> {
        let path_is_open = path
            .file_name()
            .and_then(|value| value.to_str())
            .map(|name| name.ends_with(".open"))
            .unwrap_or(false);
        let file_exists = path.exists();
        let mut report = SegmentIntegrityValidationReport {
            path: path.display().to_string(),
            artifact_type: options.artifact_type.clone(),
            sequence_number: options.sequence_number,
            path_is_open,
            file_exists,
            expected_record_count: options.expected_record_count,
            compression: options.compression.clone(),
            ..Default::default()
        };
        if path_is_open {
            report.errors.push("open_segment_path".to_owned());
        }
        if !file_exists {
            report.errors.push("file_missing".to_owned());
            report.valid = false;
            return Ok(report);
        }
        let bytes = fs::read(path)?;
        Self::validate_bytes_into(&bytes, options, &mut report);
        Ok(report)
    }

    pub fn validate_bytes(
        bytes: &[u8],
        options: SegmentIntegrityValidationOptions,
    ) -> SegmentIntegrityValidationReport {
        let mut report = SegmentIntegrityValidationReport {
            artifact_type: options.artifact_type.clone(),
            sequence_number: options.sequence_number,
            file_exists: true,
            expected_record_count: options.expected_record_count,
            compression: options.compression.clone(),
            ..Default::default()
        };
        Self::validate_bytes_into(bytes, options, &mut report);
        report
    }

    fn validate_bytes_into(
        bytes: &[u8],
        options: SegmentIntegrityValidationOptions,
        report: &mut SegmentIntegrityValidationReport,
    ) {
        report.size_bytes = bytes.len() as u64;
        report.checksum_sha256 = checksum_hex(bytes);
        if bytes.is_empty() && !options.explicitly_empty {
            report.errors.push("segment_empty".to_owned());
        }
        let compressed = options
            .compression
            .as_deref()
            .map(|value| value.eq_ignore_ascii_case("zstd"))
            .unwrap_or(false);
        let decoded = if compressed {
            match zstd::stream::decode_all(bytes) {
                Ok(decoded) => {
                    report.zstd_decode_ok = true;
                    decoded
                }
                Err(error) => {
                    report.zstd_decode_ok = false;
                    report.errors.push(format!("zstd_decode_failed:{error}"));
                    Vec::new()
                }
            }
        } else {
            report.zstd_decode_ok = true;
            bytes.to_vec()
        };
        report.decoded_prefix_hex = bytes_hex_prefix(&decoded, 8);
        report.decoded_prefix_is_zstd = decoded.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]);
        report.final_newline_present = if decoded.is_empty() {
            options.explicitly_empty
        } else {
            decoded.last() == Some(&b'\n')
        };
        if !report.final_newline_present && !options.explicitly_empty {
            report.errors.push("missing_final_newline".to_owned());
        }
        let (record_count, parse_errors) = parse_jsonl_value_record_count(&decoded);
        report.decoded_record_count = record_count;
        report.parse_error_count = parse_errors;
        report.jsonl_parse_ok = parse_errors == 0;
        if !report.jsonl_parse_ok {
            report.errors.push("jsonl_parse_failed".to_owned());
        }
        report.record_count_matches = options.expected_record_count == record_count as u64;
        if !report.record_count_matches {
            report.errors.push(format!(
                "record_count_mismatch:expected={} actual={}",
                options.expected_record_count, record_count
            ));
        }
        if report.decoded_prefix_is_zstd {
            report
                .errors
                .push("decoded_payload_is_zstd_magic_double_compression_suspected".to_owned());
        }
        report.valid = report.errors.is_empty();
    }
}

#[derive(Debug, Clone)]
pub struct ListObjectResult {
    pub key: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
struct R2ResolvedConfig {
    endpoint: String,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    buckets: ResolvedBuckets,
    force_path_style: bool,
    region: String,
}

#[derive(Debug, Clone, Default)]
struct ResolvedBuckets {
    datasets: Option<String>,
    reports: Option<String>,
    calibration: Option<String>,
    provider_compatibility: Option<String>,
}

#[derive(Debug, Clone)]
pub struct R2Client {
    config: R2Config,
    resolved: R2ResolvedConfig,
}

impl R2Client {
    pub fn inspect(config: &R2Config) -> R2ClientConfig {
        let resolved = resolve_config(config);
        let buckets_configured = BTreeMap::from([
            ("datasets".to_owned(), resolved.buckets.datasets.is_some()),
            ("reports".to_owned(), resolved.buckets.reports.is_some()),
            (
                "calibration".to_owned(),
                resolved.buckets.calibration.is_some(),
            ),
            (
                "provider_compat".to_owned(),
                resolved.buckets.provider_compatibility.is_some(),
            ),
        ]);
        R2ClientConfig {
            endpoint_redacted: redact_endpoint(&resolved.endpoint),
            account_id_present: env_value_present(&config.account_id_env),
            access_key_present: env_value_present(&config.access_key_id_env),
            secret_key_present: env_value_present(&config.secret_access_key_env),
            buckets_configured,
            dry_run: config.dry_run,
            upload_enabled: config.upload_enabled,
            delete_enabled: config.delete_enabled,
            managed_prefix: config.managed_prefix.clone(),
            destructive_actions_enabled: config.delete_enabled && !config.dry_run,
        }
    }

    pub fn new(config: &R2Config) -> Result<Self> {
        let resolved = resolve_config(config);
        if config.upload_enabled && !config.dry_run {
            if resolved.access_key_id.is_none() || resolved.secret_access_key.is_none() {
                return Err(anyhow!(
                    "R2 upload requires {} and {} when r2.upload_enabled=true and r2.dry_run=false",
                    config.access_key_id_env,
                    config.secret_access_key_env
                ));
            }
            if resolved.buckets.datasets.is_none()
                && resolved.buckets.reports.is_none()
                && resolved.buckets.calibration.is_none()
                && resolved.buckets.provider_compatibility.is_none()
            {
                return Err(anyhow!(
                    "R2 upload requires at least one configured bucket env when r2.upload_enabled=true"
                ));
            }
        }
        Ok(Self {
            config: config.clone(),
            resolved,
        })
    }

    pub fn bucket_for_reports(&self) -> Result<String> {
        self.bucket_or_placeholder(
            self.resolved.buckets.reports.clone(),
            &self.config.buckets.reports_bucket_env,
        )
    }

    pub fn bucket_for_datasets(&self) -> Result<String> {
        self.bucket_or_placeholder(
            self.resolved.buckets.datasets.clone(),
            &self.config.buckets.datasets_bucket_env,
        )
    }

    pub fn bucket_for_calibration(&self) -> Result<String> {
        self.bucket_or_placeholder(
            self.resolved.buckets.calibration.clone(),
            &self.config.buckets.calibration_bucket_env,
        )
    }

    pub fn bucket_for_provider_compat(&self) -> Result<String> {
        self.bucket_or_placeholder(
            self.resolved
                .buckets
                .provider_compatibility
                .clone()
                .or_else(|| self.resolved.buckets.reports.clone()),
            if self.resolved.buckets.provider_compatibility.is_some() {
                &self.config.buckets.provider_compat_bucket_env
            } else {
                &self.config.buckets.reports_bucket_env
            },
        )
    }

    pub fn managed_key(&self, prefix: &str, relative_path: &str) -> String {
        build_object_key(&self.config.managed_prefix, prefix, relative_path)
    }

    pub fn config(&self) -> &R2Config {
        &self.config
    }

    pub fn prepare_upload(
        &self,
        local_path: &Path,
        bucket: impl Into<String>,
        key: impl Into<String>,
        content_type: impl Into<String>,
        metadata: BTreeMap<String, String>,
        compress_override: Option<bool>,
    ) -> Result<PreparedUpload> {
        let local_path = local_path.to_path_buf();
        let content_type = content_type.into();
        let mut body = fs::read(&local_path)?;
        let already_compressed =
            is_zstd_payload_path(&local_path) || content_type_is_zstd(&content_type);
        let compress =
            compress_override.unwrap_or(self.config.compress_before_upload && !already_compressed);
        let compression = normalize_compression(&self.config.compression);
        let compressed = compress && compression.as_deref() == Some("zstd");
        if compressed {
            body = zstd::stream::encode_all(&body[..], 3)?;
        }
        let checksum_sha256 = checksum_hex(&body);
        Ok(PreparedUpload {
            local_path,
            bucket: bucket.into(),
            key: key.into(),
            body,
            content_type,
            checksum_sha256,
            metadata,
            compressed,
            compression,
        })
    }

    pub async fn upload_prepared(
        &self,
        prepared: &PreparedUpload,
        verify_override: Option<bool>,
    ) -> Result<R2UploadResult> {
        if self.config.dry_run || !self.config.upload_enabled {
            return Ok(R2UploadResult {
                local_path: prepared.local_path.display().to_string(),
                bucket: prepared.bucket.clone(),
                key: prepared.key.clone(),
                size_bytes: prepared.body.len() as u64,
                checksum_sha256: prepared.checksum_sha256.clone(),
                uploaded: false,
                verified: false,
                dry_run: true,
                error: None,
            });
        }
        let client = self.build_sdk_client().await?;
        let mut metadata = prepared
            .metadata
            .clone()
            .into_iter()
            .collect::<HashMap<_, _>>();
        metadata.insert("sha256".to_owned(), prepared.checksum_sha256.clone());
        if prepared.compressed {
            metadata.insert(
                "compression".to_owned(),
                prepared
                    .compression
                    .clone()
                    .unwrap_or_else(|| "zstd".to_owned()),
            );
        }
        client
            .put_object()
            .bucket(&prepared.bucket)
            .key(&prepared.key)
            .content_type(prepared.content_type.clone())
            .set_metadata(Some(metadata))
            .body(ByteStream::from(prepared.body.clone()))
            .send()
            .await
            .map_err(|error| anyhow!(describe_sdk_error("put_object", &error)))?;
        let verify = verify_override.unwrap_or(self.config.verify_after_upload);
        let verified = if verify {
            self.verify_object(
                &prepared.bucket,
                &prepared.key,
                prepared.body.len() as u64,
                Some(&prepared.checksum_sha256),
            )
            .await?
        } else {
            false
        };
        Ok(R2UploadResult {
            local_path: prepared.local_path.display().to_string(),
            bucket: prepared.bucket.clone(),
            key: prepared.key.clone(),
            size_bytes: prepared.body.len() as u64,
            checksum_sha256: prepared.checksum_sha256.clone(),
            uploaded: true,
            verified,
            dry_run: false,
            error: None,
        })
    }

    pub async fn verify_object(
        &self,
        bucket: &str,
        key: &str,
        size_bytes: u64,
        checksum_sha256: Option<&str>,
    ) -> Result<bool> {
        if self.config.dry_run || !self.config.upload_enabled {
            return Ok(false);
        }
        let client = self.build_sdk_client().await?;
        let head = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| anyhow!(describe_sdk_error("head_object", &error)))?;
        let size_matches = head.content_length().unwrap_or_default() as u64 == size_bytes;
        let checksum_matches = checksum_sha256.map_or(true, |expected| {
            head.metadata()
                .and_then(|metadata| metadata.get("sha256"))
                .map(|value| value == expected)
                .unwrap_or(false)
        });
        Ok(size_matches && checksum_matches)
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: Option<i32>,
    ) -> Result<Vec<ListObjectResult>> {
        if self.config.dry_run {
            return Ok(Vec::new());
        }
        let client = self.build_sdk_client().await?;
        let mut request = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix.to_owned());
        if let Some(max_keys) = max_keys {
            request = request.max_keys(max_keys);
        }
        let response = request
            .send()
            .await
            .map_err(|error| anyhow!(describe_sdk_error("list_objects_v2", &error)))?;
        Ok(response
            .contents()
            .iter()
            .map(|object| ListObjectResult {
                key: object.key().unwrap_or_default().to_owned(),
                size_bytes: object.size().unwrap_or_default() as u64,
            })
            .collect())
    }

    pub async fn download_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        if self.config.dry_run {
            return Err(anyhow!("cannot download R2 object in dry-run mode"));
        }
        let client = self.build_sdk_client().await?;
        let response = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| anyhow!(describe_sdk_error("get_object", &error)))?;
        let collected = response
            .body
            .collect()
            .await
            .map_err(|error| anyhow!("get_object body collect failed: {error}"))?;
        Ok(collected.into_bytes().to_vec())
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        if !self.config.delete_enabled || !self.config.buckets.allow_object_delete {
            return Err(anyhow!("R2 object deletion is disabled by configuration"));
        }
        if self.config.dry_run {
            return Ok(());
        }
        let client = self.build_sdk_client().await?;
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| anyhow!(describe_sdk_error("delete_object", &error)))?;
        Ok(())
    }

    pub async fn list_buckets(&self) -> Result<Vec<String>> {
        if self.config.dry_run {
            return Ok(Vec::new());
        }
        let client = self.build_sdk_client().await?;
        let response = client
            .list_buckets()
            .send()
            .await
            .map_err(|error| anyhow!(describe_sdk_error("list_buckets", &error)))?;
        Ok(response
            .buckets()
            .iter()
            .filter_map(|bucket| bucket.name().map(ToOwned::to_owned))
            .collect())
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        if self.config.dry_run {
            return Ok(());
        }
        let client = self.build_sdk_client().await?;
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|error| anyhow!(describe_sdk_error("create_bucket", &error)))?;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        if !self.config.delete_enabled || !self.config.buckets.allow_bucket_delete {
            return Err(anyhow!("R2 bucket deletion is disabled by configuration"));
        }
        if self.config.dry_run {
            return Ok(());
        }
        let client = self.build_sdk_client().await?;
        client
            .delete_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|error| anyhow!(describe_sdk_error("delete_bucket", &error)))?;
        Ok(())
    }

    async fn build_sdk_client(&self) -> Result<Client> {
        let access_key = self
            .resolved
            .access_key_id
            .clone()
            .ok_or_else(|| anyhow!("R2 access key env is not configured"))?;
        let secret_key = self
            .resolved
            .secret_access_key
            .clone()
            .ok_or_else(|| anyhow!("R2 secret key env is not configured"))?;
        let credentials = Credentials::new(access_key, secret_key, None, None, "env");
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(
                self.resolved.region.clone(),
            ))
            .credentials_provider(credentials)
            .load()
            .await;
        let config = S3ConfigBuilder::from(&shared)
            .endpoint_url(self.resolved.endpoint.clone())
            .force_path_style(self.resolved.force_path_style)
            .build();
        Ok(Client::from_conf(config))
    }

    fn bucket_or_placeholder(&self, value: Option<String>, env_name: &str) -> Result<String> {
        value
            .or_else(|| self.config.dry_run.then(|| format!("<{env_name}>")))
            .ok_or_else(|| anyhow!("{env_name} is not configured"))
    }
}

fn describe_sdk_error<E, R>(operation: &str, error: &aws_sdk_s3::error::SdkError<E, R>) -> String
where
    aws_sdk_s3::error::SdkError<E, R>: ProvideErrorMetadata,
    E: std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug,
{
    let code = error.code().unwrap_or("unknown");
    let message = error.message().unwrap_or("service error");
    format!(
        "{operation} failed: code={code} message={message} detail={}",
        DisplayErrorContext(error)
    )
}

pub fn build_object_key(managed_prefix: &str, prefix: &str, relative_path: &str) -> String {
    let clean_prefix = prefix.trim_matches('/');
    let clean_relative = relative_path.trim_start_matches('/');
    if clean_prefix.is_empty() {
        format!("{}/{}", managed_prefix.trim_matches('/'), clean_relative)
    } else {
        format!(
            "{}/{}/{}",
            managed_prefix.trim_matches('/'),
            clean_prefix,
            clean_relative
        )
    }
}

pub fn checksum_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn bytes_hex_prefix(bytes: &[u8], max_len: usize) -> String {
    bytes
        .iter()
        .take(max_len)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn parse_jsonl_value_record_count(bytes: &[u8]) -> (usize, usize) {
    let mut decoded_record_count = 0usize;
    let mut parse_error_count = 0usize;
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        if serde_json::from_slice::<serde_json::Value>(line).is_ok() {
            decoded_record_count = decoded_record_count.saturating_add(1);
            continue;
        }
        parse_error_count = parse_error_count.saturating_add(1);
    }
    (decoded_record_count, parse_error_count)
}

pub fn load_manifest(path: &Path) -> Result<ArtifactManifest> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

pub fn write_manifest(path: &Path, manifest: &ArtifactManifest) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(manifest)?)?;
    Ok(())
}

pub fn redact_endpoint(endpoint: &str) -> String {
    if endpoint.trim().is_empty() {
        return String::new();
    }
    let trimmed = endpoint.trim();
    if let Some((scheme, remainder)) = trimmed.split_once("://") {
        let host = remainder.split('/').next().unwrap_or_default();
        let redacted_host = if host.contains(".r2.cloudflarestorage.com") {
            "<redacted-account>.r2.cloudflarestorage.com".to_owned()
        } else {
            "<redacted-endpoint>".to_owned()
        };
        format!("{scheme}://{redacted_host}")
    } else {
        "<redacted-endpoint>".to_owned()
    }
}

fn resolve_config(config: &R2Config) -> R2ResolvedConfig {
    let account_id = env_value(&config.account_id_env);
    let endpoint = env_value(&config.endpoint_env).unwrap_or_else(|| {
        account_id
            .as_ref()
            .map(|account_id| format!("https://{account_id}.r2.cloudflarestorage.com"))
            .unwrap_or_default()
    });
    R2ResolvedConfig {
        endpoint,
        access_key_id: env_value(&config.access_key_id_env),
        secret_access_key: env_value(&config.secret_access_key_env),
        buckets: ResolvedBuckets {
            datasets: env_value(&config.buckets.datasets_bucket_env),
            reports: env_value(&config.buckets.reports_bucket_env),
            calibration: env_value(&config.buckets.calibration_bucket_env),
            provider_compatibility: env_value(&config.buckets.provider_compat_bucket_env),
        },
        force_path_style: config.force_path_style,
        region: config.region.clone(),
    }
}

fn normalize_compression(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        None
    } else {
        Some(trimmed.to_ascii_lowercase())
    }
}

fn is_zstd_payload_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.eq_ignore_ascii_case("zst"))
        .unwrap_or(false)
}

fn content_type_is_zstd(content_type: &str) -> bool {
    let lower = content_type.to_ascii_lowercase();
    lower.contains("zstd") || lower.contains("zst") || lower == "application/octet-stream+zstd"
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn env_value_present(name: &str) -> bool {
    env_value(name).is_some()
}

#[cfg(test)]
fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(test)]
fn with_env_vars<F>(vars: &[(&str, Option<&str>)], callback: F)
where
    F: FnOnce(),
{
    struct EnvRestoreGuard {
        previous: Vec<(String, Option<String>)>,
    }

    impl Drop for EnvRestoreGuard {
        fn drop(&mut self) {
            for (key, value) in self.previous.drain(..) {
                // SAFETY: Tests serialize environment mutation through `env_lock`,
                // restore only process-local keys captured before mutation, and do
                // not retain references into the environment across this scope.
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(&key, value),
                        None => std::env::remove_var(&key),
                    }
                }
            }
        }
    }

    let _guard = env_lock().lock().expect("env lock");
    let previous = vars
        .iter()
        .map(|(key, _)| ((*key).to_owned(), std::env::var(key).ok()))
        .collect::<Vec<_>>();
    let _restore = EnvRestoreGuard { previous };
    for (key, value) in vars {
        // SAFETY: Tests serialize environment mutation through `env_lock`, and
        // this helper is used only in short-lived unit tests that do not share
        // borrowed environment data across the mutation boundary.
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
    callback();
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::R2Config;

    fn config() -> R2Config {
        R2Config::default()
    }

    #[test]
    fn endpoint_derives_from_account_id() {
        with_env_vars(
            &[
                ("CF_ACCOUNT_ID", Some("example-account")),
                ("R2_ENDPOINT", None),
                ("R2_ACCESS_KEY_ID", None),
                ("R2_SECRET_ACCESS_KEY", None),
            ],
            || {
                let client = R2Client::new(&config()).expect("client");
                assert_eq!(
                    client.resolved.endpoint,
                    "https://example-account.r2.cloudflarestorage.com"
                );
            },
        );
    }

    #[test]
    fn dry_run_upload_returns_planned_result() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("artifact.txt");
        fs::write(&path, b"artifact").expect("write");
        with_env_vars(
            &[
                ("CF_ACCOUNT_ID", Some("example-account")),
                ("R2_ACCESS_KEY_ID", None),
                ("R2_SECRET_ACCESS_KEY", None),
            ],
            || {
                let client = R2Client::new(&config()).expect("client");
                let prepared = client
                    .prepare_upload(
                        &path,
                        "reports-bucket",
                        client.managed_key("reports", "run/artifact.txt.zst"),
                        "text/plain",
                        BTreeMap::new(),
                        Some(true),
                    )
                    .expect("prepare");
                let result = tokio::runtime::Runtime::new()
                    .expect("rt")
                    .block_on(client.upload_prepared(&prepared, Some(true)))
                    .expect("upload");
                assert!(result.dry_run);
                assert!(!result.uploaded);
                assert_eq!(result.bucket, "reports-bucket");
            },
        );
    }

    #[test]
    fn zstd_payloads_are_not_double_compressed_by_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("segment_normalized_events_00010.jsonl.zst");
        let payload = zstd::stream::encode_all(
            br#"{"event":"ok"}
"#
            .as_slice(),
            3,
        )
        .expect("zstd");
        fs::write(&path, &payload).expect("write");
        with_env_vars(
            &[
                ("CF_ACCOUNT_ID", Some("example-account")),
                ("R2_ACCESS_KEY_ID", None),
                ("R2_SECRET_ACCESS_KEY", None),
            ],
            || {
                let client = R2Client::new(&config()).expect("client");
                let prepared = client
                    .prepare_upload(
                        &path,
                        "datasets-bucket",
                        client.managed_key(
                            "runs/run-id/segments",
                            "segment_normalized_events_00010.jsonl.zst",
                        ),
                        "application/zstd",
                        BTreeMap::new(),
                        None,
                    )
                    .expect("prepare");
                assert!(!prepared.compressed);
                assert_eq!(prepared.body, payload);
                assert_eq!(prepared.checksum_sha256, checksum_hex(&payload));
            },
        );
    }

    #[test]
    fn segment_integrity_validator_accepts_valid_zstd_jsonl() {
        let payload = zstd::stream::encode_all(
            br#"{"event":"one"}
{"event":"two"}
"#
            .as_slice(),
            3,
        )
        .expect("zstd");
        let report = SegmentIntegrityValidator::validate_bytes(
            &payload,
            SegmentIntegrityValidationOptions {
                artifact_type: "normalized_events".to_owned(),
                sequence_number: 1,
                expected_record_count: 2,
                compression: Some("zstd".to_owned()),
                explicitly_empty: false,
            },
        );
        assert!(report.valid, "{:?}", report.errors);
        assert!(report.zstd_decode_ok);
        assert!(report.jsonl_parse_ok);
        assert!(report.final_newline_present);
        assert_eq!(report.decoded_record_count, 2);
    }

    #[test]
    fn segment_integrity_validator_rejects_missing_final_newline() {
        let payload = zstd::stream::encode_all(br#"{"event":"one"}"#.as_slice(), 3).expect("zstd");
        let report = SegmentIntegrityValidator::validate_bytes(
            &payload,
            SegmentIntegrityValidationOptions {
                artifact_type: "normalized_events".to_owned(),
                sequence_number: 1,
                expected_record_count: 1,
                compression: Some("zstd".to_owned()),
                explicitly_empty: false,
            },
        );
        assert!(!report.valid);
        assert!(
            report
                .errors
                .iter()
                .any(|error| error == "missing_final_newline")
        );
    }

    #[test]
    fn segment_integrity_validator_rejects_malformed_jsonl() {
        let payload = zstd::stream::encode_all(b"{bad json}\n".as_slice(), 3).expect("zstd");
        let report = SegmentIntegrityValidator::validate_bytes(
            &payload,
            SegmentIntegrityValidationOptions {
                artifact_type: "normalized_events".to_owned(),
                sequence_number: 1,
                expected_record_count: 1,
                compression: Some("zstd".to_owned()),
                explicitly_empty: false,
            },
        );
        assert!(!report.valid);
        assert!(
            report
                .errors
                .iter()
                .any(|error| error == "jsonl_parse_failed")
        );
    }

    #[test]
    fn segment_integrity_validator_rejects_open_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir
            .path()
            .join("segment_normalized_events_00001.jsonl.open");
        fs::write(&path, b"{\"event\":\"one\"}\n").expect("write");
        let report = SegmentIntegrityValidator::validate_file(
            &path,
            SegmentIntegrityValidationOptions {
                artifact_type: "normalized_events".to_owned(),
                sequence_number: 1,
                expected_record_count: 1,
                compression: None,
                explicitly_empty: false,
            },
        )
        .expect("validate");
        assert!(!report.valid);
        assert!(report.path_is_open);
        assert!(
            report
                .errors
                .iter()
                .any(|error| error == "open_segment_path")
        );
    }

    #[test]
    fn segment_integrity_validator_flags_double_compressed_zstd() {
        let payload = zstd::stream::encode_all(
            br#"{"event":"one"}
"#
            .as_slice(),
            3,
        )
        .expect("zstd");
        let double_compressed = zstd::stream::encode_all(payload.as_slice(), 3).expect("zstd");
        let report = SegmentIntegrityValidator::validate_bytes(
            &double_compressed,
            SegmentIntegrityValidationOptions {
                artifact_type: "normalized_events".to_owned(),
                sequence_number: 1,
                expected_record_count: 1,
                compression: Some("zstd".to_owned()),
                explicitly_empty: false,
            },
        );
        assert!(!report.valid);
        assert!(report.decoded_prefix_is_zstd);
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("double_compression"))
        );
    }

    #[test]
    fn missing_credentials_fail_when_upload_enabled() {
        with_env_vars(
            &[
                ("CF_ACCOUNT_ID", Some("example-account")),
                ("R2_ACCESS_KEY_ID", None),
                ("R2_SECRET_ACCESS_KEY", None),
                ("R2_REPORTS_BUCKET", Some("reports-bucket")),
            ],
            || {
                let mut config = config();
                config.upload_enabled = true;
                config.dry_run = false;
                let error = R2Client::new(&config).expect_err("missing creds");
                assert!(
                    error
                        .to_string()
                        .contains("R2 upload requires R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY")
                );
            },
        );
    }

    #[test]
    fn secret_values_are_redacted_in_inspect_output() {
        with_env_vars(
            &[
                ("CF_ACCOUNT_ID", Some("example-account")),
                ("R2_ACCESS_KEY_ID", Some("super-secret-id")),
                ("R2_SECRET_ACCESS_KEY", Some("super-secret-key")),
                ("R2_REPORTS_BUCKET", Some("reports-bucket")),
            ],
            || {
                let inspect = R2Client::inspect(&config());
                let rendered = serde_json::to_string(&inspect).expect("json");
                assert!(!rendered.contains("super-secret-id"));
                assert!(!rendered.contains("super-secret-key"));
                assert!(rendered.contains("<redacted-account>"));
            },
        );
    }

    #[test]
    fn object_key_construction_is_stable() {
        assert_eq!(
            build_object_key("pump-launch-quant", "reports", "run_id/report.md"),
            "pump-launch-quant/reports/run_id/report.md"
        );
    }

    #[test]
    fn checksum_calculation_is_stable() {
        assert_eq!(
            checksum_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn delete_object_is_denied_by_default() {
        with_env_vars(&[], || {
            let client = R2Client::new(&config()).expect("client");
            let error = tokio::runtime::Runtime::new()
                .expect("rt")
                .block_on(client.delete_object("reports-bucket", "key"))
                .expect_err("delete denied");
            assert!(
                error
                    .to_string()
                    .contains("R2 object deletion is disabled by configuration")
            );
        });
    }
}
