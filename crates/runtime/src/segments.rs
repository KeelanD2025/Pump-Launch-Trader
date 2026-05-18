use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow, Context};
use common::{LoadedConfig, RuntimeModeName, SCHEMA_VERSION};
use features::FeatureSnapshot;
use r2_storage::{R2Client, SegmentIntegrityValidationOptions, SegmentIntegrityValidator};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::StoredRecord;
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct RunSegmentManifestSummary {
    pub(crate) total_segments: usize,
    pub(crate) uploaded_segments: usize,
    pub(crate) verified_segments: usize,
    pub(crate) pruned_segments: usize,
    pub(crate) local_bytes_remaining: u64,
    pub(crate) remote_bytes_verified: u64,
    pub(crate) failed_segments: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct RunSegmentEntry {
    pub(crate) segment_id: String,
    pub(crate) artifact_type: String,
    pub(crate) run_id: String,
    pub(crate) sequence_number: usize,
    pub(crate) local_path: String,
    #[serde(default)]
    pub(crate) source_paths: Vec<String>,
    pub(crate) remote_key: String,
    pub(crate) size_bytes: u64,
    pub(crate) checksum_sha256: String,
    pub(crate) record_count: u64,
    pub(crate) first_event_time: Option<OffsetDateTime>,
    pub(crate) last_event_time: Option<OffsetDateTime>,
    pub(crate) compression: Option<String>,
    pub(crate) uploaded: bool,
    pub(crate) verified: bool,
    pub(crate) pruned_local: bool,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunSegmentManifest {
    pub(crate) run_id: String,
    pub(crate) schema_version: u32,
    pub(crate) created_at: OffsetDateTime,
    pub(crate) updated_at: OffsetDateTime,
    pub(crate) segments: Vec<RunSegmentEntry>,
    pub(crate) summary: RunSegmentManifestSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SegmentStatusSummary {
    pub(crate) run_id: String,
    pub(crate) updated_at: Option<OffsetDateTime>,
    pub(crate) active_segment_count: usize,
    pub(crate) closed_pending_upload_count: usize,
    pub(crate) segment_upload_backlog: usize,
    pub(crate) verified_pruned_segment_count: usize,
    pub(crate) local_segment_bytes: u64,
    pub(crate) remote_verified_segment_bytes: u64,
    pub(crate) last_segment_upload_error: Option<String>,
    pub(crate) disk_warning_active: bool,
    pub(crate) last_disk_cleanup_action: Option<String>,
    pub(crate) upload_backlog_max: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiskActionRecord {
    pub(crate) at: OffsetDateTime,
    pub(crate) level: String,
    pub(crate) action: String,
    pub(crate) free_mb: u64,
    pub(crate) pending_segments: usize,
    pub(crate) pruned_bytes: u64,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DiskActionReport {
    run_id: String,
    updated_at: Option<OffsetDateTime>,
    actions: Vec<DiskActionRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RuntimeExportChunkManifestEntry {
    export_name: String,
    local_path: String,
    size_bytes: u64,
    uploaded: bool,
    verified: bool,
    pruned_local: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RuntimeExportChunkManifest {
    run_id: String,
    export_status: String,
    #[serde(default)]
    skipped_exports: Vec<String>,
    #[serde(default)]
    chunks: Vec<RuntimeExportChunkManifestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct LocalFootprintBudgetSummary {
    pub(crate) run_id: String,
    pub(crate) updated_at: Option<OffsetDateTime>,
    pub(crate) low_disk_mode_enabled: bool,
    pub(crate) active_open_segment_bytes: u64,
    pub(crate) closed_unverified_segment_bytes: u64,
    pub(crate) closed_verified_local_segment_bytes: u64,
    pub(crate) export_bytes: u64,
    pub(crate) report_bytes: u64,
    pub(crate) restore_cache_bytes: u64,
    pub(crate) temp_bytes: u64,
    pub(crate) manifests_summaries_bytes: u64,
    pub(crate) total_local_runtime_bytes: u64,
    pub(crate) local_runtime_budget_bytes: u64,
    pub(crate) local_runtime_budget_overage_bytes: u64,
    pub(crate) free_mb: u64,
    pub(crate) verified_segment_bytes_pruned_total: u64,
    pub(crate) verified_export_bytes_pruned_total: u64,
    pub(crate) optional_exports_skipped_total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SegmentProcessSummary {
    pub(crate) uploaded_count: usize,
    pub(crate) verified_count: usize,
    pub(crate) pruned_bytes: u64,
    pub(crate) pending_count: usize,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentTextFileRecord {
    relative_path: String,
    body: String,
    #[serde(default)]
    source_path: Option<String>,
}

#[derive(Debug, Clone)]
struct OpenSegment {
    artifact_type: String,
    sequence_number: usize,
    open_path: PathBuf,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    size_bytes: u64,
    record_count: u64,
    first_event_time: Option<OffsetDateTime>,
    last_event_time: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Default)]
struct SegmentRetryState {
    attempts: u64,
    last_attempt_at: Option<OffsetDateTime>,
}

pub(crate) struct LiveRunSegmentManager {
    run_id: String,
    report_root: PathBuf,
    edge_mode: bool,
    manifest: RunSegmentManifest,
    open_segments: BTreeMap<String, OpenSegment>,
    next_sequence_by_type: BTreeMap<String, usize>,
    retry_state: BTreeMap<String, SegmentRetryState>,
    disk_actions: Vec<DiskActionRecord>,
    max_segment_size_bytes: u64,
    max_segment_age_seconds: u64,
    keep_last_n_segments_local: usize,
    compress_closed_segments: bool,
    compression: Option<String>,
    upload_enabled: bool,
    verify_before_local_delete: bool,
    delete_verified_segments: bool,
    upload_closed_segments_during_run: bool,
    verify_closed_segments: bool,
    delete_verified_closed_segments: bool,
    max_concurrent_segment_uploads: usize,
    max_pending_segments: usize,
    max_pending_segments_warning: usize,
    max_pending_segments_pause: usize,
    max_pending_segments_stop: usize,
    pause_collection_if_pending_segments_exceed: usize,
    pause_new_token_tracking_on_backlog: bool,
    pause_feature_snapshot_writes_on_backlog: bool,
    force_close_and_upload_on_backlog: bool,
    retry_failed_uploads: bool,
    max_upload_retries: u64,
    retry_backoff_seconds: u64,
    alert_on_upload_backlog: bool,
    last_segment_upload_error: Option<String>,
    disk_warning_active: bool,
    last_disk_cleanup_action: Option<String>,
    max_observed_upload_backlog: usize,
    low_disk_mode_enabled: bool,
    target_local_runtime_bytes: u64,
    max_local_verified_segments: usize,
    max_local_unverified_closed_segments: usize,
    keep_open_segments_only: bool,
    delete_verified_segments_immediately: bool,
    delete_verified_exports_immediately: bool,
    verified_segment_bytes_pruned_total: u64,
    verified_export_bytes_pruned_total: u64,
    optional_exports_skipped_total: u64,
    local_budget_pressure_active: bool,
    r2_client: Option<R2Client>,
}

impl LiveRunSegmentManager {
    pub(crate) fn from_loaded(loaded: &LoadedConfig, run_id: &str) -> Result<Option<Self>> {
        if !loaded.config.storage.segments.enabled {
            return Ok(None);
        }
        let report_root = Path::new(&loaded.config.storage.root)
            .join(&loaded.config.storage.report_dir)
            .join(run_id);
        fs::create_dir_all(&report_root)?;
        let created_at = OffsetDateTime::now_utc();
        let mut manifest = load_segment_manifest(&report_root)?.unwrap_or(RunSegmentManifest {
            run_id: run_id.to_owned(),
            schema_version: SCHEMA_VERSION,
            created_at,
            updated_at: created_at,
            segments: Vec::new(),
            summary: RunSegmentManifestSummary::default(),
        });
        refresh_manifest_summary(&mut manifest);
        let next_sequence_by_type =
            manifest
                .segments
                .iter()
                .fold(BTreeMap::<String, usize>::new(), |mut acc, segment| {
                    let next = segment.sequence_number.saturating_add(1);
                    acc.entry(segment.artifact_type.clone())
                        .and_modify(|value| *value = (*value).max(next))
                        .or_insert(next);
                    acc
                });
        let upload_config = &loaded.config.storage.segments.upload;
        let r2_client = if upload_config.enabled
            && upload_config.upload_closed_segments_during_run
            && loaded.config.storage.segments.upload_closed_segments
            && loaded.config.r2.enabled
        {
            match R2Client::new(&loaded.config.r2) {
                Ok(client) => Some(client),
                Err(error) => {
                    let mut manager = Self {
                        run_id: run_id.to_owned(),
                        report_root,
                        edge_mode: loaded.config.runtime.mode == RuntimeModeName::EdgeCollector,
                        manifest,
                        open_segments: BTreeMap::new(),
                        next_sequence_by_type,
                        retry_state: BTreeMap::new(),
                        disk_actions: Vec::new(),
                        max_segment_size_bytes: loaded
                            .config
                            .storage
                            .segments
                            .max_segment_size_mb
                            .saturating_mul(1024 * 1024)
                            .max(1),
                        max_segment_age_seconds: loaded
                            .config
                            .storage
                            .segments
                            .max_segment_age_seconds
                            .max(1),
                        keep_last_n_segments_local: loaded
                            .config
                            .storage
                            .segments
                            .keep_last_n_segments_local,
                        compress_closed_segments: loaded
                            .config
                            .storage
                            .segments
                            .compress_closed_segments,
                        compression: normalize_compression(
                            &loaded.config.storage.segments.compression,
                        ),
                        upload_enabled: false,
                        verify_before_local_delete: loaded
                            .config
                            .storage
                            .segments
                            .verify_before_local_delete,
                        delete_verified_segments: loaded
                            .config
                            .storage
                            .segments
                            .delete_verified_segments,
                        upload_closed_segments_during_run: false,
                        verify_closed_segments: upload_config.verify_closed_segments,
                        delete_verified_closed_segments: upload_config
                            .delete_verified_closed_segments,
                        max_concurrent_segment_uploads: upload_config
                            .max_concurrent_segment_uploads
                            .max(1),
                        max_pending_segments: upload_config.max_pending_segments.max(1),
                        max_pending_segments_warning: upload_config
                            .max_pending_segments_warning
                            .max(1),
                        max_pending_segments_pause: upload_config.max_pending_segments_pause.max(1),
                        max_pending_segments_stop: upload_config.max_pending_segments_stop.max(1),
                        pause_collection_if_pending_segments_exceed: upload_config
                            .pause_collection_if_pending_segments_exceed,
                        pause_new_token_tracking_on_backlog: upload_config
                            .pause_new_token_tracking_on_backlog,
                        pause_feature_snapshot_writes_on_backlog: upload_config
                            .pause_feature_snapshot_writes_on_backlog,
                        force_close_and_upload_on_backlog: upload_config
                            .force_close_and_upload_on_backlog,
                        retry_failed_uploads: upload_config.retry_failed_uploads,
                        max_upload_retries: upload_config.max_upload_retries.max(1),
                        retry_backoff_seconds: upload_config.retry_backoff_seconds.max(1),
                        alert_on_upload_backlog: upload_config.alert_on_upload_backlog,
                        last_segment_upload_error: Some(format!(
                            "segment_uploader_unavailable:{error}"
                        )),
                        disk_warning_active: false,
                        last_disk_cleanup_action: None,
                        max_observed_upload_backlog: 0,
                        low_disk_mode_enabled: loaded.config.storage.low_disk_mode.enabled,
                        target_local_runtime_bytes: loaded
                            .config
                            .storage
                            .low_disk_mode
                            .target_local_runtime_mb
                            .saturating_mul(1024 * 1024),
                        max_local_verified_segments: loaded
                            .config
                            .storage
                            .low_disk_mode
                            .max_local_verified_segments
                            .max(1),
                        max_local_unverified_closed_segments: loaded
                            .config
                            .storage
                            .low_disk_mode
                            .max_local_unverified_closed_segments
                            .max(1),
                        keep_open_segments_only: loaded
                            .config
                            .storage
                            .low_disk_mode
                            .keep_open_segments_only,
                        delete_verified_segments_immediately: loaded
                            .config
                            .storage
                            .low_disk_mode
                            .delete_verified_segments_immediately,
                        delete_verified_exports_immediately: loaded
                            .config
                            .storage
                            .low_disk_mode
                            .delete_verified_exports_immediately,
                        verified_segment_bytes_pruned_total: 0,
                        verified_export_bytes_pruned_total: 0,
                        optional_exports_skipped_total: 0,
                        local_budget_pressure_active: false,
                        r2_client: None,
                    };
                    let _ = manager.write_all_reports();
                    return Ok(Some(manager));
                }
            }
        } else {
            None
        };
        let mut manager = Self {
            run_id: run_id.to_owned(),
            report_root,
            edge_mode: loaded.config.runtime.mode == RuntimeModeName::EdgeCollector,
            manifest,
            open_segments: BTreeMap::new(),
            next_sequence_by_type,
            retry_state: BTreeMap::new(),
            disk_actions: Vec::new(),
            max_segment_size_bytes: loaded
                .config
                .storage
                .segments
                .max_segment_size_mb
                .saturating_mul(1024 * 1024)
                .max(1),
            max_segment_age_seconds: loaded
                .config
                .storage
                .segments
                .max_segment_age_seconds
                .max(1),
            keep_last_n_segments_local: loaded.config.storage.segments.keep_last_n_segments_local,
            compress_closed_segments: loaded.config.storage.segments.compress_closed_segments,
            compression: normalize_compression(&loaded.config.storage.segments.compression),
            upload_enabled: upload_config.enabled
                && upload_config.upload_closed_segments_during_run
                && loaded.config.storage.segments.upload_closed_segments
                && loaded.config.r2.upload_enabled
                && !loaded.config.r2.dry_run
                && r2_client.is_some(),
            verify_before_local_delete: loaded.config.storage.segments.verify_before_local_delete,
            delete_verified_segments: loaded.config.storage.segments.delete_verified_segments,
            upload_closed_segments_during_run: upload_config.upload_closed_segments_during_run,
            verify_closed_segments: upload_config.verify_closed_segments,
            delete_verified_closed_segments: upload_config.delete_verified_closed_segments,
            max_concurrent_segment_uploads: upload_config.max_concurrent_segment_uploads.max(1),
            max_pending_segments: upload_config.max_pending_segments.max(1),
            max_pending_segments_warning: upload_config.max_pending_segments_warning.max(1),
            max_pending_segments_pause: upload_config.max_pending_segments_pause.max(1),
            max_pending_segments_stop: upload_config.max_pending_segments_stop.max(1),
            pause_collection_if_pending_segments_exceed: upload_config
                .pause_collection_if_pending_segments_exceed,
            pause_new_token_tracking_on_backlog: upload_config.pause_new_token_tracking_on_backlog,
            pause_feature_snapshot_writes_on_backlog: upload_config
                .pause_feature_snapshot_writes_on_backlog,
            force_close_and_upload_on_backlog: upload_config.force_close_and_upload_on_backlog,
            retry_failed_uploads: upload_config.retry_failed_uploads,
            max_upload_retries: upload_config.max_upload_retries.max(1),
            retry_backoff_seconds: upload_config.retry_backoff_seconds.max(1),
            alert_on_upload_backlog: upload_config.alert_on_upload_backlog,
            last_segment_upload_error: None,
            disk_warning_active: false,
            last_disk_cleanup_action: None,
            max_observed_upload_backlog: 0,
            low_disk_mode_enabled: loaded.config.storage.low_disk_mode.enabled,
            target_local_runtime_bytes: loaded
                .config
                .storage
                .low_disk_mode
                .target_local_runtime_mb
                .saturating_mul(1024 * 1024),
            max_local_verified_segments: loaded
                .config
                .storage
                .low_disk_mode
                .max_local_verified_segments
                .max(1),
            max_local_unverified_closed_segments: loaded
                .config
                .storage
                .low_disk_mode
                .max_local_unverified_closed_segments
                .max(1),
            keep_open_segments_only: loaded.config.storage.low_disk_mode.keep_open_segments_only,
            delete_verified_segments_immediately: loaded
                .config
                .storage
                .low_disk_mode
                .delete_verified_segments_immediately,
            delete_verified_exports_immediately: loaded
                .config
                .storage
                .low_disk_mode
                .delete_verified_exports_immediately,
            verified_segment_bytes_pruned_total: 0,
            verified_export_bytes_pruned_total: 0,
            optional_exports_skipped_total: 0,
            local_budget_pressure_active: false,
            r2_client,
        };
        manager.write_all_reports()?;
        Ok(Some(manager))
    }

    pub(crate) fn append_json_record<T: Serialize>(
        &mut self,
        artifact_type: &str,
        record: &T,
        event_time: Option<OffsetDateTime>,
    ) -> Result<()> {
        let mut line = serde_json::to_string(record)?;
        line.push('\n');
        self.append_line(artifact_type, line.as_bytes(), event_time)
    }

    pub(crate) fn append_feature_snapshot(
        &mut self,
        record: &StoredRecord<FeatureSnapshot>,
    ) -> Result<()> {
        let wrapped = SegmentTextFileRecord {
            relative_path: format!(
                "{}_{}.json",
                record.record.mint.0,
                record.record.observed_at.unix_timestamp_nanos()
            ),
            body: serde_json::to_string(record)?,
            source_path: None,
        };
        let mut line = serde_json::to_string(&wrapped)?;
        line.push('\n');
        self.append_line(
            "feature_snapshots",
            line.as_bytes(),
            Some(record.record.observed_at),
        )
    }

    pub(crate) fn append_text_snapshot(
        &mut self,
        artifact_type: &str,
        relative_path: &str,
        body: &str,
        event_time: OffsetDateTime,
    ) -> Result<()> {
        let wrapped = SegmentTextFileRecord {
            relative_path: relative_path.to_owned(),
            body: body.to_owned(),
            source_path: None,
        };
        let mut line = serde_json::to_string(&wrapped)?;
        line.push('\n');
        self.append_line(artifact_type, line.as_bytes(), Some(event_time))
    }

    pub(crate) fn close_segments_due_to_age(&mut self, now: OffsetDateTime) -> Result<usize> {
        let to_close = self
            .open_segments
            .iter()
            .filter(|(_, segment)| {
                (now - segment.created_at).whole_seconds().max(0) as u64
                    >= self.max_segment_age_seconds
            })
            .map(|(artifact_type, _)| artifact_type.clone())
            .collect::<Vec<_>>();
        let mut closed = 0usize;
        for artifact_type in to_close {
            if self.close_segment(&artifact_type)? {
                closed = closed.saturating_add(1);
            }
        }
        if closed > 0 {
            self.write_all_reports()?;
        }
        Ok(closed)
    }

    pub(crate) fn close_all_open_segments(&mut self) -> Result<usize> {
        let to_close = self.open_segments.keys().cloned().collect::<Vec<_>>();
        let mut closed = 0usize;
        for artifact_type in to_close {
            if self.close_segment(&artifact_type)? {
                closed = closed.saturating_add(1);
            }
        }
        if closed > 0 {
            self.write_all_reports()?;
        }
        Ok(closed)
    }

    pub(crate) fn pending_segment_count(&self) -> usize {
        self.manifest
            .segments
            .iter()
            .filter(|segment| {
                !segment.uploaded
                    && !segment.pruned_local
                    && Path::new(&segment.local_path).exists()
            })
            .count()
    }

    pub(crate) fn backlog_warning_threshold_reached(&self) -> bool {
        self.max_pending_segments_warning > 0
            && self.pending_segment_count() >= self.max_pending_segments_warning
    }

    pub(crate) fn backlog_exceeds_pause_threshold(&self) -> bool {
        self.pause_collection_if_pending_segments_exceed > 0
            && self.pending_segment_count() >= self.pause_collection_if_pending_segments_exceed
    }

    pub(crate) fn backlog_exceeds_stop_threshold(&self) -> bool {
        self.max_pending_segments_stop > 0
            && self.pending_segment_count() >= self.max_pending_segments_stop
    }

    pub(crate) fn should_pause_feature_snapshot_writes(&self) -> bool {
        self.local_budget_pressure_active
            || (self.pause_feature_snapshot_writes_on_backlog
                && self.max_pending_segments_pause > 0
                && self.pending_segment_count() >= self.max_pending_segments_pause)
    }

    pub(crate) fn should_pause_new_token_tracking(&self) -> bool {
        self.local_budget_pressure_active
            || (self.pause_new_token_tracking_on_backlog
                && self.max_pending_segments_pause > 0
                && self.pending_segment_count() >= self.max_pending_segments_pause)
    }

    pub(crate) fn should_force_close_and_upload_on_backlog(&self) -> bool {
        self.force_close_and_upload_on_backlog
            && self.max_pending_segments_warning > 0
            && self.pending_segment_count() >= self.max_pending_segments_warning
    }

    pub(crate) fn is_disk_warning_active(&self) -> bool {
        self.disk_warning_active
    }

    pub(crate) fn status_summary(&self) -> SegmentStatusSummary {
        let closed_pending_upload_count = self
            .manifest
            .segments
            .iter()
            .filter(|segment| !segment.uploaded && !segment.pruned_local)
            .count();
        SegmentStatusSummary {
            run_id: self.run_id.clone(),
            updated_at: Some(OffsetDateTime::now_utc()),
            active_segment_count: self.open_segments.len(),
            closed_pending_upload_count,
            segment_upload_backlog: closed_pending_upload_count,
            verified_pruned_segment_count: self
                .manifest
                .segments
                .iter()
                .filter(|segment| segment.verified && segment.pruned_local)
                .count(),
            local_segment_bytes: self
                .manifest
                .segments
                .iter()
                .filter(|segment| !segment.pruned_local)
                .map(|segment| segment.size_bytes)
                .sum::<u64>()
                .saturating_add(
                    self.open_segments
                        .values()
                        .map(|segment| segment.size_bytes)
                        .sum::<u64>(),
                ),
            remote_verified_segment_bytes: self
                .manifest
                .segments
                .iter()
                .filter(|segment| segment.verified)
                .map(|segment| segment.size_bytes)
                .sum(),
            last_segment_upload_error: self.last_segment_upload_error.clone(),
            disk_warning_active: self.disk_warning_active,
            last_disk_cleanup_action: self.last_disk_cleanup_action.clone(),
            upload_backlog_max: self.max_observed_upload_backlog,
        }
    }

    pub(crate) fn record_disk_action(
        &mut self,
        level: &str,
        action: &str,
        free_mb: u64,
        pruned_bytes: u64,
        notes: Vec<String>,
    ) -> Result<()> {
        self.last_disk_cleanup_action = Some(action.to_owned());
        self.disk_actions.push(DiskActionRecord {
            at: OffsetDateTime::now_utc(),
            level: level.to_owned(),
            action: action.to_owned(),
            free_mb,
            pending_segments: self.pending_segment_count(),
            pruned_bytes,
            notes,
        });
        if self.disk_actions.len() > 50 {
            let drain = self.disk_actions.len().saturating_sub(50);
            self.disk_actions.drain(0..drain);
        }
        self.write_all_reports()
    }

    pub(crate) fn set_disk_warning_active(&mut self, active: bool) -> Result<()> {
        self.disk_warning_active = active;
        self.write_all_reports()
    }

    pub(crate) fn local_footprint_summary(
        &self,
        free_mb: u64,
    ) -> Result<LocalFootprintBudgetSummary> {
        let active_open_segment_bytes = self
            .open_segments
            .values()
            .map(|segment| segment.size_bytes)
            .sum::<u64>();
        let closed_unverified_segment_bytes = self
            .manifest
            .segments
            .iter()
            .filter(|segment| !segment.verified && !segment.pruned_local)
            .map(|segment| segment.size_bytes)
            .sum::<u64>();
        let closed_verified_local_segment_bytes = self
            .manifest
            .segments
            .iter()
            .filter(|segment| segment.verified && !segment.pruned_local)
            .map(|segment| segment.size_bytes)
            .sum::<u64>();
        let export_manifest = self.load_export_chunk_manifest();
        let optional_exports_skipped_total = export_manifest
            .as_ref()
            .map(|manifest| manifest.skipped_exports.len() as u64)
            .unwrap_or_default();
        let report_files = collect_report_files(&self.report_root)?;
        let mut export_bytes = 0u64;
        let mut report_bytes = 0u64;
        let mut restore_cache_bytes = 0u64;
        let mut temp_bytes = 0u64;
        let mut manifests_summaries_bytes = 0u64;
        for path in report_files {
            let size = fs::metadata(&path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            let relative = path
                .strip_prefix(&self.report_root)
                .ok()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_owned();
            if name.ends_with(".open") {
                continue;
            }
            if relative.contains("tmp_restore") {
                restore_cache_bytes = restore_cache_bytes.saturating_add(size);
                continue;
            }
            if name.ends_with(".tmp") || name.starts_with('.') {
                temp_bytes = temp_bytes.saturating_add(size);
                continue;
            }
            if name.starts_with("segment_") {
                continue;
            }
            if is_export_file_name(name) {
                export_bytes = export_bytes.saturating_add(size);
                continue;
            }
            if is_manifest_or_summary_file_name(name) {
                manifests_summaries_bytes = manifests_summaries_bytes.saturating_add(size);
                continue;
            }
            report_bytes = report_bytes.saturating_add(size);
        }
        let total_local_runtime_bytes = active_open_segment_bytes
            .saturating_add(closed_unverified_segment_bytes)
            .saturating_add(closed_verified_local_segment_bytes)
            .saturating_add(export_bytes)
            .saturating_add(report_bytes)
            .saturating_add(restore_cache_bytes)
            .saturating_add(temp_bytes)
            .saturating_add(manifests_summaries_bytes);
        let local_runtime_budget_bytes = if self.low_disk_mode_enabled {
            self.target_local_runtime_bytes
        } else {
            0
        };
        let local_runtime_budget_overage_bytes = if self.low_disk_mode_enabled
            && total_local_runtime_bytes > local_runtime_budget_bytes
        {
            total_local_runtime_bytes.saturating_sub(local_runtime_budget_bytes)
        } else {
            0
        };
        Ok(LocalFootprintBudgetSummary {
            run_id: self.run_id.clone(),
            updated_at: Some(OffsetDateTime::now_utc()),
            low_disk_mode_enabled: self.low_disk_mode_enabled,
            active_open_segment_bytes,
            closed_unverified_segment_bytes,
            closed_verified_local_segment_bytes,
            export_bytes,
            report_bytes,
            restore_cache_bytes,
            temp_bytes,
            manifests_summaries_bytes,
            total_local_runtime_bytes,
            local_runtime_budget_bytes,
            local_runtime_budget_overage_bytes,
            free_mb,
            verified_segment_bytes_pruned_total: self.verified_segment_bytes_pruned_total,
            verified_export_bytes_pruned_total: self.verified_export_bytes_pruned_total,
            optional_exports_skipped_total,
        })
    }

    pub(crate) fn enforce_local_footprint_budget(
        &mut self,
        free_mb: u64,
    ) -> Result<LocalFootprintBudgetSummary> {
        if self.delete_verified_segments
            && self.verify_before_local_delete
            && self.delete_verified_closed_segments
            && self.delete_verified_segments_immediately
        {
            let pruned = self.prune_verified_closed_segments()?;
            self.verified_segment_bytes_pruned_total = self
                .verified_segment_bytes_pruned_total
                .saturating_add(pruned);
        }
        if self.low_disk_mode_enabled && self.delete_verified_exports_immediately {
            let pruned = self.prune_verified_export_chunks()?;
            self.verified_export_bytes_pruned_total = self
                .verified_export_bytes_pruned_total
                .saturating_add(pruned);
        }
        let summary = self.local_footprint_summary(free_mb)?;
        let unverified_closed_segments = self
            .manifest
            .segments
            .iter()
            .filter(|segment| !segment.verified && !segment.pruned_local)
            .count();
        self.local_budget_pressure_active = self.low_disk_mode_enabled
            && (summary.local_runtime_budget_overage_bytes > 0
                || unverified_closed_segments > self.max_local_unverified_closed_segments);
        self.write_all_reports()?;
        Ok(summary)
    }

    fn load_export_chunk_manifest(&self) -> Option<RuntimeExportChunkManifest> {
        let path = self.report_root.join("export_chunks_manifest.json");
        if !path.exists() {
            return None;
        }
        serde_json::from_slice(&fs::read(path).ok()?).ok()
    }

    fn prune_verified_export_chunks(&mut self) -> Result<u64> {
        let path = self.report_root.join("export_chunks_manifest.json");
        if !path.exists() {
            return Ok(0);
        }
        let bytes = fs::read(&path)?;
        let mut manifest = serde_json::from_slice::<RuntimeExportChunkManifest>(&bytes)?;
        let mut deleted_bytes = 0u64;
        for chunk in &mut manifest.chunks {
            if !chunk.verified || chunk.pruned_local {
                continue;
            }
            let local_path = PathBuf::from(&chunk.local_path);
            if local_path.exists() {
                let size = fs::metadata(&local_path)
                    .map(|metadata| metadata.len())
                    .unwrap_or(chunk.size_bytes);
                fs::remove_file(&local_path)?;
                deleted_bytes = deleted_bytes.saturating_add(size);
            }
            chunk.pruned_local = true;
        }
        if !manifest.chunks.is_empty() && manifest.chunks.iter().all(|chunk| chunk.verified) {
            manifest.export_status = "chunked_remote_verified".to_owned();
        }
        atomic_write_json(&path, &manifest)?;
        Ok(deleted_bytes)
    }

    pub(crate) async fn process_background_uploads(&mut self) -> Result<SegmentProcessSummary> {
        if let Some(error) = self.backlog_warning_message() {
            self.last_segment_upload_error = Some(error);
        }
        let free_mb = filesystem_free_mb(&self.report_root).unwrap_or(u64::MAX);
        let mut summary = SegmentProcessSummary {
            uploaded_count: 0,
            verified_count: 0,
            pruned_bytes: 0,
            pending_count: self.pending_segment_count(),
            last_error: self.last_segment_upload_error.clone(),
        };
        if !self.upload_enabled || !self.upload_closed_segments_during_run {
            self.write_all_reports()?;
            return Ok(summary);
        }
        self.max_observed_upload_backlog =
            self.max_observed_upload_backlog.max(summary.pending_count);
        let candidate_indexes = self
            .manifest
            .segments
            .iter()
            .enumerate()
            .filter(|(_, segment)| {
                !segment.uploaded
                    && !segment.pruned_local
                    && Path::new(&segment.local_path).exists()
            })
            .map(|(idx, _)| idx)
            .take(self.max_concurrent_segment_uploads)
            .collect::<Vec<_>>();
        for idx in candidate_indexes {
            if self.try_upload_segment(idx).await? {
                summary.uploaded_count = summary.uploaded_count.saturating_add(1);
                if self.manifest.segments[idx].verified {
                    summary.verified_count = summary.verified_count.saturating_add(1);
                }
            }
        }
        let before_segment_pruned = self.verified_segment_bytes_pruned_total;
        let before_export_pruned = self.verified_export_bytes_pruned_total;
        let _ = self.enforce_local_footprint_budget(free_mb)?;
        summary.pruned_bytes = self
            .verified_segment_bytes_pruned_total
            .saturating_sub(before_segment_pruned)
            .saturating_add(
                self.verified_export_bytes_pruned_total
                    .saturating_sub(before_export_pruned),
            );
        let _ = self.upload_segment_manifest().await;
        self.write_all_reports()?;
        summary.pending_count = self.pending_segment_count();
        summary.last_error = self.last_segment_upload_error.clone();
        Ok(summary)
    }

    fn append_line(
        &mut self,
        artifact_type: &str,
        bytes: &[u8],
        event_time: Option<OffsetDateTime>,
    ) -> Result<()> {
        let max_segment_size_bytes = self.max_segment_size_bytes;
        let should_close = {
            let segment = self.open_segment_mut(artifact_type)?;
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&segment.open_path)?;
            file.write_all(bytes)?;
            segment.updated_at = event_time.unwrap_or_else(OffsetDateTime::now_utc);
            segment.size_bytes = segment.size_bytes.saturating_add(bytes.len() as u64);
            segment.record_count = segment.record_count.saturating_add(1);
            if segment.first_event_time.is_none() {
                segment.first_event_time = event_time;
            }
            if event_time.is_some() {
                segment.last_event_time = event_time;
            }
            segment.size_bytes >= max_segment_size_bytes
        };
        if should_close {
            let _ = self.close_segment(artifact_type)?;
        }
        Ok(())
    }

    fn open_segment_mut(&mut self, artifact_type: &str) -> Result<&mut OpenSegment> {
        if !self.open_segments.contains_key(artifact_type) {
            let sequence = self
                .next_sequence_by_type
                .entry(artifact_type.to_owned())
                .or_insert(1usize);
            let file_name = if artifact_type.starts_with("edge_") {
                format!("{artifact_type}_{:06}.jsonl.open", *sequence)
            } else {
                format!("segment_{artifact_type}_{:05}.jsonl.open", *sequence)
            };
            let path = self.report_root.join(file_name);
            let now = OffsetDateTime::now_utc();
            self.open_segments.insert(
                artifact_type.to_owned(),
                OpenSegment {
                    artifact_type: artifact_type.to_owned(),
                    sequence_number: *sequence,
                    open_path: path,
                    created_at: now,
                    updated_at: now,
                    size_bytes: 0,
                    record_count: 0,
                    first_event_time: None,
                    last_event_time: None,
                },
            );
            *sequence = sequence.saturating_add(1);
        }
        Ok(self
            .open_segments
            .get_mut(artifact_type)
            .expect("open segment inserted"))
    }

    fn close_segment(&mut self, artifact_type: &str) -> Result<bool> {
        let Some(open) = self.open_segments.remove(artifact_type) else {
            return Ok(false);
        };
        if open.record_count == 0 {
            let _ = fs::remove_file(&open.open_path);
            return Ok(false);
        }
        let raw = fs::read(&open.open_path)?;
        let compressed =
            self.compress_closed_segments && self.compression.as_deref() == Some("zstd");
        let final_bytes = if compressed {
            zstd::stream::encode_all(&raw[..], 3)?
        } else {
            raw
        };
        let final_name =
            segment_file_name(artifact_type, open.sequence_number, compressed, "jsonl");
        let final_path = self.report_root.join(&final_name);
        let prewrite_report = SegmentIntegrityValidator::validate_bytes(
            &final_bytes,
            SegmentIntegrityValidationOptions {
                artifact_type: artifact_type.to_owned(),
                sequence_number: open.sequence_number,
                expected_record_count: open.record_count,
                compression: compressed.then(|| "zstd".to_owned()),
                explicitly_empty: false,
            },
        );
        if !prewrite_report.valid {
            let message = format!(
                "segment_integrity_failed_before_write:{}",
                prewrite_report.errors.join(",")
            );
            self.last_segment_upload_error = Some(message.clone());
            self.disk_actions.push(DiskActionRecord {
                at: OffsetDateTime::now_utc(),
                level: "error".to_owned(),
                action: message,
                free_mb: filesystem_free_mb(&self.report_root).unwrap_or_default(),
                pending_segments: self.pending_segment_count(),
                pruned_bytes: 0,
                notes: prewrite_report.errors.clone(),
            });
            self.open_segments.insert(artifact_type.to_owned(), open);
            self.write_all_reports()?;
            return Ok(false);
        }
        atomic_write_bytes(&final_path, &final_bytes)?;
        let final_report = SegmentIntegrityValidator::validate_file(
            &final_path,
            SegmentIntegrityValidationOptions {
                artifact_type: artifact_type.to_owned(),
                sequence_number: open.sequence_number,
                expected_record_count: open.record_count,
                compression: compressed.then(|| "zstd".to_owned()),
                explicitly_empty: false,
            },
        )?;
        if !final_report.valid {
            let message = format!(
                "segment_integrity_failed_after_write:{}",
                final_report.errors.join(",")
            );
            self.last_segment_upload_error = Some(message.clone());
            self.disk_actions.push(DiskActionRecord {
                at: OffsetDateTime::now_utc(),
                level: "error".to_owned(),
                action: message,
                free_mb: filesystem_free_mb(&self.report_root).unwrap_or_default(),
                pending_segments: self.pending_segment_count(),
                pruned_bytes: 0,
                notes: final_report.errors.clone(),
            });
            self.open_segments.insert(artifact_type.to_owned(), open);
            self.write_all_reports()?;
            return Ok(false);
        }
        let final_bytes = fs::read(&final_path)?;
        let _ = fs::remove_file(&open.open_path);
        let segment_id = format!(
            "{}:{}:{:05}",
            self.run_id, artifact_type, open.sequence_number
        );
        let checksum_sha256 = format!("{:x}", Sha256::digest(&final_bytes));
        self.manifest
            .segments
            .retain(|segment| segment.segment_id != segment_id);
        self.manifest.segments.push(RunSegmentEntry {
            segment_id,
            artifact_type: artifact_type.to_owned(),
            run_id: self.run_id.clone(),
            sequence_number: open.sequence_number,
            local_path: final_path.display().to_string(),
            source_paths: Vec::new(),
            remote_key: String::new(),
            size_bytes: final_bytes.len() as u64,
            checksum_sha256,
            record_count: open.record_count,
            first_event_time: open.first_event_time,
            last_event_time: open.last_event_time.or(Some(open.updated_at)),
            compression: compressed.then(|| "zstd".to_owned()),
            uploaded: false,
            verified: false,
            pruned_local: false,
            error: None,
        });
        self.write_all_reports()?;
        Ok(true)
    }

    async fn try_upload_segment(&mut self, idx: usize) -> Result<bool> {
        let Some(client) = self.r2_client.clone() else {
            return Ok(false);
        };
        let retry_state = self
            .retry_state
            .entry(self.manifest.segments[idx].segment_id.clone())
            .or_default()
            .clone();
        if !self.retry_failed_uploads && self.manifest.segments[idx].error.is_some() {
            return Ok(false);
        }
        if self.manifest.segments[idx]
            .error
            .as_deref()
            .map(|error| error.starts_with("segment_integrity_failed:"))
            .unwrap_or(false)
        {
            return Ok(false);
        }
        if retry_state.attempts >= self.max_upload_retries {
            return Ok(false);
        }
        if let Some(last_attempt) = retry_state.last_attempt_at {
            let since = (OffsetDateTime::now_utc() - last_attempt)
                .whole_seconds()
                .max(0) as u64;
            if since < self.retry_backoff_seconds {
                return Ok(false);
            }
        }
        let segment = self.manifest.segments[idx].clone();
        let local_path = PathBuf::from(&segment.local_path);
        if !local_path.exists() {
            return Ok(false);
        }
        let integrity = SegmentIntegrityValidator::validate_file(
            &local_path,
            SegmentIntegrityValidationOptions {
                artifact_type: segment.artifact_type.clone(),
                sequence_number: segment.sequence_number,
                expected_record_count: segment.record_count,
                compression: segment.compression.clone(),
                explicitly_empty: false,
            },
        )?;
        if !integrity.valid {
            let entry = &mut self.manifest.segments[idx];
            entry.error = Some(format!(
                "segment_integrity_failed_before_upload:{}",
                integrity.errors.join(",")
            ));
            self.last_segment_upload_error = entry.error.clone();
            return Ok(false);
        }
        let bucket = client.bucket_for_datasets()?;
        let file_name = local_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("segment.jsonl");
        let remote_key = client.managed_key(
            &client.config().paths.runs_prefix,
            &format!("{}/segments/{}", self.run_id, file_name),
        );
        let mut metadata = BTreeMap::new();
        metadata.insert("run_id".to_owned(), self.run_id.clone());
        metadata.insert("segment_id".to_owned(), segment.segment_id.clone());
        metadata.insert("artifact_type".to_owned(), segment.artifact_type.clone());
        metadata.insert(
            "sequence_number".to_owned(),
            segment.sequence_number.to_string(),
        );
        let prepared = client.prepare_upload(
            &local_path,
            bucket.clone(),
            remote_key.clone(),
            content_type_for_segment(&segment),
            metadata,
            Some(false),
        )?;
        match client
            .upload_prepared(&prepared, Some(self.verify_closed_segments))
            .await
        {
            Ok(result) => {
                let entry = &mut self.manifest.segments[idx];
                entry.remote_key = remote_key;
                entry.uploaded = result.uploaded;
                entry.verified = result.verified;
                entry.size_bytes = result.size_bytes;
                entry.checksum_sha256 = result.checksum_sha256;
                entry.error = result.error;
                self.retry_state.remove(&entry.segment_id);
                self.last_segment_upload_error = None;
                Ok(result.uploaded)
            }
            Err(error) => {
                let entry = &mut self.manifest.segments[idx];
                entry.error = Some(error.to_string());
                self.last_segment_upload_error = Some(error.to_string());
                let retry = self
                    .retry_state
                    .entry(entry.segment_id.clone())
                    .or_default();
                retry.attempts = retry.attempts.saturating_add(1);
                retry.last_attempt_at = Some(OffsetDateTime::now_utc());
                Ok(false)
            }
        }
    }

    async fn upload_segment_manifest(&mut self) -> Result<()> {
        let Some(client) = self.r2_client.clone() else {
            return Ok(());
        };
        let path = self.manifest_path();
        if !path.exists() {
            return Ok(());
        }
        let bucket = client.bucket_for_reports()?;
        let remote_key = client.managed_key(
            &client.config().paths.manifests_prefix,
            &format!("segments/{}/segment_manifest.json", self.run_id),
        );
        let prepared = client.prepare_upload(
            &path,
            bucket,
            remote_key,
            "application/json",
            BTreeMap::from([("run_id".to_owned(), self.run_id.clone())]),
            Some(false),
        )?;
        match client
            .upload_prepared(&prepared, Some(self.verify_closed_segments))
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                self.last_segment_upload_error = Some(error.to_string());
                Ok(())
            }
        }
    }

    fn prune_verified_closed_segments(&mut self) -> Result<u64> {
        let grouped = self
            .manifest
            .segments
            .iter()
            .enumerate()
            .filter(|(_, segment)| segment.verified)
            .fold(
                BTreeMap::<String, Vec<usize>>::new(),
                |mut groups, (idx, segment)| {
                    groups
                        .entry(segment.artifact_type.clone())
                        .or_default()
                        .push(idx);
                    groups
                },
            );
        let mut deleted_bytes = 0u64;
        for (_, mut indexes) in grouped {
            indexes.sort_by_key(|idx| self.manifest.segments[*idx].sequence_number);
            let keep_last_n = if self.low_disk_mode_enabled {
                self.max_local_verified_segments.max(1)
            } else {
                self.keep_last_n_segments_local.max(1)
            };
            let preserve = indexes
                .iter()
                .rev()
                .take(keep_last_n)
                .copied()
                .collect::<BTreeSet<_>>();
            for idx in indexes {
                if preserve.contains(&idx) {
                    continue;
                }
                let entry = &mut self.manifest.segments[idx];
                if entry.pruned_local {
                    continue;
                }
                let path = PathBuf::from(&entry.local_path);
                if path.exists() {
                    let size = fs::metadata(&path)
                        .map(|metadata| metadata.len())
                        .unwrap_or(0);
                    fs::remove_file(&path)?;
                    deleted_bytes = deleted_bytes.saturating_add(size);
                }
                entry.pruned_local = true;
            }
        }
        Ok(deleted_bytes)
    }

    fn backlog_warning_message(&self) -> Option<String> {
        let pending = self.pending_segment_count();
        if self.alert_on_upload_backlog && pending >= self.max_pending_segments_warning {
            Some(format!(
                "segment_upload_backlog_high: pending_segments={} warning_threshold={}",
                pending, self.max_pending_segments_warning
            ))
        } else {
            None
        }
    }

    fn manifest_path(&self) -> PathBuf {
        self.report_root.join("segment_manifest.json")
    }

    fn status_json_path(&self) -> PathBuf {
        self.report_root.join("segment_status.json")
    }

    fn edge_manifest_path(&self) -> PathBuf {
        self.report_root.join("edge_segment_manifest.json")
    }

    fn status_markdown_path(&self) -> PathBuf {
        self.report_root.join("segment_status.md")
    }

    fn disk_actions_json_path(&self) -> PathBuf {
        self.report_root.join("disk_actions.json")
    }

    fn disk_actions_markdown_path(&self) -> PathBuf {
        self.report_root.join("disk_actions.md")
    }

    fn local_footprint_json_path(&self) -> PathBuf {
        self.report_root.join("local_footprint_budget.json")
    }

    fn local_footprint_markdown_path(&self) -> PathBuf {
        self.report_root.join("local_footprint_budget.md")
    }

    fn write_all_reports(&mut self) -> Result<()> {
        self.manifest.updated_at = OffsetDateTime::now_utc();
        refresh_manifest_summary(&mut self.manifest);
        atomic_write_json(&self.manifest_path(), &self.manifest)?;
        if self.edge_mode {
            atomic_write_json(&self.edge_manifest_path(), &self.manifest)?;
        }
        let status = self.status_summary();
        atomic_write_json(&self.status_json_path(), &status)?;
        fs::write(
            self.status_markdown_path(),
            render_segment_status_markdown(&status),
        )?;
        let report = DiskActionReport {
            run_id: self.run_id.clone(),
            updated_at: Some(OffsetDateTime::now_utc()),
            actions: self.disk_actions.clone(),
        };
        atomic_write_json(&self.disk_actions_json_path(), &report)?;
        fs::write(
            self.disk_actions_markdown_path(),
            render_disk_actions_markdown(&report),
        )?;
        let free_mb = filesystem_free_mb(&self.report_root).unwrap_or(u64::MAX);
        let footprint = self.local_footprint_summary(free_mb)?;
        atomic_write_json(&self.local_footprint_json_path(), &footprint)?;
        fs::write(
            self.local_footprint_markdown_path(),
            render_local_footprint_markdown(&footprint),
        )?;
        Ok(())
    }
}

fn content_type_for_segment(segment: &RunSegmentEntry) -> String {
    if segment
        .compression
        .as_deref()
        .map(|value| value.eq_ignore_ascii_case("zstd"))
        .unwrap_or(false)
    {
        "application/zstd".to_owned()
    } else if matches!(segment.artifact_type.as_str(), "data_gaps") {
        "application/json".to_owned()
    } else {
        "application/x-ndjson".to_owned()
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

fn segment_file_name(
    artifact_type: &str,
    sequence_number: usize,
    compressed: bool,
    extension: &str,
) -> String {
    let prefix = if artifact_type.starts_with("edge_") {
        format!("{artifact_type}_{sequence_number:06}")
    } else {
        format!("segment_{artifact_type}_{sequence_number:05}")
    };
    if compressed {
        format!("{prefix}.{extension}.zst")
    } else {
        format!("{prefix}.{extension}")
    }
}

fn validate_segment_integrity(
    artifact_type: &str,
    final_bytes: &[u8],
    compressed: bool,
    expected_record_count: u64,
) -> Result<()> {
    if final_bytes.is_empty() {
        return Err(anyhow!("empty_segment"));
    }
    let decoded = if compressed {
        zstd::stream::decode_all(final_bytes)
            .with_context(|| format!("zstd_decode_failed:{artifact_type}"))?
    } else {
        final_bytes.to_vec()
    };
    if decoded.is_empty() {
        return Err(anyhow!("decoded_segment_empty"));
    }
    if !decoded.ends_with(b"\n") {
        return Err(anyhow!("missing_final_newline"));
    }
    let mut decoded_record_count = 0u64;
    for line in decoded.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        serde_json::from_slice::<serde_json::Value>(line)
            .with_context(|| format!("jsonl_parse_failed:{artifact_type}"))?;
        decoded_record_count = decoded_record_count.saturating_add(1);
    }
    if decoded_record_count == 0 {
        return Err(anyhow!("decoded_record_count_zero"));
    }
    if expected_record_count > 0 && decoded_record_count != expected_record_count {
        return Err(anyhow!(
            "record_count_mismatch:expected={expected_record_count}:decoded={decoded_record_count}"
        ));
    }
    Ok(())
}

fn refresh_manifest_summary(manifest: &mut RunSegmentManifest) {
    manifest.summary = RunSegmentManifestSummary {
        total_segments: manifest.segments.len(),
        uploaded_segments: manifest
            .segments
            .iter()
            .filter(|segment| segment.uploaded)
            .count(),
        verified_segments: manifest
            .segments
            .iter()
            .filter(|segment| segment.verified)
            .count(),
        pruned_segments: manifest
            .segments
            .iter()
            .filter(|segment| segment.pruned_local)
            .count(),
        local_bytes_remaining: manifest
            .segments
            .iter()
            .filter(|segment| !segment.pruned_local)
            .map(|segment| segment.size_bytes)
            .sum(),
        remote_bytes_verified: manifest
            .segments
            .iter()
            .filter(|segment| segment.verified)
            .map(|segment| segment.size_bytes)
            .sum(),
        failed_segments: manifest
            .segments
            .iter()
            .filter(|segment| segment.error.is_some())
            .count(),
    };
}

fn load_segment_manifest(report_root: &Path) -> Result<Option<RunSegmentManifest>> {
    let path = report_root.join("segment_manifest.json");
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_slice(&fs::read(path)?)?))
}

fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    atomic_write_bytes(path, &serde_json::to_vec_pretty(value)?)
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp_name = format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("segment"),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    );
    let temp_path = path.with_file_name(temp_name);
    {
        let mut file = File::create(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&temp_path, path)?;
    Ok(())
}

fn render_segment_status_markdown(summary: &SegmentStatusSummary) -> String {
    format!(
        "# Segment Status\n\n- run_id: {}\n- active_segment_count: {}\n- closed_pending_upload_count: {}\n- segment_upload_backlog: {}\n- verified_pruned_segment_count: {}\n- local_segment_bytes: {}\n- remote_verified_segment_bytes: {}\n- last_segment_upload_error: {}\n- disk_warning_active: {}\n- last_disk_cleanup_action: {}\n",
        summary.run_id,
        summary.active_segment_count,
        summary.closed_pending_upload_count,
        summary.segment_upload_backlog,
        summary.verified_pruned_segment_count,
        summary.local_segment_bytes,
        summary.remote_verified_segment_bytes,
        summary
            .last_segment_upload_error
            .clone()
            .unwrap_or_default(),
        summary.disk_warning_active,
        summary.last_disk_cleanup_action.clone().unwrap_or_default(),
    )
}

fn render_disk_actions_markdown(report: &DiskActionReport) -> String {
    let mut body = format!("# Disk Actions\n\n- run_id: {}\n", report.run_id);
    for action in &report.actions {
        body.push_str(&format!(
            "\n## {}\n- level: {}\n- action: {}\n- free_mb: {}\n- pending_segments: {}\n- pruned_bytes: {}\n- notes: {:?}\n",
            action.at,
            action.level,
            action.action,
            action.free_mb,
            action.pending_segments,
            action.pruned_bytes,
            action.notes,
        ));
    }
    body
}

fn render_local_footprint_markdown(summary: &LocalFootprintBudgetSummary) -> String {
    format!(
        "# Local Footprint Budget\n\n- run_id: {}\n- low_disk_mode_enabled: {}\n- active_open_segment_bytes: {}\n- closed_unverified_segment_bytes: {}\n- closed_verified_local_segment_bytes: {}\n- export_bytes: {}\n- report_bytes: {}\n- restore_cache_bytes: {}\n- temp_bytes: {}\n- manifests_summaries_bytes: {}\n- total_local_runtime_bytes: {}\n- local_runtime_budget_bytes: {}\n- local_runtime_budget_overage_bytes: {}\n- free_mb: {}\n- verified_segment_bytes_pruned_total: {}\n- verified_export_bytes_pruned_total: {}\n- optional_exports_skipped_total: {}\n",
        summary.run_id,
        summary.low_disk_mode_enabled,
        summary.active_open_segment_bytes,
        summary.closed_unverified_segment_bytes,
        summary.closed_verified_local_segment_bytes,
        summary.export_bytes,
        summary.report_bytes,
        summary.restore_cache_bytes,
        summary.temp_bytes,
        summary.manifests_summaries_bytes,
        summary.total_local_runtime_bytes,
        summary.local_runtime_budget_bytes,
        summary.local_runtime_budget_overage_bytes,
        summary.free_mb,
        summary.verified_segment_bytes_pruned_total,
        summary.verified_export_bytes_pruned_total,
        summary.optional_exports_skipped_total,
    )
}

fn collect_report_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
            } else if entry_path.is_file() {
                files.push(entry_path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn is_export_file_name(name: &str) -> bool {
    name.ends_with(".csv")
        || name.ends_with(".csv.zst")
        || name.starts_with("features_")
        || name.starts_with("decisions_")
        || name.starts_with("fills_")
        || name.starts_with("data_gaps_")
        || name.starts_with("rejection_reasons_")
        || name.starts_with("run_summary_")
}

fn is_manifest_or_summary_file_name(name: &str) -> bool {
    matches!(
        name,
        "artifact_manifest.json"
            | "segment_manifest.json"
            | "export_chunks_manifest.json"
            | "segment_status.json"
            | "segment_status.md"
            | "disk_actions.json"
            | "disk_actions.md"
            | "local_footprint_budget.json"
            | "local_footprint_budget.md"
            | "r2_upload_summary.json"
            | "r2_upload_summary.md"
            | "r2_upload_audit.json"
            | "r2_upload_audit.md"
            | "rpc_ledger.csv"
            | "rpc_ledger.json"
            | "stream_only_audit.md"
            | "stream_only_audit.json"
            | "live_collection_quality.md"
            | "live_collection_quality.json"
            | "backtest_readiness.md"
            | "backtest_readiness.json"
            | "run_summary.csv"
            | "data_gaps.csv"
            | "rejection_reasons.csv"
            | "run_summary.md"
            | "run_summary.json"
            | "collection_summary.md"
            | "collection_summary.json"
            | "metadata_backfill.md"
            | "metadata_backfill.json"
            | "finalize_run_recovery.md"
            | "finalize_run_recovery.json"
    )
}

fn filesystem_free_mb(path: &Path) -> Result<u64> {
    let canonical = if path.exists() {
        path.to_path_buf()
    } else {
        path.parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    };
    let stats = rustix::fs::statvfs(&canonical)?;
    Ok(stats
        .f_bavail
        .saturating_mul(stats.f_frsize)
        .saturating_div(1024 * 1024))
}

pub(crate) fn load_feature_snapshot_segment_records(
    report_root: &Path,
    manifest: &RunSegmentManifest,
    allow_remote_segment_lookup: bool,
) -> Result<Vec<StoredRecord<FeatureSnapshot>>> {
    let mut records = Vec::new();
    for segment in manifest
        .segments
        .iter()
        .filter(|segment| segment.artifact_type == "feature_snapshots")
    {
        let path = PathBuf::from(&segment.local_path);
        if !path.exists() && !allow_remote_segment_lookup {
            continue;
        }
        if !path.exists() {
            continue;
        }
        let bytes = fs::read(&path)?;
        let raw = if segment
            .compression
            .as_deref()
            .map(|value| value.eq_ignore_ascii_case("zstd"))
            .unwrap_or(false)
        {
            zstd::stream::decode_all(&bytes[..])?
        } else {
            bytes
        };
        for line in raw
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            if let Ok(record) = serde_json::from_slice::<StoredRecord<FeatureSnapshot>>(line) {
                records.push(record);
                continue;
            }
            if let Ok(wrapper) = serde_json::from_slice::<SegmentTextFileRecord>(line) {
                if let Ok(record) =
                    serde_json::from_str::<StoredRecord<FeatureSnapshot>>(&wrapper.body)
                {
                    records.push(record);
                }
            }
        }
    }
    let _ = report_root;
    records.sort_by(|left, right| left.record.observed_at.cmp(&right.record.observed_at));
    Ok(records)
}

pub(crate) fn dedupe_feature_snapshots(
    records: Vec<StoredRecord<FeatureSnapshot>>,
) -> Vec<StoredRecord<FeatureSnapshot>> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::new();
    for record in records {
        let key = (
            record.run_id.clone(),
            record.scenario_id.clone(),
            record.record.mint.0.clone(),
            record.record.vector_hash.clone(),
            record.record.observed_at,
        );
        if seen.insert(key) {
            unique.push(record);
        }
    }
    unique
}

pub(crate) fn restore_segment_file_from_remote(
    manifest: &RunSegmentManifest,
    segment_id: &str,
    report_root: &Path,
    client: &R2Client,
) -> Result<Option<PathBuf>> {
    let Some(segment) = manifest
        .segments
        .iter()
        .find(|segment| segment.segment_id == segment_id)
    else {
        return Ok(None);
    };
    if !segment.verified || segment.remote_key.trim().is_empty() {
        return Ok(None);
    }
    let local_path = PathBuf::from(&segment.local_path);
    let file_name = local_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("segment.restore");
    let restore_root = Path::new(report_root).join("tmp_restore_segments");
    fs::create_dir_all(&restore_root)?;
    let restore_path = restore_root.join(file_name);
    let bucket = client.bucket_for_datasets()?;
    let bytes = futures::executor::block_on(client.download_object(&bucket, &segment.remote_key))?;
    let checksum = format!("{:x}", Sha256::digest(&bytes));
    if checksum != segment.checksum_sha256 {
        return Err(anyhow::anyhow!(
            "segment restore checksum mismatch for {}",
            segment.segment_id
        ));
    }
    fs::write(&restore_path, bytes)?;
    Ok(Some(restore_path))
}
