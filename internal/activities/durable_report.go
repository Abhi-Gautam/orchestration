package activities

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	_ "modernc.org/sqlite"

	orchestrationv1 "orchestration/gen/orchestration/v1"
)

const (
	PersistReportActivityName          = "PersistReportActivity"
	ReportIdempotencyConflictErrorType = "ReportIdempotencyConflict"
)

type ReportStoreConfig struct {
	Path string
}

type ReportActivities struct {
	db *sql.DB
}

type ReportSummary struct {
	ArtifactCount  int32
	SemanticDigest string
}

type PersistReportInput struct {
	ReportID    string
	Summary     ReportSummary
	FailureCase orchestrationv1.DurableReportFailureCase
}

type ReportRecord struct {
	ReportID       string
	ArtifactCount  int32
	SemanticDigest string
	CreatedAt      time.Time
}

func NewReportActivities(ctx context.Context, config ReportStoreConfig) (*ReportActivities, error) {
	path := strings.TrimSpace(config.Path)
	if path == "" {
		return nil, errors.New("SQLite report store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create SQLite report store directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite report store: %w", err)
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
		`CREATE TABLE IF NOT EXISTS reports (
			report_id TEXT PRIMARY KEY,
			artifact_count INTEGER NOT NULL,
			semantic_digest TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("initialize SQLite report store: %w", err)
		}
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping SQLite report store: %w", err)
	}
	return &ReportActivities{db: db}, nil
}

func (a *ReportActivities) PersistReportActivity(ctx context.Context, input PersistReportInput) (ReportRecord, error) {
	if strings.TrimSpace(input.ReportID) == "" || input.Summary.ArtifactCount <= 0 || strings.TrimSpace(input.Summary.SemanticDigest) == "" {
		return ReportRecord{}, temporal.NewNonRetryableApplicationError(
			"report ID and a complete report summary are required",
			"InvalidPersistReportInput",
			nil,
		)
	}

	info := activity.GetInfo(ctx)
	existing, found, err := a.loadReport(ctx, input.ReportID)
	if err != nil {
		return ReportRecord{}, fmt.Errorf("load report %q: %w", input.ReportID, err)
	}
	if found {
		record, matchErr := matchingReport(input, existing)
		if matchErr == nil {
			activity.GetLogger(ctx).Info("PersistReportActivity reused report",
				"reportId", record.ReportID,
				"semanticDigest", record.SemanticDigest,
				"attempt", info.Attempt,
			)
		}
		return record, matchErr
	}

	if info.Attempt == 1 && input.FailureCase == orchestrationv1.DurableReportFailureCase_DURABLE_REPORT_FAILURE_CASE_PERSIST_BEFORE_COMMIT {
		return ReportRecord{}, temporal.NewApplicationError(
			"injected retryable failure before report commit",
			"InjectedBeforeReportCommit",
			Attempt(ctx),
		)
	}

	createdAt := time.Now().UTC()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return ReportRecord{}, fmt.Errorf("begin report transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		"INSERT INTO reports (report_id, artifact_count, semantic_digest, created_at) VALUES (?, ?, ?, ?)",
		input.ReportID,
		input.Summary.ArtifactCount,
		input.Summary.SemanticDigest,
		createdAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		_ = tx.Rollback()
		existing, found, loadErr := a.loadReport(ctx, input.ReportID)
		if loadErr != nil {
			return ReportRecord{}, fmt.Errorf("resolve concurrent report insert: %w", loadErr)
		}
		if found {
			return matchingReport(input, existing)
		}
		return ReportRecord{}, fmt.Errorf("insert report %q: %w", input.ReportID, err)
	}
	if err := tx.Commit(); err != nil {
		return ReportRecord{}, fmt.Errorf("commit report %q: %w", input.ReportID, err)
	}

	record := ReportRecord{
		ReportID:       input.ReportID,
		ArtifactCount:  input.Summary.ArtifactCount,
		SemanticDigest: input.Summary.SemanticDigest,
		CreatedAt:      createdAt,
	}
	activity.GetLogger(ctx).Info("PersistReportActivity committed report",
		"reportId", record.ReportID,
		"artifactCount", record.ArtifactCount,
		"semanticDigest", record.SemanticDigest,
		"attempt", info.Attempt,
	)
	if info.Attempt == 1 && input.FailureCase == orchestrationv1.DurableReportFailureCase_DURABLE_REPORT_FAILURE_CASE_PERSIST_AFTER_COMMIT {
		return ReportRecord{}, temporal.NewApplicationError(
			"injected retryable failure after report commit",
			"InjectedAfterReportCommit",
			Attempt(ctx),
		)
	}
	return record, nil
}

func (a *ReportActivities) loadReport(ctx context.Context, reportID string) (ReportRecord, bool, error) {
	var record ReportRecord
	var createdAt string
	err := a.db.QueryRowContext(ctx,
		"SELECT report_id, artifact_count, semantic_digest, created_at FROM reports WHERE report_id = ?",
		reportID,
	).Scan(&record.ReportID, &record.ArtifactCount, &record.SemanticDigest, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ReportRecord{}, false, nil
	}
	if err != nil {
		return ReportRecord{}, false, err
	}
	record.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return ReportRecord{}, false, fmt.Errorf("parse report creation time: %w", err)
	}
	return record, true, nil
}

func matchingReport(input PersistReportInput, existing ReportRecord) (ReportRecord, error) {
	if existing.ArtifactCount != input.Summary.ArtifactCount || existing.SemanticDigest != input.Summary.SemanticDigest {
		return ReportRecord{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("report ID %q already exists with different semantic content", input.ReportID),
			ReportIdempotencyConflictErrorType,
			nil,
			input.ReportID,
			existing.SemanticDigest,
			input.Summary.SemanticDigest,
		)
	}
	return existing, nil
}
