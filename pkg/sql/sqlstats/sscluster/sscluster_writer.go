// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sscluster

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
)

// TODO(azhng): wip: docs

type StatsWriter struct {
	// local in-memory storage.
	memWriter sqlstats.Writer

	// Use to signal the stats writer is experiencing memory pressure.
	memoryPressureSignal chan struct{}
}

var _ sqlstats.Writer = &StatsWriter{}

// RecordStatement implements sqlstats.Writer interface.
func (s *StatsWriter) RecordStatement(
	ctx context.Context, key roachpb.StatementStatisticsKey, value sqlstats.RecordedStmtStats,
) (roachpb.StmtFingerprintID, error) {
	stmtFingerprintID, err := s.memWriter.RecordStatement(ctx, key, value)
	if err == ssmemstorage.ErrFingerprintLimitReached || err == ssmemstorage.ErrMemoryPressure {
		// TODO(azhng): wip: this might block, do we want to handle this at the receiver side?
		//  or do we want to handle it here?
		// TODO(azhng): wip: also metrics + logging
		s.memoryPressureSignal <- struct{}{}
	}
	return stmtFingerprintID, err
}

// RecordStatementExecStats implements sqlstats.Writer interface.
func (s *StatsWriter) RecordStatementExecStats(
	key roachpb.StatementStatisticsKey, stats execstats.QueryLevelStats,
) error {
	return s.memWriter.RecordStatementExecStats(key, stats)
}

// ShouldSaveLogicalPlanDesc implements sqlstats.Writer interface.
func (s *StatsWriter) ShouldSaveLogicalPlanDesc(
	fingerprint string, implicitTxn bool, database string,
) bool {
	return s.memWriter.ShouldSaveLogicalPlanDesc(fingerprint, implicitTxn, database)
}

// RecordTransaction implements sqlstats.Writer interface and saves
// per-transaction statistics.
func (s *StatsWriter) RecordTransaction(
	ctx context.Context, key roachpb.TransactionFingerprintID, value sqlstats.RecordedTxnStats,
) error {
	err := s.memWriter.RecordTransaction(ctx, key, value)
	if err == ssmemstorage.ErrMemoryPressure || err == ssmemstorage.ErrFingerprintLimitReached {
		// TODO(azhng): wip: ditto
		s.memoryPressureSignal <- struct{}{}
	}
	return err
}
