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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TODO(azhng): wip: docs

type Config struct {
	InternalExecutor sqlutil.InternalExecutor
}

// flushState represents the current flushState of the ClusterSQLStats.
type flushState int

const (
	idle flushState = iota
	flushing
)

type ClusterSQLStats struct {
	localSQLStats sslocal.SQLStats

	ie sqlutil.InternalExecutor

	memoryPressureSignal chan struct{}

	mu struct {
		syncutil.RWMutex
		state flushState
	}
}

var _ sqlstats.Provider = &ClusterSQLStats{}

func New(cfg *Config) *ClusterSQLStats {
	return &ClusterSQLStats{
		ie:                   cfg.InternalExecutor,
		memoryPressureSignal: make(chan struct{}),
	}
}

func (s *ClusterSQLStats) Start(ctx context.Context, stopper *stop.Stopper) {
	s.startSQLStatsFlushLoop(ctx, stopper)
}

func (s *ClusterSQLStats) startSQLStatsFlushLoop(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "sql-stats-flusher", func(ctx context.Context) {
		for {
			select {
			case <-s.memoryPressureSignal:
				s.startFlush(ctx, stopper)
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

func (s *ClusterSQLStats) startFlush(ctx context.Context, stopper *stop.Stopper) {
	s.mu.Lock()

	switch s.mu.state {
	case idle:
		s.mu.state = flushing
		s.mu.Unlock()
		err := stopper.RunAsyncTask(ctx, "sql-stats-flush-worker", func(ctx context.Context) {
			defer func() {
				s.mu.Lock()
				s.mu.state = idle
				s.mu.Unlock()
			}()

			// TODO(azhng): wip: actually do work to flush.
		})

		if err != nil {
			panic("how do we want to handle this, this is not related to goroutine themselves")
		}
	case flushing:
		s.mu.Unlock()
		// We do nothing here if we are already flushing.
		return
	}
}

// GetWriterForApplication implements sqlstats.Provider interface.
func (s *ClusterSQLStats) GetWriterForApplication(appName string) sqlstats.Writer {
	writer := s.localSQLStats.GetWriterForApplication(appName)
	return &StatsWriter{
		memWriter:            writer,
		memoryPressureSignal: s.memoryPressureSignal,
	}
}

// GetLastReset implements sqlstats.Provider interface.
func (s *ClusterSQLStats) GetLastReset() time.Time {
	panic("does this still make sense to be here?")
}

// IterateStatementStats implements sqlstats.Provider interface.
func (s *ClusterSQLStats) IterateStatementStats(
	_ context.Context, options *sqlstats.IteratorOptions, visitor sqlstats.StatementVisitor,
) error {
	panic("not yet implemented, we still need to extend the iterator options i suppose")
}

// IterateTransactionStats implements sqlstats.Provider interface.
func (s *ClusterSQLStats) IterateTransactionStats(
	_ context.Context, options *sqlstats.IteratorOptions, visitor sqlstats.TransactionVisitor,
) error {
	panic("not yet implemented, we still need to extend the iterator options i suppose")
}

// IterateAggregatedTransactionStats implements sqlstats.Provider interface.
func (s *ClusterSQLStats) IterateAggregatedTransactionStats(
	_ context.Context,
	options *sqlstats.IteratorOptions,
	visitor sqlstats.AggregatedTransactionVisitor,
) error {
	panic("not yet implemented, we still need to extend the iterator options i suppose")
}

// GetStatementStats implements sqlstats.Provider interface.
func (s *ClusterSQLStats) GetStatementStats(
	key *roachpb.StatementStatisticsKey,
) (*roachpb.CollectedStatementStatistics, error) {
	panic("not yet implemented")
}

// GetTransactionStats implements sqlstats.Provider interface.
func (s *ClusterSQLStats) GetTransactionStats(
	appName string, key roachpb.TransactionFingerprintID,
) (*roachpb.CollectedTransactionStatistics, error) {
	panic("not yet implemented")
}

// Reset implements sqlstats.Provider interface.
func (s *ClusterSQLStats) Reset(ctx context.Context) error {
	panic("not yet implemented, also does Reset here makes sense?")
}
