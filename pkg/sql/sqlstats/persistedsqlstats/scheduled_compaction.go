// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

type scheduledSQLStatsCompactionExecutor struct {
	metrics sqlStatsCompactionMetrics
}

var _ jobs.ScheduledJobExecutor = &scheduledSQLStatsCompactionExecutor{}

type sqlStatsCompactionMetrics struct {
	*jobs.ExecutorMetrics
	// TODO(azhng): wip: we might need more
}

var _ metric.Struct = &sqlStatsCompactionMetrics{}

// MetricStruct implements metric.Struct interface
func (m *sqlStatsCompactionMetrics) MetricStruct() {}

// ExecuteJob implements the jobs.ScheduledJobExecutor interface.
func (e *scheduledSQLStatsCompactionExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	if err := e.executeCompact(ctx, cfg, sj, txn); err != nil {
		e.metrics.NumFailed.Inc(1)
	}

	e.metrics.NumStarted.Inc(1)
	return nil
}

func (e *scheduledSQLStatsCompactionExecutor) executeCompact(
	ctx context.Context, cfg *scheduledjobs.JobExecutionConfig, sj *jobs.ScheduledJob, txn *kv.Txn,
) error {
	// TODO(azhng): wip:
	log.Infof(ctx, "TODO(azhng): wip")
	return nil
}

// NotifyJobTermination implements the jobs.ScheduledJobExecutor interface.
func (e *scheduledSQLStatsCompactionExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus jobs.Status,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	// TODO(azhng): wip:
	log.Infof(ctx, "TODO(azhng): wip: maybe we should handle this?")
	return nil
}

// Metrics implements the jobs.ScheduledJobExecutor interface.
func (e *scheduledSQLStatsCompactionExecutor) Metrics() metric.Struct {
	return &e.metrics
}

func init() {
	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledSQLStatsCompactionExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledSQLStatsCompactionExecutor.UserName())
			return &scheduledSQLStatsCompactionExecutor{
				metrics: sqlStatsCompactionMetrics{
					ExecutorMetrics: &m,
				},
			}, nil
		})
}
