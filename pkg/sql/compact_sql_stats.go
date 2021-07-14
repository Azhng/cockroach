// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
)

// TODO(azhng): wip: docs and stuff

type resumer struct {
	*jobs.Job

	compactionExecutor *persistedsqlstats.CompactionExecutor
}

var _ jobs.Resumer = &resumer{}

// Resume implements the jobs.Resumer interface.
func (r *resumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	r.compactionExecutor.Init(p.ExecCfg().InternalExecutor, p.ExecCfg().DB)

	err := r.compactionExecutor.DeleteOldestEntries(ctx)
	if err != nil {
		fmt.Printf("error within resume: %s\n", err)
	}

	return err
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	fmt.Printf("OnFailOrCancel: execCtx: %+v\n", execCtx)
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeSQLStatsCompaction, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &resumer{
			Job: job,
			compactionExecutor: &persistedsqlstats.CompactionExecutor{
				Settings: settings,
			},
		}
	})
}
