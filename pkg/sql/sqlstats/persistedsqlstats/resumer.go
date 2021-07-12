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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// TODO(azhng): wip:

type resumer struct {
	*jobs.Job
}

var _ jobs.Resumer = &resumer{}

func (r *resumer) Resume(ctx context.Context, execCtxI interface{}) error {
	panic("not yet implemented")
}

func (r *resumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	panic("not yet implemented")
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeSQLStatsCompaction, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &resumer{Job: job}
	})
}
