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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/errors"
)

// TODO(azhng): wip: docs and stuff
// TODO(azhng): wip: we also move this file to its own package because we need
//  to import both sql package here. That would be a lot of stuff to be pulled
//  in. As long as this package is not being pulled in by any of other sqlstats
//  package we should be fine.

type resumer struct {
	*jobs.Job
}

var _ jobs.Resumer = &resumer{}

func (r *resumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	ie := p.ExecCfg().InternalExecutor

	err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		row, err := ie.QueryRowEx(ctx,
			"scan-sql-stats-entries",
			txn,
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			"SELECT count(*) FROM system.statement_statistics")
		if err != nil {
			return err
		}

		if row.Len() != 1 {
			return errors.AssertionFailedf("unexpected number of column returned")
		}

		maxStatsEntry := persistedsqlstats.SQLStatsMaxPersistedRows.Get(&p.ExecCfg().Settings.SV)
		stmtStatsEntryCnt := (int64)(*row[0].(*tree.DInt))
		fmt.Printf("Pretending to be compacting stats: stmtStatsCnt: %d, maxStatsEntry: %d\n", stmtStatsEntryCnt, maxStatsEntry)

		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

func (r *resumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	fmt.Println("yeeeet!")
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeSQLStatsCompaction, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &resumer{Job: job}
	})
}
