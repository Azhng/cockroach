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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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

	// TODO(azhng): wip: checking for running job? if such job exists the abort.
	stmtStatsEntryCnt, txnStatsEntryCnt, err := r.getExistingStmtAndTxnStatsEntries(ctx, ie)
	if err != nil {
		return err
	}

	err = r.deleteOldestEntries(ctx, p.ExecCfg(), stmtStatsEntryCnt, txnStatsEntryCnt)
	return nil
}

func (r *resumer) deleteOldestEntries(
	ctx context.Context, execCfg *ExecutorConfig, curStmtStatsEntries, curTxnStatsEntries int64,
) error {
	maxStatsEntry := persistedsqlstats.SQLStatsMaxPersistedRows.Get(&execCfg.Settings.SV)

	// TODO(azhng): wip: remove this.
	fmt.Printf("Pretending to be compacting stats: stmtStatsCnt: %d, txnStatsCnt: %d, maxStatsEntry: %d\n", curStmtStatsEntries, curTxnStatsEntries, maxStatsEntry)

	stmt := `
DELETE FROM %[1]s
WHERE fingerprint_id IN (
  SELECT fingerprint_id
  FROM %[1]s
  ORDER BY aggregated_ts
  LIMIT %[2]d
)
`
	err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if entriesToRemove := curStmtStatsEntries - maxStatsEntry; entriesToRemove > 0 {
			_, err := execCfg.InternalExecutor.ExecEx(ctx,
				"delete-old-stmt-stats",
				txn,
				sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
				fmt.Sprintf(stmt, "system.statement_statistics", entriesToRemove))

			if err != nil {
				return err
			}
		}

		if entriesToRemove := curTxnStatsEntries - maxStatsEntry; entriesToRemove > 0 {
			_, err := execCfg.InternalExecutor.ExecEx(ctx,
				"delete-old-txn-stats",
				txn,
				sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
				fmt.Sprintf(stmt, "system.transaction_statistics", entriesToRemove))

			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (r *resumer) getExistingStmtAndTxnStatsEntries(
	ctx context.Context, ie sqlutil.InternalExecutor,
) (stmtStatsEntryCnt, txnStatsEntryCnt int64, err error) {
	row, err := ie.QueryRowEx(ctx,
		"scan-sql-stmt-stats-entries",
		nil,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		"SELECT count(*) FROM system.statement_statistics AS OF SYSTEM TIME follower_read_timestamp()")
	if err != nil {
		return 0, 0, err
	}

	if row.Len() != 1 {
		return 0, 0, errors.AssertionFailedf("unexpected number of column returned")
	}
	stmtStatsEntryCnt = (int64)(*row[0].(*tree.DInt))

	row, err = ie.QueryRowEx(ctx,
		"scan-sql-txn-stats-entries",
		nil,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		"SELECT count(*) FROM system.transaction_statistics AS OF SYSTEM TIME follower_read_timestamp()")
	if err != nil {
		return 0, 0, err
	}

	if row.Len() != 1 {
		return 0, 0, errors.AssertionFailedf("unexpected number of column returned")
	}
	txnStatsEntryCnt = (int64)(*row[0].(*tree.DInt))

	return stmtStatsEntryCnt, txnStatsEntryCnt, nil
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
