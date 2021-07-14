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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

// CompactionExecutor is ... something // TODO(azhng): wip
type CompactionExecutor struct {
	Settings *cluster.Settings

	initialized bool
	db          *kv.DB
	ie          sqlutil.InternalExecutor
}

func (c *CompactionExecutor) Init(internalEx sqlutil.InternalExecutor, db *kv.DB) {
	c.db = db
	c.ie = internalEx
	c.initialized = true
}

func (c *CompactionExecutor) DeleteOldestEntries(ctx context.Context) error {
	if !c.initialized {
		return errors.New("CompactExecutor is not being initialized")
	}

	stmtStatsEntryCnt, txnStatsEntryCnt, err := c.getExistingStmtAndTxnStatsEntries(ctx)
	if err != nil {
		return err
	}

	err = c.deleteOldestEntries(ctx, stmtStatsEntryCnt, txnStatsEntryCnt)
	return nil
}

func (c *CompactionExecutor) getExistingStmtAndTxnStatsEntries(
	ctx context.Context,
) (stmtStatsEntryCnt, txnStatsEntryCnt int64, err error) {
	row, err := c.ie.QueryRowEx(ctx,
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

	row, err = c.ie.QueryRowEx(ctx,
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

func (c *CompactionExecutor) deleteOldestEntries(
	ctx context.Context, curStmtStatsEntries, curTxnStatsEntries int64,
) error {
	maxStatsEntry := SQLStatsMaxPersistedRows.Get(&c.Settings.SV)

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
	err := c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if entriesToRemove := curStmtStatsEntries - maxStatsEntry; entriesToRemove > 0 {
			_, err := c.ie.ExecEx(ctx,
				"delete-old-stmt-stats",
				txn,
				sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
				fmt.Sprintf(stmt, "system.statement_statistics", entriesToRemove))

			if err != nil {
				return err
			}
		}

		if entriesToRemove := curTxnStatsEntries - maxStatsEntry; entriesToRemove > 0 {
			_, err := c.ie.ExecEx(ctx,
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
