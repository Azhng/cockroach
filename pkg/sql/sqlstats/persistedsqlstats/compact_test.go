// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestSQLStatsCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	flushCallback, waitForFlush := createOnStatsFlushedCallback(t)

	testCluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &persistedsqlstats.TestingKnobs{
					OnStatsFlushFinished: flushCallback,
				},
			},
		},
	})

	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	firstServer := testCluster.Server(0 /* idx */)
	firstPgURL, firstServerConnCleanup := sqlutils.PGUrl(
		t, firstServer.ServingSQLAddr(), "CreateConnections" /* prefix */, url.User(security.RootUser))
	defer firstServerConnCleanup()

	firstSQLConn, err := gosql.Open("postgres", firstPgURL.String())
	require.NoError(t, err)

	defer func() {
		err := firstSQLConn.Close()
		require.NoError(t, err)
	}()

	firstServerSQLStatsHandle := firstServer.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)
	_, err = firstSQLConn.Exec("SET CLUSTER SETTING sql.stats.persisted_rows.max = 5")
	require.NoError(t, err)

	stmt := "SELECT 1"

	// TODO(azhng): wip: each iteration in the loop creates a new distinct fingerprint
	for i := 0; i < 10; i++ {
		_, err = firstSQLConn.Exec(stmt)
		require.NoError(t, err)

		stmt = fmt.Sprintf("%s, 1", stmt)
	}

	firstServerSQLStatsHandle.Flush(ctx, stopper)
	waitForFlush(t, time.Minute /* timeout */, 1 /* expectedFlushEventCount */)

	compactionExecutor := &persistedsqlstats.CompactionExecutor{
		Settings: firstServer.ClusterSettings(),
	}
	compactionExecutor.Init(firstServer.InternalExecutor().(sqlutil.InternalExecutor), firstServer.DB())

	err = compactionExecutor.DeleteOldestEntries(ctx)
	require.NoError(t, err)
}
