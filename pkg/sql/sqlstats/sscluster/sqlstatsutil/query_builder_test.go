// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstatsutil_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sscluster/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func encodeFingerprintID(id uint64) []byte {
	result := make([]byte, 0, 8)
	return encoding.EncodeUint64Ascending(result, id)
}

func getStmtStatsArgs(
	t *testing.T, stmtStats *roachpb.CollectedStatementStatistics,
) (fingerprintID []byte, metadata tree.Datum, stats *tree.DJSON, plan []byte) {
	fingerprintID = encodeFingerprintID(uint64(stmtStats.ID))

	serializedStmtStats, err := sqlstatsutil.BuildStmtStatisticsJSON(stmtStats)
	require.NoError(t, err)

	metadataJSON, err := sqlstatsutil.BuildStmtMetadataJSON(stmtStats)
	require.NoError(t, err)

	metadata = tree.NewDJSON(metadataJSON)
	stats = tree.NewDJSON(serializedStmtStats)

	plan, err = stmtStats.Stats.SensitiveInfo.MostRecentPlanDescription.Marshal()
	require.NoError(t, err)

	return fingerprintID, metadata, stats, plan
}

func getTxnStatsArgs(
	t *testing.T, dummyFingerprintID uint64, txnStats *roachpb.CollectedTransactionStatistics,
) (fingerprintID []byte, metadata tree.Datum, stats *tree.DJSON) {
	// Use a dummy txnFingerprintID since it is not available within the protobuf.
	fingerprintID = encodeFingerprintID(dummyFingerprintID)

	serializedTxnStats, err := sqlstatsutil.BuildTxnStatisticsJSON(txnStats)
	require.NoError(t, err)

	metadataJSON := sqlstatsutil.BuildTxnMetadataJSON(txnStats)
	require.NoError(t, err)

	metadata = tree.NewDJSON(metadataJSON)
	stats = tree.NewDJSON(serializedTxnStats)

	return fingerprintID, metadata, stats
}

func getInsertedData(
	t *testing.T, tableName string, sqlConn *gosql.DB, fingerprintID []byte, appName string,
) (count int64, stats json.JSON, metadata json.JSON) {
	rows, _ := sqlConn.Query(fmt.Sprintf(`
SELECT
    count,
    statistics,
		metadata
FROM
    %s
WHERE fingerprint_id = $1
    AND app_name = $2`, tableName), fingerprintID, appName)

	require.True(t, rows.Next(), "expecting rows from the result, but found none")

	var statsRaw, metadataRaw []byte
	err := rows.Scan(&count, &statsRaw, &metadataRaw)
	require.NoError(t, err)

	stats, err = json.ParseJSON(string(statsRaw))
	require.NoError(t, err)
	metadata, err = json.ParseJSON(string(metadataRaw))
	require.NoError(t, err)

	require.False(t, rows.Next(), "expected exactly one row, but found more")
	err = rows.Close()
	require.NoError(t, err)

	return count, stats, metadata
}

func getInsertedTxnStats(
	t *testing.T, sqlConn *gosql.DB, fingerprintID []byte, appName string,
) roachpb.CollectedTransactionStatistics {
	count, stats, metadata :=
		getInsertedData(t, "system.transaction_statistics", sqlConn, fingerprintID, appName)

	actualInsertedData := roachpb.CollectedTransactionStatistics{
		App: appName,
		Stats: roachpb.TransactionStatistics{
			Count: count,
		},
	}

	err := sqlstatsutil.UnmarshalTxnStatsJSON(stats, metadata, &actualInsertedData)
	require.NoError(t, err)

	return actualInsertedData
}

func getInsertedStmtStats(
	t *testing.T, sqlConn *gosql.DB, fingerprintID []byte, appName string,
) roachpb.CollectedStatementStatistics {
	count, stats, metadata :=
		getInsertedData(t, "system.statement_statistics", sqlConn, fingerprintID, appName)

	actualInsertedData := roachpb.CollectedStatementStatistics{
		Key: roachpb.StatementStatisticsKey{
			App: appName,
		},
		Stats: roachpb.StatementStatistics{
			Count: count,
		},
	}

	err := sqlstatsutil.UnmarshalStmtStatsJSON(stats, metadata, &actualInsertedData)
	require.NoError(t, err)

	return actualInsertedData
}

func checkExecStats(t *testing.T, expected, actual roachpb.ExecStats, epsilon float64) {
	require.Equal(t, expected.Count, actual.Count)
	require.True(t, expected.MaxMemUsage.AlmostEqual(actual.MaxMemUsage, epsilon), "expected %+v, found %+v", expected.MaxMemUsage, actual.MaxMemUsage)
	require.True(t, expected.MaxDiskUsage.AlmostEqual(actual.MaxDiskUsage, epsilon), "expected %+v, found %+v", expected.MaxDiskUsage, actual.MaxDiskUsage)
	require.True(t, expected.NetworkMessages.AlmostEqual(actual.NetworkMessages, epsilon), "expected %+v, found %+v", expected.NetworkMessages, actual.NetworkMessages)
	require.True(t, expected.NetworkBytes.AlmostEqual(actual.NetworkBytes, epsilon), "expected %+v, found %+v", expected.NetworkBytes, actual.NetworkBytes)
	require.True(t, expected.ContentionTime.AlmostEqual(actual.ContentionTime, epsilon), "expected %+v, found %+v", expected.ContentionTime, actual.ContentionTime)
}

func TestInsertionQueries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, sqlConn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	defer func() {
		err := sqlConn.Close()
		require.NoError(t, err)
	}()

	t.Run("statement", func(t *testing.T) {
		insertStmtData := roachpb.CollectedStatementStatistics{}

		internalEx := srv.InternalExecutor().(sqlutil.InternalExecutor)

		t.Run("initial_insert", func(t *testing.T) {
			fingerprintID, stmtStatsMetadataDatum, stmtStatsDatum, stmtPlan := getStmtStatsArgs(t, &insertStmtData)
			rowsAffected, err := internalEx.ExecEx(
				ctx,
				"insert-stmt-stats",
				nil, /* txn */
				sessiondata.InternalExecutorOverride{
					User: security.NodeUserName(),
				},
				sqlstatsutil.StmtStatsInsertQuery,
				timeutil.Unix(0 /* sec */, 0 /* nsec */), // aggregated_ts
				fingerprintID,                            // fingerprint_ID
				0,                                        // plan_id
				insertStmtData.Key.App,                   // app_name
				1,                                        // node_id
				insertStmtData.Stats.Count,               // count
				time.Hour,                                // agg_internal
				stmtStatsMetadataDatum,                   // metadata
				stmtStatsDatum,                           // statistics
				stmtPlan,                                 // plan
			)

			require.NoError(t, err)
			require.Equal(t, 1 /* expected */, rowsAffected)

			actualInsertedData := getInsertedStmtStats(t, sqlConn, fingerprintID, insertStmtData.Key.App)

			// This needs to be exactly the same since we haven't done any floating
			// point arithmetic yet.
			require.Equal(t, insertStmtData, actualInsertedData)
		})

		// Initial update tests the case where an existing stats row where certain
		// stats fields are empty.
		var expectedStats roachpb.CollectedStatementStatistics
		t.Run("initial_update", func(t *testing.T) {
			// We modify some of the data and inserting it again to verify the
			// ON CONFLICT clause is properly executed.
			insertStmtData.Stats.Count = 10

			insertStmtData.Stats.MaxRetries = 25

			insertStmtData.Stats.NumRows.Mean = 8.0234
			insertStmtData.Stats.NumRows.SquaredDiffs = 20.7333

			insertStmtData.Stats.RowsRead.Mean = 12.165
			insertStmtData.Stats.RowsRead.SquaredDiffs = 23.519

			insertStmtData.Stats.LastExecTimestamp = timeutil.Unix(1 /* sec */, 10 /* nsec */)

			// Also test the ExecStats since it behaves differently than regular stats.
			insertStmtData.Stats.ExecStats.Count = 6
			insertStmtData.Stats.ExecStats.NetworkMessages.Mean = 18.165
			insertStmtData.Stats.ExecStats.NetworkMessages.SquaredDiffs = 21.519

			expectedStats.Stats.Add(&insertStmtData.Stats)

			fingerprintID, stmtStatsMetadataDatum, stmtStatsDatum, stmtPlan := getStmtStatsArgs(t, &insertStmtData)
			rowsAffected, err := internalEx.ExecEx(
				ctx,
				"update-stmt-stats",
				nil, /* txn */
				sessiondata.InternalExecutorOverride{
					User: security.NodeUserName(),
				},
				sqlstatsutil.StmtStatsInsertQuery,
				timeutil.Unix(0 /* sec */, 0 /* nsec */), // aggregated_ts
				fingerprintID,                            // fingerprint_id
				0,                                        // plan_id
				insertStmtData.Key.App,                   // app_name
				1,                                        // node_id
				insertStmtData.Stats.Count,               // count
				time.Hour,                                // agg_internal
				stmtStatsMetadataDatum,                   // metadata
				stmtStatsDatum,                           // statistics
				stmtPlan,                                 // plan
			)

			require.NoError(t, err)
			require.Equal(t, 1 /* expected */, rowsAffected)

			actual := getInsertedStmtStats(t, sqlConn, fingerprintID, insertStmtData.Key.App)

			require.Equal(t, expectedStats.Key, actual.Key)
			epsilon := 0.000001
			require.True(t, actual.Stats.AlmostEqual(&expectedStats.Stats, epsilon), "expected %+v\nbut found %+v", expectedStats.Stats, actual.Stats)

			checkExecStats(t, expectedStats.Stats.ExecStats, actual.Stats.ExecStats, epsilon)
		})

		// We perform a subsequent update to the same stats row to test the case
		// where non-empty stats fields will be updated correctly.
		t.Run("subsequent_update", func(t *testing.T) {
			insertStmtData.Stats.Count = 60
			insertStmtData.Stats.MaxRetries = 90

			insertStmtData.Stats.NumRows.Mean = 32.519
			insertStmtData.Stats.NumRows.SquaredDiffs = 1.2358

			insertStmtData.Stats.RowsRead.Mean = 12.235
			insertStmtData.Stats.RowsRead.SquaredDiffs = 124.6234

			insertStmtData.Stats.LastExecTimestamp = timeutil.Unix(50 /* sec */, 6000 /* nsec */)

			insertStmtData.Stats.ExecStats.Count = 15
			insertStmtData.Stats.ExecStats.NetworkMessages.Mean = 43.5
			insertStmtData.Stats.ExecStats.NetworkMessages.SquaredDiffs = 123.5

			// We reuse the expectedStats from the previous subtests but update it
			// with the new value.
			expectedStats.Stats.Add(&insertStmtData.Stats)

			fingerprintID, stmtStatsMetadataDatum, stmtStatsDatum, stmtPlan := getStmtStatsArgs(t, &insertStmtData)
			rowsAffected, err := internalEx.ExecEx(
				ctx,
				"subsequent-update",
				nil, /* txn */
				sessiondata.InternalExecutorOverride{
					User: security.NodeUserName(),
				},
				sqlstatsutil.StmtStatsInsertQuery,
				timeutil.Unix(0 /* sec */, 0 /* nsec */), // aggregated_ts
				fingerprintID,                            // fingerprint_id
				0,                                        // plan_id
				insertStmtData.Key.App,                   // app_name
				1,                                        // node_id
				insertStmtData.Stats.Count,               // count
				time.Hour,                                // agg_internal
				stmtStatsMetadataDatum,                   // metadata
				stmtStatsDatum,                           // statistics
				stmtPlan,                                 // plan
			)

			require.NoError(t, err)
			require.Equal(t, 1 /* expected */, rowsAffected)

			actual := getInsertedStmtStats(t, sqlConn, fingerprintID, insertStmtData.Key.App)

			require.Equal(t, expectedStats.Key, actual.Key)
			epsilon := 0.000001
			require.True(t, actual.Stats.AlmostEqual(&expectedStats.Stats, epsilon), "expected %+v\nbut found %+v", expectedStats.Stats, actual.Stats)
			checkExecStats(t, expectedStats.Stats.ExecStats, actual.Stats.ExecStats, epsilon)
		})

		t.Run("different_stats_insert", func(t *testing.T) {
			differentStmtStats := roachpb.CollectedStatementStatistics{
				Key: roachpb.StatementStatisticsKey{
					App: "different app",
				},
				Stats: roachpb.StatementStatistics{
					Count: 15,
				},
			}

			fingerprintID, stmtStatsMetadataDatum, stmtStatsDatum, stmtPlan := getStmtStatsArgs(t, &differentStmtStats)

			rowsAffected, err := internalEx.ExecEx(
				ctx,
				"different-stats-insert",
				nil, /* txn */
				sessiondata.InternalExecutorOverride{
					User: security.NodeUserName(),
				},
				sqlstatsutil.StmtStatsInsertQuery,
				timeutil.Unix(0 /* sec */, 0 /* nsec */), // aggregated_ts
				fingerprintID,                            // fingerprint_id
				0,                                        // plan_id
				differentStmtStats.Key.App,               // app_name
				1,                                        // node_id
				differentStmtStats.Stats.Count,           // count
				time.Hour,                                // agg_internal
				stmtStatsMetadataDatum,                   // metadata
				stmtStatsDatum,                           // statistics
				stmtPlan,                                 // plan
			)
			require.NoError(t, err)
			require.Equal(t, 1 /* expected */, rowsAffected)

			actual := getInsertedStmtStats(t, sqlConn, fingerprintID, differentStmtStats.Key.App)

			require.Equal(t, differentStmtStats, actual)
		})
	})

	t.Run("transaction", func(t *testing.T) {
		insertTxnData := roachpb.CollectedTransactionStatistics{
			StatementFingerprintIDs: []roachpb.StmtFingerprintID{
				1, 100, 12340, 542523412,
			},
		}
		internalEx := srv.InternalExecutor().(sqlutil.InternalExecutor)
		dummyFingerprintID := rand.Uint64()

		t.Run("initial_insert", func(t *testing.T) {
			fingerprintID, txnStatsMetadataDatum, txnStatsDatum := getTxnStatsArgs(t, dummyFingerprintID, &insertTxnData)
			rowsAffected, err := internalEx.ExecEx(
				ctx,
				"insert-txn-stats",
				nil, /* txn */
				sessiondata.InternalExecutorOverride{
					User: security.NodeUserName(),
				},
				sqlstatsutil.TxnStatsInsertQuery,
				timeutil.Unix(0 /* sec */, 0 /* nsec */), // aggregated_ts
				fingerprintID,                            // fingerprint_ID
				insertTxnData.App,                        // app_name
				1,                                        // node_id
				insertTxnData.Stats.Count,                // count
				time.Hour,                                // agg_internal
				txnStatsMetadataDatum,                    // metadata
				txnStatsDatum,                            // statistics
			)

			require.NoError(t, err)
			require.Equal(t, 1 /* expected */, rowsAffected)

			actualInsertedData := getInsertedTxnStats(t, sqlConn, fingerprintID, insertTxnData.App)

			// This needs to be exactly the same since we haven't done any floating
			// point arithmetic yet.
			require.Equal(t, insertTxnData, actualInsertedData)
		})

		expectedTxnStats := roachpb.CollectedTransactionStatistics{
			StatementFingerprintIDs: insertTxnData.StatementFingerprintIDs,
		}

		t.Run("initial_update", func(t *testing.T) {
			insertTxnData.Stats.Count = 15
			insertTxnData.Stats.MaxRetries = 3

			insertTxnData.Stats.ServiceLat.Mean = 9345.234
			insertTxnData.Stats.ServiceLat.SquaredDiffs = 234.425123

			insertTxnData.Stats.ExecStats.MaxDiskUsage.Mean = 341.452345
			insertTxnData.Stats.ExecStats.MaxDiskUsage.SquaredDiffs = 61.231

			expectedTxnStats.Stats.Add(&insertTxnData.Stats)

			fingerprintID, txnStatsMetadataDatum, txnStatsDatum := getTxnStatsArgs(t, dummyFingerprintID, &insertTxnData)
			rowsAffected, err := internalEx.ExecEx(
				ctx,
				"insert-txn-stats",
				nil, /* txn */
				sessiondata.InternalExecutorOverride{
					User: security.NodeUserName(),
				},
				sqlstatsutil.TxnStatsInsertQuery,
				timeutil.Unix(0 /* sec */, 0 /* nsec */), // aggregated_ts
				fingerprintID,                            // fingerprint_ID
				insertTxnData.App,                        // app_name
				1,                                        // node_id
				insertTxnData.Stats.Count,                // count
				time.Hour,                                // agg_internal
				txnStatsMetadataDatum,                    // metadata
				txnStatsDatum,                            // statistics
			)

			require.NoError(t, err)
			require.Equal(t, 1 /* expected */, rowsAffected)

			actualInsertedData := getInsertedTxnStats(t, sqlConn, fingerprintID, insertTxnData.App)

			require.Equal(t, expectedTxnStats.StatementFingerprintIDs, actualInsertedData.StatementFingerprintIDs)
			require.Equal(t, expectedTxnStats.App, actualInsertedData.App)

			epsilon := 0.000001
			require.True(t, actualInsertedData.Stats.AlmostEqual(&expectedTxnStats.Stats, epsilon), "expected %+v\nbut found %+v", expectedTxnStats.Stats, actualInsertedData.Stats)
			checkExecStats(t, expectedTxnStats.Stats.ExecStats, actualInsertedData.Stats.ExecStats, epsilon)
		})

		t.Run("subsequent_update", func(t *testing.T) {
			insertTxnData.Stats.Count = 94
			insertTxnData.Stats.MaxRetries = 58

			insertTxnData.Stats.ServiceLat.Mean = 12.234
			insertTxnData.Stats.ServiceLat.SquaredDiffs = 1.3

			insertTxnData.Stats.BytesRead.Mean = 234.53
			insertTxnData.Stats.BytesRead.SquaredDiffs = 36.234123

			insertTxnData.Stats.ExecStats.MaxDiskUsage.Mean = 1.234123
			insertTxnData.Stats.ExecStats.MaxDiskUsage.SquaredDiffs = 325.313

			expectedTxnStats.Stats.Add(&insertTxnData.Stats)

			fingerprintID, txnStatsMetadataDatum, txnStatsDatum := getTxnStatsArgs(t, dummyFingerprintID, &insertTxnData)
			rowsAffected, err := internalEx.ExecEx(
				ctx,
				"insert-txn-stats",
				nil, /* txn */
				sessiondata.InternalExecutorOverride{
					User: security.NodeUserName(),
				},
				sqlstatsutil.TxnStatsInsertQuery,
				timeutil.Unix(0 /* sec */, 0 /* nsec */), // aggregated_ts
				fingerprintID,                            // fingerprint_ID
				insertTxnData.App,                        // app_name
				1,                                        // node_id
				insertTxnData.Stats.Count,                // count
				time.Hour,                                // agg_internal
				txnStatsMetadataDatum,                    // metadata
				txnStatsDatum,                            // statistics
			)

			require.NoError(t, err)
			require.Equal(t, 1 /* expected */, rowsAffected)

			actualInsertedData := getInsertedTxnStats(t, sqlConn, fingerprintID, insertTxnData.App)

			require.Equal(t, expectedTxnStats.StatementFingerprintIDs, actualInsertedData.StatementFingerprintIDs)
			require.Equal(t, expectedTxnStats.App, actualInsertedData.App)

			epsilon := 0.000001
			require.True(t, actualInsertedData.Stats.AlmostEqual(&expectedTxnStats.Stats, epsilon), "expected %+v\nbut found %+v", expectedTxnStats.Stats, actualInsertedData.Stats)
			checkExecStats(t, expectedTxnStats.Stats.ExecStats, actualInsertedData.Stats.ExecStats, epsilon)
		})

		t.Run("different_stats_insert", func(t *testing.T) {
			differentTxnStats := roachpb.CollectedTransactionStatistics{
				App: "different app2",
				StatementFingerprintIDs: []roachpb.StmtFingerprintID{
					3291, 34, 3481,
				},
			}

			differentTxnFingerprintID := dummyFingerprintID + 1

			fingerprintID, txnStatsMetadataDatum, txnStatsDatum := getTxnStatsArgs(t, differentTxnFingerprintID, &differentTxnStats)
			rowsAffected, err := internalEx.ExecEx(
				ctx,
				"insert-txn-stats",
				nil, /* txn */
				sessiondata.InternalExecutorOverride{
					User: security.NodeUserName(),
				},
				sqlstatsutil.TxnStatsInsertQuery,
				timeutil.Unix(0 /* sec */, 0 /* nsec */), // aggregated_ts
				fingerprintID,                            // fingerprint_ID
				differentTxnStats.App,                    // app_name
				1,                                        // node_id
				differentTxnStats.Stats.Count,            // count
				time.Hour,                                // agg_internal
				txnStatsMetadataDatum,                    // metadata
				txnStatsDatum,                            // statistics
			)

			require.NoError(t, err)
			require.Equal(t, 1 /* expected */, rowsAffected)

			actualInsertedData := getInsertedTxnStats(t, sqlConn, fingerprintID, differentTxnStats.App)

			require.Equal(t, differentTxnStats, actualInsertedData)
		})
	})
}

func BenchmarkSQLStatsInsertion(t *testing.B) {
	// TODO(azhng): wip: realistic benchmark for this one.
}
