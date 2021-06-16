package sqlstatsutil_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sscluster/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func jsonTestHelper(t *testing.T, sqlConn *gosql.DB, expectedStr string, actual json.JSON) {
	expected, err := json.ParseJSON(expectedStr)
	require.NoError(t, err)

	cmp, err := actual.Compare(expected)
	require.NoError(t, err)
	require.True(t, cmp == 0, "expected %s\nbut found %s", expected.String(), actual.String())
}

func TestSQLStatsJsonEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, sqlConn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	defer func() {
		err := sqlConn.Close()
		require.NoError(t, err)
	}()

	t.Run("encode statement metadata", func(t *testing.T) {
		input := roachpb.CollectedStatementStatistics{}
		expectedMetadataStr := `
{
  "stmtTyp": "",
  "query":   "",
  "db":      "",
  "distsql": false,
  "failed":  false,
  "opt":     false,
  "implicitTxn": false,
  "vec":         false,
  "fullScan":    false
}
`

		actualJSON, err := sqlstatsutil.BuildStmtMetadataJSON(&input)
		require.NoError(t, err)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled roachpb.CollectedStatementStatistics
		err = sqlstatsutil.UnmarshalStmtStatsMetadataJSON(actualJSON, &actualJSONUnmarshalled.Key, &actualJSONUnmarshalled.Stats)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)

		jsonTestHelper(t, sqlConn, expectedMetadataStr, actualJSON)
	})

	t.Run("encode statement statistics", func(t *testing.T) {
		input := roachpb.CollectedStatementStatistics{}
		expectedStatisticsStr := `
     {
       "statistics": {
         "firstAttemptCnt": 0,
         "maxRetries":      0,
         "lastExecAt":      "0001-01-01T00:00:00Z",
         "numRows": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "parseLat": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "planLat": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "runLat": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "svcLat": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "ovhLat": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "bytesRead": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "rowsRead": {
           "mean": 0.0,
           "sqDiff": 0.0
         }
       },
       "execution_statistics": {
         "cnt": 0,
         "networkBytes": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "maxMemUsage": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "contentionTime": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "networkMsgs": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "maxDiskUsage": {
           "mean": 0.0,
           "sqDiff": 0.0
         }
       }
     }
		 `

		actualJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&input)
		require.NoError(t, err)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled roachpb.CollectedStatementStatistics
		err = sqlstatsutil.UnmarshalStmtStatsStatisticsJSON(actualJSON, &actualJSONUnmarshalled.Stats)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)

		jsonTestHelper(t, sqlConn, expectedStatisticsStr, actualJSON)
	})

	t.Run("encode transaction metadata", func(t *testing.T) {
		input := roachpb.CollectedTransactionStatistics{
			StatementFingerprintIDs: []roachpb.StmtFingerprintID{
				1, 100, 1000, 5467890,
			},
		}
		expectedMetadataStr := `
{
  "stmtFingerprintIDs": [
    "0000000000000001",
    "0000000000000064",
    "00000000000003e8",
    "0000000000536ef2"
  ]
}
`
		actualJSON := sqlstatsutil.BuildTxnMetadataJSON(&input)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled roachpb.CollectedTransactionStatistics
		err := sqlstatsutil.UnmarshalTxnStatsMetadataJSON(actualJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)

		jsonTestHelper(t, sqlConn, expectedMetadataStr, actualJSON)
	})

	t.Run("encode transaction statistics", func(t *testing.T) {
		input := roachpb.CollectedTransactionStatistics{}
		expectedStatisticsStr := `
{
  "statistics": {
    "maxRetries": 0,
    "numRows": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "svcLat": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "retryLat": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "commitLat": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "bytesRead": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "rowsRead": {
      "mean": 0.0,
      "sqDiff": 0.0
    }
  },
  "execution_statistics": {
    "cnt": 0,
    "networkBytes": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "maxMemUsage": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "contentionTime": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "networkMsgs": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "maxDiskUsage": {
      "mean": 0.0,
      "sqDiff": 0.0
    }
  }
}
`
		actualJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(&input)
		require.NoError(t, err)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled roachpb.CollectedTransactionStatistics
		err = sqlstatsutil.UnmarshalTxnStatsStatisticsJSON(actualJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)

		jsonTestHelper(t, sqlConn, expectedStatisticsStr, actualJSON)
	})
}

func BenchmarkSQLStatsJson(b *testing.B) {
	defer log.Scope(b).Close(b)
	// TODO(azhng): wip: am i doing this right?

	b.Run("statement_stats", func(b *testing.B) {
		inputStmtStats := roachpb.CollectedStatementStatistics{}
		b.Run("encoding", func(b *testing.B) {
			b.SetBytes(int64(inputStmtStats.Size()))

			for i := 0; i < b.N; i++ {
				_, _ = sqlstatsutil.BuildStmtMetadataJSON(&inputStmtStats)
				_, _ = sqlstatsutil.BuildStmtStatisticsJSON(&inputStmtStats)
			}
		})

		inputStmtStatsMetaJSON, _ := sqlstatsutil.BuildStmtMetadataJSON(&inputStmtStats)
		inputStmtStatsJSON, _ := sqlstatsutil.BuildStmtStatisticsJSON(&inputStmtStats)
		result := roachpb.CollectedStatementStatistics{}

		b.Run("decoding", func(b *testing.B) {
			b.SetBytes(int64(inputStmtStatsJSON.Size()))

			for i := 0; i < b.N; i++ {
				_ = sqlstatsutil.UnmarshalStmtStatsJSON(inputStmtStatsJSON, inputStmtStatsMetaJSON, &result)
			}
		})
	})

	b.Run("transaction_stats", func(b *testing.B) {
		inputTxnStats := roachpb.CollectedTransactionStatistics{}
		b.Run("encoding", func(b *testing.B) {
			b.SetBytes(int64(inputTxnStats.Size()))

			for i := 0; i < b.N; i++ {
				_ = sqlstatsutil.BuildTxnMetadataJSON(&inputTxnStats)
				_, _ = sqlstatsutil.BuildTxnStatisticsJSON(&inputTxnStats)
			}
		})

		inputTxnStatsJSON, _ := sqlstatsutil.BuildTxnStatisticsJSON(&inputTxnStats)
		inputTxnStatsMetaJSON := sqlstatsutil.BuildTxnMetadataJSON(&inputTxnStats)
		result := roachpb.CollectedTransactionStatistics{}

		b.Run("decoding", func(b *testing.B) {
			b.SetBytes(int64(inputTxnStatsJSON.Size()))

			for i := 0; i < b.N; i++ {
				_ = sqlstatsutil.UnmarshalTxnStatsJSON(inputTxnStatsJSON, inputTxnStatsMetaJSON, &result)
			}
		})
	})
}
