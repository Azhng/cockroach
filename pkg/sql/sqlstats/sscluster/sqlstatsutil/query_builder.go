// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstatsutil

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// TODO(azhng): wip: docs: output of this file
// These two queries are build during the initialization time and should not be
// modified during runtime.
var (
	StmtStatsInsertQuery string
	TxnStatsInsertQuery  string
)

func init() {
	StmtStatsInsertQuery = buildStmtInsertQuery()
	TxnStatsInsertQuery = buildTxnInsertQuery()
}

const (
	stmtStatsTable = "system.statement_statistics"

	txnStatsTable = "system.transaction_statistics"

	excludedTable = "excluded"

	// We need to include the computed hash column here for the UPSERT conflict
	// clause.
	stmtStatsConflictClause = "(crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_plan_hash_shard_8, aggregated_ts, fingerprint_id, app_name, plan_hash, node_id)"
	txnStatsConflictClause  = "(crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_shard_8, aggregated_ts, fingerprint_id, app_name, node_id)"

	stmtStatsPlaceholder = "($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	txnStatsPlaceholder  = "($1, $2, $3, $4, $5, $6, $7, $8)"

	// [1]: table name
	// [2]: statistics attribute
	// [3]: execution_statistics attribute
	// [4]: insert placeholders
	// [5]: primary keys
	statsInsertQueryTemplate = `
INSERT INTO %[1]s
VALUES %[4]s
ON CONFLICT %[5]s
DO UPDATE SET (count, statistics) = (SELECT
    (excluded.count + %[1]s.count) AS count,
    jsonb_build_object(
      'statistics', jsonb_build_object(
        %[2]s
      ),
    	'execution_statistics', jsonb_build_object( 
        %[3]s
      )
    ) AS statistics
  )`

	// [1]: table name
	// [2]: oneof(statistics, execution_statistics)
	// [3]: attribute name
	// [4]: oneof(INT, FLOAT)
	jsonbDataPathSQLTemplate = `(%[1]s.statistics -> '%[2]s' ->> '%[3]s')::%[4]s`

	// [1]: scalar expr
	// [2]: scalar expr
	jsonbScalarBinaryMaxAggSQLTemplate = `greatest(%[1]s, %[2]s)`

	// [1]: scalar expr
	// [2]: scalar expr
	jsonbIntSumAggSQLTemplate = `%[1]s + %[2]s`

	// [1]: attribute name
	// [2]: agg expr
	jsonbAggSQLTemplate = `
'%[1]s', %[2]s
`

	// [1]: table name
	jsonbStatsCountExprTemplate = `
%[1]s.count`

	// [1]: table name
	jsonbExecStatsCountExprTemplate = `
(%[1]s.statistics -> 'execution_statistics' ->> 'cnt')::INT`

	//  a different count, and it sets the count to 1 if both counts are 0.
	// This implements the NumericStats.Add() method in SQL.
	// [1]: attribute
	// [2]: table name
	// [3]: oneof(statistics, execution_statistics)
	// [4]: existing `count` expr
	// [5]: excluded `count` expr
	jsonbNumericStatsAggSQLTemplate = `
'%[1]s', jsonb_build_object(
  'mean',
    (
      (%[2]s.statistics -> '%[3]s' -> '%[1]s' ->> 'mean')::FLOAT * %[4]s::FLOAT
     + (excluded.statistics -> '%[3]s' -> '%[1]s' ->> 'mean')::FLOAT * %[5]s::FLOAT
    )
     / greatest((%[4]s + %[5]s)::FLOAT, 1),
  'sqDiff',
    (%[2]s.statistics -> '%[3]s' -> '%[1]s' ->> 'sqDiff')::FLOAT
    + (excluded.statistics -> '%[3]s' -> '%[1]s' ->> 'sqDiff')::FLOAT
    + ((%[4]s::FLOAT
         * %[5]s::FLOAT)
         * POWER((%[2]s.statistics -> '%[3]s' -> '%[1]s' ->> 'mean')::FLOAT
                 - (excluded.statistics -> '%[3]s' -> '%[1]s' ->> 'mean')::FLOAT, 2))
       / greatest((%[4]s + %[5]s)::FLOAT, 1)
  )`
)

type aggType int8

const (
	binOpAgg aggType = iota
	binaryMaxOpAgg
	dateMaxOpAgg
)

type attrMetadata struct {
	name           string
	isNumericStats bool

	isExecStats bool
	aggTyp      aggType
}

var (
	stmtStatsMetadata = []attrMetadata{
		{
			name:           "firstAttemptCnt",
			isNumericStats: false,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "maxRetries",
			isNumericStats: false,
			isExecStats:    false,
			aggTyp:         binaryMaxOpAgg,
		},
		{
			name:           "lastExecAt",
			isNumericStats: false,
			isExecStats:    false,
			aggTyp:         dateMaxOpAgg,
		},
		{
			name:           "numRows",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "parseLat",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "planLat",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "runLat",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "svcLat",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "ovhLat",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "bytesRead",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "rowsRead",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
	}

	txnStatsMetadata = []attrMetadata{
		{
			name:           "maxRetries",
			isNumericStats: false,
			isExecStats:    false,
			aggTyp:         binaryMaxOpAgg,
		},
		{
			name:           "numRows",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "svcLat",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "retryLat",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "commitLat",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "bytesRead",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
		{
			name:           "rowsRead",
			isNumericStats: true,
			isExecStats:    false,
			aggTyp:         binOpAgg,
		},
	}

	execStatsMetadata = []attrMetadata{
		{
			name:           "cnt",
			isNumericStats: false,
			isExecStats:    true,
			aggTyp:         binOpAgg,
		},
		{
			name:           "networkBytes",
			isNumericStats: true,
			isExecStats:    true,
			aggTyp:         binOpAgg,
		},
		{
			name:           "maxMemUsage",
			isNumericStats: true,
			isExecStats:    true,
			aggTyp:         binOpAgg,
		},
		{
			name:           "contentionTime",
			isNumericStats: true,
			isExecStats:    true,
			aggTyp:         binOpAgg,
		},
		{
			name:           "networkMsgs",
			isNumericStats: true,
			isExecStats:    true,
			aggTyp:         binOpAgg,
		},
		{
			name:           "maxDiskUsage",
			isNumericStats: true,
			isExecStats:    true,
			aggTyp:         binOpAgg,
		},
	}
)

func buildStmtInsertQuery() string {
	stmtStatsMergeClause := buildStmtStatsMergeClause()
	execStatsMergeClause := buildExecStatsMergeClause(stmtStatsTable)

	return fmt.Sprintf(statsInsertQueryTemplate, stmtStatsTable, stmtStatsMergeClause,
		execStatsMergeClause, stmtStatsPlaceholder, stmtStatsConflictClause)
}

func buildTxnInsertQuery() string {
	txnStatsMergeClause := buildTxnStatsMergeClause()
	execStatsMergeClause := buildExecStatsMergeClause(txnStatsTable)

	return fmt.Sprintf(statsInsertQueryTemplate, txnStatsTable, txnStatsMergeClause,
		execStatsMergeClause, txnStatsPlaceholder, txnStatsConflictClause)
}

func buildTxnStatsMergeClause() string {
	return buildJSONBMergeClause(txnStatsTable, false /* isExecStats */, txnStatsMetadata)
}

func buildStmtStatsMergeClause() string {
	return buildJSONBMergeClause(stmtStatsTable, false /* isExecStats */, stmtStatsMetadata)
}

func buildExecStatsMergeClause(tableName string) string {
	return buildJSONBMergeClause(tableName, true /* isExecStats */, execStatsMetadata)
}

func buildJSONBMergeClause(tableName string, isExecStats bool, metadataList []attrMetadata) string {
	var buf bytes.Buffer
	builder := util.MakeStringListBuilder("", ",", "")

	for _, attr := range metadataList {
		if attr.isNumericStats {
			builder.Add(&buf, buildNumericStatsMergeQuery(tableName, isExecStats, attr.name))
		} else {
			builder.Add(&buf, buildScalarMergeQuery(tableName, isExecStats, attr.name, attr.aggTyp))
		}
	}

	builder.Finish(&buf)
	return buf.String()
}

func buildScalarMergeQuery(
	tableName string, isExecStats bool, attributeName string, op aggType,
) string {
	field := "statistics"
	if isExecStats {
		field = "execution_statistics"
	}

	var castType string
	var aggTemplate string
	switch op {
	case binOpAgg:
		aggTemplate = jsonbIntSumAggSQLTemplate
		castType = "INT"
	case binaryMaxOpAgg:
		aggTemplate = jsonbScalarBinaryMaxAggSQLTemplate
		castType = "INT"
	case dateMaxOpAgg:
		aggTemplate = jsonbScalarBinaryMaxAggSQLTemplate
		castType = "TIMESTAMPTZ"
	default:
		panic(errors.AssertionFailedf("unhandled aggregation type"))
	}

	existingDataPath := fmt.Sprintf(jsonbDataPathSQLTemplate, tableName, field, attributeName, castType)
	excludedDataPath := fmt.Sprintf(jsonbDataPathSQLTemplate, excludedTable, field, attributeName, castType)

	aggExpr := fmt.Sprintf(aggTemplate, existingDataPath, excludedDataPath)
	return fmt.Sprintf(jsonbAggSQLTemplate, attributeName, aggExpr)
}

func buildNumericStatsMergeQuery(tableName string, isExecStats bool, attribute string) string {
	field := "statistics"
	countTemplate := jsonbStatsCountExprTemplate
	if isExecStats {
		field = "execution_statistics"
		countTemplate = jsonbExecStatsCountExprTemplate
	}

	existingCountExpr := fmt.Sprintf(countTemplate, tableName)
	excludedCountExpr := fmt.Sprintf(countTemplate, excludedTable)

	return fmt.Sprintf(jsonbNumericStatsAggSQLTemplate, attribute, tableName, field, existingCountExpr, excludedCountExpr)
}
