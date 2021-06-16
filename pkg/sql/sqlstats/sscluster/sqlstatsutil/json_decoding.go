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
	"encoding/hex"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

func UnmarshalTxnStatsJSON(
	stats json.JSON, metadata json.JSON, result *roachpb.CollectedTransactionStatistics,
) error {
	err := UnmarshalTxnStatsStatisticsJSON(stats, result)
	if err != nil {
		return errors.Errorf("failed to unmarshal statement statistics: %s", err)
	}

	err = UnmarshalTxnStatsMetadataJSON(metadata, result)
	if err != nil {
		return errors.Errorf("failed to unmarshal statement statistics: %s", err)
	}
	return nil
}

func UnmarshalTxnStatsMetadataJSON(
	metadata json.JSON, result *roachpb.CollectedTransactionStatistics,
) error {
	field, err := metadata.FetchValKey("stmtFingerprintIDs")
	if err != nil {
		return errors.Errorf("unable to retrieve statement fingerprint IDs: %s", err)
	}

	// TODO(azhng): wip: package level error
	arrLen := field.Len()
	for i := 0; i < arrLen; i++ {
		stmtFingerprintIDJSON, err := field.FetchValIdx(i)
		if err != nil {
			return errors.Errorf("unable to retrieve statement fingerprint ID: %s", err)
		}
		stmtFingerprintIDEncoded, err := stmtFingerprintIDJSON.AsText()
		if err != nil {
			return errors.Errorf("unable to unmarshal statement fingerprint ID: %s", err)
		}

		stmtFingerprintIDDecoded, err := hex.DecodeString(*stmtFingerprintIDEncoded)
		if err != nil {
			return errors.Errorf("unable to decode statement fingerprint ID: %s", err)
		}

		_, stmtFingerprintID, err := encoding.DecodeUint64Ascending(stmtFingerprintIDDecoded)
		if err != nil {
			return errors.Errorf("unable to decode statement fingerprint ID: %s", err)
		}

		result.StatementFingerprintIDs = append(result.StatementFingerprintIDs, roachpb.StmtFingerprintID(stmtFingerprintID))
	}

	return nil
}

func UnmarshalTxnStatsStatisticsJSON(
	jsonVal json.JSON, result *roachpb.CollectedTransactionStatistics,
) error {
	stats, err := jsonVal.FetchValKey("statistics")
	if err != nil {
		return err
	}
	err = unmarshalTxnStatsStatisticsField(stats, &result.Stats)
	if err != nil {
		return errors.Errorf("failed to unmarshal statistics field: %s", err)
	}

	execStats, err := jsonVal.FetchValKey("execution_statistics")
	if err != nil {
		return err
	}

	err = unmarshalExecStats(execStats, &result.Stats.ExecStats)
	if err != nil {
		return errors.Errorf("failed to unmarshal execution_statistics field: %s", err)
	}

	return nil
}

func unmarshalTxnStatsStatisticsField(
	stats json.JSON, result *roachpb.TransactionStatistics,
) error {
	err := unmarshalInt64(stats, "maxRetries", &result.MaxRetries)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "numRows", &result.NumRows)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "svcLat", &result.ServiceLat)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "retryLat", &result.RetryLat)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "commitLat", &result.CommitLat)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "bytesRead", &result.BytesRead)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "rowsRead", &result.RowsRead)
	if err != nil {
		return err
	}

	return nil
}

// UnmarshalStmtStatsJSON populates the result with the stats and metadata JSON
// fields. This function does not populate the fields in the result that is not
// present in the JSON payload.
func UnmarshalStmtStatsJSON(
	stats json.JSON, metadata json.JSON, result *roachpb.CollectedStatementStatistics,
) error {
	err := UnmarshalStmtStatsStatisticsJSON(stats, &result.Stats)
	if err != nil {
		return errors.Errorf("failed to unmarshal statement statistics: %s", err)
	}

	err = UnmarshalStmtStatsMetadataJSON(metadata, &result.Key, &result.Stats)
	if err != nil {
		return errors.Errorf("failed to unmarshal statement statistics: %s", err)
	}
	return nil
}

func UnmarshalStmtStatsMetadataJSON(
	metadata json.JSON,
	resultKey *roachpb.StatementStatisticsKey,
	result *roachpb.StatementStatistics,
) error {
	err := unmarshalString(metadata, "stmtTyp", &result.SQLType)
	if err != nil {
		return err
	}

	err = unmarshalString(metadata, "query", &resultKey.Query)
	if err != nil {
		return err
	}

	err = unmarshalString(metadata, "db", &resultKey.Database)
	if err != nil {
		return err
	}

	err = unmarshalBool(metadata, "distsql", &resultKey.DistSQL)
	if err != nil {
		return err
	}

	err = unmarshalBool(metadata, "failed", &resultKey.Failed)
	if err != nil {
		return err
	}

	err = unmarshalBool(metadata, "opt", &resultKey.Opt)
	if err != nil {
		return err
	}

	err = unmarshalBool(metadata, "implicitTxn", &resultKey.ImplicitTxn)
	if err != nil {
		return err
	}

	err = unmarshalBool(metadata, "vec", &resultKey.Vec)
	if err != nil {
		return err
	}

	err = unmarshalBool(metadata, "fullScan", &resultKey.FullScan)
	if err != nil {
		return err
	}

	return nil
}

// UnmarshalStmtStatsStatisticsJSON unmarshalls the 'statistics' field and the
// 'execution_statistics' field in the given json into
// roachpb.StatementStatistics.
func UnmarshalStmtStatsStatisticsJSON(
	jsonVal json.JSON, result *roachpb.StatementStatistics,
) error {
	stats, err := jsonVal.FetchValKey("statistics")
	if err != nil {
		return err
	}
	err = unmarshalStmtStatsStatisticsField(stats, result)
	if err != nil {
		return errors.Errorf("failed to unmarshal statistics field: %s", err)
	}

	execStats, err := jsonVal.FetchValKey("execution_statistics")
	if err != nil {
		return err
	}

	err = unmarshalExecStats(execStats, &result.ExecStats)
	if err != nil {
		return errors.Errorf("failed to unmarshal execution_statistics field: %s", err)
	}

	return nil
}

// unmarshalStmtStatsStatisticsField unmarshalls the 'statistics' field of the
// given json into result.
func unmarshalStmtStatsStatisticsField(stats json.JSON, result *roachpb.StatementStatistics) error {
	err := unmarshalInt64(stats, "firstAttemptCnt", &result.FirstAttemptCount)
	if err != nil {
		return err
	}

	err = unmarshalInt64(stats, "maxRetries", &result.MaxRetries)
	if err != nil {
		return err
	}

	var lastExecTimestamp string
	err = unmarshalString(stats, "lastExecAt", &lastExecTimestamp)
	if err != nil {
		return err
	}

	err = result.LastExecTimestamp.UnmarshalText([]byte(lastExecTimestamp))
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "numRows", &result.NumRows)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "parseLat", &result.ParseLat)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "planLat", &result.PlanLat)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "runLat", &result.RunLat)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "svcLat", &result.ServiceLat)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "ovhLat", &result.OverheadLat)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "bytesRead", &result.BytesRead)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(stats, "rowsRead", &result.RowsRead)
	if err != nil {
		return err
	}

	return nil
}

// unmarshalStmtStatsStatisticsField unmarshalls the 'execution_statistics'
// field of the given json into result.
func unmarshalExecStats(jsonVal json.JSON, result *roachpb.ExecStats) error {
	err := unmarshalInt64(jsonVal, "cnt", &result.Count)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(jsonVal, "networkBytes", &result.NetworkBytes)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(jsonVal, "maxMemUsage", &result.MaxMemUsage)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(jsonVal, "contentionTime", &result.ContentionTime)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(jsonVal, "networkMsgs", &result.NetworkMessages)
	if err != nil {
		return err
	}

	err = unmarshalNumericStats(jsonVal, "maxDiskUsage", &result.MaxDiskUsage)
	if err != nil {
		return err
	}

	return nil
}

func unmarshalNumericStats(jsonVal json.JSON, key string, result *roachpb.NumericStat) error {
	field, err := jsonVal.FetchValKey(key)
	if err != nil {
		return err
	}

	if field == nil {
		return errors.Newf("requested field %s is missing", key)
	}

	err = unmarshalFloat64(field, "mean", &result.Mean)
	if err != nil {
		return err
	}

	err = unmarshalFloat64(field, "sqDiff", &result.SquaredDiffs)
	if err != nil {
		return err
	}

	return nil
}

func unmarshalBool(jsonVal json.JSON, key string, res *bool) error {
	field, err := jsonVal.FetchValKey(key)
	if err != nil {
		return err
	}

	if field == json.TrueJSONValue {
		*res = true
		return nil
	}

	*res = false
	return nil
}

func unmarshalString(jsonVal json.JSON, key string, result *string) error {
	field, err := jsonVal.FetchValKey(key)
	if err != nil {
		return err
	}

	if field == nil {
		return errors.Newf("requested field %s is missing", key)
	}

	text, err := field.AsText()
	if err != nil {
		return err
	}

	*result = *text
	return nil
}

func unmarshalInt64(jsonVal json.JSON, key string, result *int64) error {
	decimal, err := unmarshalDecimal(jsonVal, key)
	if err != nil {
		return errors.New("unable to decode float64")
	}

	i, err := decimal.Int64()
	if err != nil {
		return err
	}
	*result = i
	return nil
}

func unmarshalFloat64(jsonVal json.JSON, key string, result *float64) error {
	decimal, err := unmarshalDecimal(jsonVal, key)
	if err != nil {
		return errors.New("unable to decode float64")
	}

	f, err := decimal.Float64()
	if err != nil {
		return err
	}
	*result = f
	return nil
}

func unmarshalDecimal(jsonVal json.JSON, key string) (*apd.Decimal, error) {
	field, err := jsonVal.FetchValKey(key)
	if err != nil {
		return nil, err
	}

	if field == nil {
		return nil, errors.Newf("requested field %s is missing", key)
	}

	decimal, ok := field.AsDecimal()
	if !ok {
		return nil, errors.New("unable to decode float64")
	}

	return decimal, nil
}
