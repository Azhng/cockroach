// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// aggregateFunc is an aggregate function that performs computation on a batch
// when Compute(batch) is called and writes the output to the Vec passed in
// in Init. The aggregateFunc performs an aggregation per group and outputs the
// aggregation once the end of the group is reached. If the end of the group is
// not reached before the batch is finished, the aggregateFunc will store a
// carry value that it will use next time Compute is called. Note that this
// carry value is stored at the output index. Therefore if any memory
// modification of the output vector is made, the caller *MUST* copy the value
// at the current index inclusive for a correct aggregation.
type aggregateFunc interface {

	// Init sets the groups for the aggregation and the output vector. Each index
	// in groups corresponds to a column value in the input batch. true represents
	// the first value of a new group.
	Init(groups []bool, vec coldata.Vec)

	// Reset resets the aggregate function for another run. Primarily used for
	// benchmarks.
	Reset()

	// CurrentOutputIndex returns the current index in the output vector that the
	// aggregate function is writing to. All indices < the index returned are
	// finished aggregations for previous groups. A negative index may be returned
	// to signify an aggregate function that has not yet performed any
	// computation.
	CurrentOutputIndex() int
	// SetOutputIndex sets the output index to write to. The value for the current
	// index is carried over. Note that calling SetOutputIndex is a noop if
	// CurrentOutputIndex returns a negative value (i.e. the aggregate function
	// has not yet performed any computation). This method also has the side
	// effect of clearing the NULLs bitmap of the output buffer past the given
	// index.
	SetOutputIndex(idx int)

	// Compute computes the aggregation on the input batch. A zero-length input
	// batch tells the aggregate function that it should flush its results.
	Compute(batch coldata.Batch, inputIdxs []uint32)

	// TODO(@azhng): merge two interfaces, for testing purposes
	Compute2(batch coldata.Batch, inputIdxs []uint32, start, end uint16)

	// TODO(azhng): finalize the aggregation value and write to
	//              the output vector at outputIdx
	Finalize(output coldata.Vec, outputIdx uint16)

	// HandleEmptyInputScalar populates the output for a case of an empty input
	// when the aggregate function is in scalar context. The output must always
	// be a single value (either null or zero, depending on the function).
	// TODO(yuzefovich): we can pull scratch field of aggregates into a shared
	// aggregator and implement this method once on the shared base.
	HandleEmptyInputScalar()
}

// SupportedAggFns contains all aggregate functions supported by the vectorized
// engine.
var SupportedAggFns = []execinfrapb.AggregatorSpec_Func{
	execinfrapb.AggregatorSpec_ANY_NOT_NULL,
	execinfrapb.AggregatorSpec_AVG,
	execinfrapb.AggregatorSpec_SUM,
	execinfrapb.AggregatorSpec_SUM_INT,
	execinfrapb.AggregatorSpec_COUNT_ROWS,
	execinfrapb.AggregatorSpec_COUNT,
	execinfrapb.AggregatorSpec_MIN,
	execinfrapb.AggregatorSpec_MAX,
	execinfrapb.AggregatorSpec_BOOL_AND,
	execinfrapb.AggregatorSpec_BOOL_OR,
}

func makeAggregateFuncs(
	allocator *Allocator, aggTyps [][]coltypes.T, aggFns []execinfrapb.AggregatorSpec_Func,
) ([]aggregateFunc, []coltypes.T, error) {
	funcs := make([]aggregateFunc, len(aggFns))
	outTyps := make([]coltypes.T, len(aggFns))

	for i := range aggFns {
		var err error
		switch aggFns[i] {
		case execinfrapb.AggregatorSpec_ANY_NOT_NULL:
			funcs[i], err = newAnyNotNullAgg(allocator, aggTyps[i][0])
		case execinfrapb.AggregatorSpec_AVG:
			funcs[i], err = newAvgAgg(aggTyps[i][0])
		case execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_SUM_INT:
			funcs[i], err = newSumAgg(aggTyps[i][0])
		case execinfrapb.AggregatorSpec_COUNT_ROWS:
			funcs[i] = newCountRowAgg()
		case execinfrapb.AggregatorSpec_COUNT:
			funcs[i] = newCountAgg()
		case execinfrapb.AggregatorSpec_MIN:
			funcs[i], err = newMinAgg(allocator, aggTyps[i][0])
		case execinfrapb.AggregatorSpec_MAX:
			funcs[i], err = newMaxAgg(allocator, aggTyps[i][0])
		case execinfrapb.AggregatorSpec_BOOL_AND:
			funcs[i] = newBoolAndAgg()
		case execinfrapb.AggregatorSpec_BOOL_OR:
			funcs[i] = newBoolOrAgg()
		default:
			return nil, nil, errors.Errorf("unsupported columnar aggregate function %s", aggFns[i].String())
		}

		// Set the output type of the aggregate.
		switch aggFns[i] {
		case execinfrapb.AggregatorSpec_COUNT_ROWS, execinfrapb.AggregatorSpec_COUNT:
			// TODO(jordan): this is a somewhat of a hack. The aggregate functions
			// should come with their own output types, somehow.
			outTyps[i] = coltypes.Int64
		default:
			// Output types are the input types for now.
			outTyps[i] = aggTyps[i][0]
		}

		if err != nil {
			return nil, nil, err
		}
	}

	return funcs, outTyps, nil
}

// isAggregateSupported returns whether the aggregate function that operates on
// columns of types 'inputTypes' (which can be empty in case of COUNT_ROWS) is
// supported.
func isAggregateSupported(
	aggFn execinfrapb.AggregatorSpec_Func, inputTypes []types.T,
) (bool, error) {
	aggTypes, err := typeconv.FromColumnTypes(inputTypes)
	if err != nil {
		return false, err
	}
	switch aggFn {
	case execinfrapb.AggregatorSpec_SUM:
		switch inputTypes[0].Family() {
		case types.IntFamily:
			// TODO(alfonso): plan ordinary SUM on integer types by casting to DECIMAL
			// at the end, mod issues with overflow. Perhaps to avoid the overflow
			// issues, at first, we could plan SUM for all types besides Int64.
			return false, errors.Newf("sum on int cols not supported (use sum_int)")
		}
	case execinfrapb.AggregatorSpec_SUM_INT:
		// TODO(yuzefovich): support this case through vectorize.
		if inputTypes[0].Width() != 64 {
			return false, errors.Newf("sum_int is only supported on Int64 through vectorized")
		}
	}
	_, outputTypes, err := makeAggregateFuncs(
		nil, /* allocator */
		[][]coltypes.T{aggTypes},
		[]execinfrapb.AggregatorSpec_Func{aggFn},
	)
	if err != nil {
		return false, err
	}
	_, retType, err := execinfrapb.GetAggregateInfo(aggFn, inputTypes...)
	if err != nil {
		return false, err
	}
	// The columnar aggregates will return the same physical output type as their
	// input. However, our current builtin resolution might say that the return
	// type is the canonical for the family (for example, MAX on INT4 is said to
	// return INT8), so we explicitly check whether the type the columnar
	// aggregate returns and the type the planning code will expect it to return
	// are the same. If they are not, we fallback to row-by-row engine.
	if typeconv.FromColumnType(retType) != outputTypes[0] {
		// TODO(yuzefovich): support this case through vectorize. Probably it needs
		// to be done at the same time as #38845.
		return false, errors.Newf("aggregates with different input and output types are not supported")
	}
	return true, nil
}
