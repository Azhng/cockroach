// Copyright 2018 The Cockroach Authors.
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
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestStep(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		step     uint64
		tuples   []tuple
		expected []tuple
	}{
		{
			step:     1,
			tuples:   tuples{{1}},
			expected: tuples{{1}},
		},
		{
			step:     2,
			tuples:   tuples{{1}},
			expected: tuples{{1}},
		},
		{
			step:     100000,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}},
		},
		{
			step:     2,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}, {3}},
		},
		{
			step:     1,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}, {2}, {3}, {4}},
		},
		{
			step:     3,
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}},
			expected: tuples{{1}, {4}},
		},
		{
			step:     3,
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected: tuples{{1}, {4}, {7}},
		},
		{
			step:     3,
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}},
			expected: tuples{{1}, {4}, {7}},
		},
		{
			step:     4,
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}},
			expected: tuples{{1}, {5}},
		},
	}

	for _, tc := range tcs {
		// The tuples consisting of all nulls still count as separate rows, so if
		// we replace all values with nulls, we should get the same output.
		runTestsWithoutAllNullsInjection(t, []tuples{tc.tuples}, nil /* typs */, tc.expected, orderedVerifier, func(input []Operator) (Operator, error) {
			return NewStepOp(input[0], tc.step), nil
		})
	}
}

func BenchmarkStep(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64})
	aCol := batch.ColVec(1).Int64()
	bCol := batch.ColVec(2).Int64()
	lastA := int64(0)
	lastB := int64(0)
	for i := 0; i < int(coldata.BatchSize()); i++ {
		// 1/4 chance of changing each distinct coldata.
		if rng.Float64() > 0.75 {
			lastA++
		}
		if rng.Float64() > 0.75 {
			lastB++
		}
		aCol[i] = lastA
		bCol[i] = lastB
	}
	batch.SetLength(coldata.BatchSize())
	source := NewRepeatableBatchSource(batch)
	source.Init()

	step := NewStepOp(source, 2)

	// don't count the artificial zeroOp'd column in the throughput
	for _, nulls := range []bool{false, true} {
		b.Run(fmt.Sprintf("nulls=%t", nulls), func(b *testing.B) {
			if nulls {
				n := coldata.NewNulls(int(coldata.BatchSize()))
				// Setting one value to null is enough to trigger the null handling
				// logic for the entire batch.
				n.SetNull(0)
				batch.ColVec(1).SetNulls(&n)
				batch.ColVec(2).SetNulls(&n)
			}
			b.SetBytes(int64(8 * coldata.BatchSize() * 3))
			for i := 0; i < b.N; i++ {
				step.Next(ctx)
			}
		})
	}
}
