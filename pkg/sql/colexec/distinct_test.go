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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		distinctCols            []uint32
		colTypes                []coltypes.T
		tuples                  []tuple
		expected                []tuple
		isOrderedOnDistinctCols bool
	}{
		{
			distinctCols: []uint32{0, 1, 2},
			colTypes:     []coltypes.T{coltypes.Float64, coltypes.Int64, coltypes.Bytes, coltypes.Int64},
			tuples: tuples{
				{nil, nil, nil, nil},
				{nil, nil, nil, nil},
				{nil, nil, "30", nil},
				{1.0, 2, "30", 4},
				{1.0, 2, "30", 4},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
				{2.0, 3, "40", 4},
			},
			expected: tuples{
				{nil, nil, nil, nil},
				{nil, nil, "30", nil},
				{1.0, 2, "30", 4},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
			},
			isOrderedOnDistinctCols: true,
		},
		{
			distinctCols: []uint32{1, 0, 2},
			colTypes:     []coltypes.T{coltypes.Float64, coltypes.Int64, coltypes.Bytes, coltypes.Int64},
			tuples: tuples{
				{nil, nil, nil, nil},
				{nil, nil, nil, nil},
				{nil, nil, "30", nil},
				{1.0, 2, "30", 4},
				{1.0, 2, "30", 4},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
				{2.0, 3, "40", 4},
			},
			expected: tuples{
				{nil, nil, nil, nil},
				{nil, nil, "30", nil},
				{1.0, 2, "30", 4},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
			},
			isOrderedOnDistinctCols: true,
		},
		{
			distinctCols: []uint32{0, 1, 2},
			colTypes:     []coltypes.T{coltypes.Float64, coltypes.Int64, coltypes.Bytes, coltypes.Int64},
			tuples: tuples{
				{1.0, 2, "30", 4},
				{1.0, 2, "30", 4},
				{nil, nil, nil, nil},
				{nil, nil, nil, nil},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{nil, nil, "30", nil},
				{2.0, 3, "40", 4},
				{2.0, 3, "40", 4},
			},
			expected: tuples{
				{1.0, 2, "30", 4},
				{nil, nil, nil, nil},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{nil, nil, "30", nil},
				{2.0, 3, "40", 4},
			},
		},
		{
			distinctCols: []uint32{0},
			colTypes:     []coltypes.T{coltypes.Int64, coltypes.Bytes},
			tuples: tuples{
				{1, "a"},
				{2, "b"},
				{3, "c"},
				{nil, "d"},
				{5, "e"},
				{6, "f"},
				{1, "1"},
				{2, "2"},
				{3, "3"},
			},
			expected: tuples{
				{1, "a"},
				{2, "b"},
				{3, "c"},
				{nil, "d"},
				{5, "e"},
				{6, "f"},
			},
		},
		{
			// This is to test hashTable deduplication with various batch size
			// boundaries and ensure it always emits the first tuple it encountered.
			distinctCols: []uint32{0},
			colTypes:     []coltypes.T{coltypes.Int64, coltypes.Bytes},
			tuples: tuples{
				{1, "1"},
				{1, "2"},
				{1, "3"},
				{1, "4"},
				{1, "5"},
				{2, "6"},
				{2, "7"},
				{2, "8"},
				{2, "9"},
				{2, "10"},
				{0, "11"},
				{0, "12"},
				{0, "13"},
				{1, "14"},
				{1, "15"},
				{1, "16"},
			},
			expected: tuples{
				{1, "1"},
				{2, "6"},
				{0, "11"},
			},
		},
	}

	for _, tc := range tcs {
		if tc.isOrderedOnDistinctCols {
			t.Run("ordered", func(t *testing.T) {
				runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier,
					func(input []Operator) (Operator, error) {
						return NewOrderedDistinct(input[0], tc.distinctCols, tc.colTypes)
					})
			})
		}
	}
}

func BenchmarkDistinct(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	distinctConstructors := []func(Operator, []uint32, int, []coltypes.T) (Operator, error){
		func(input Operator, distinctCols []uint32, numOrderedCols int, typs []coltypes.T) (Operator, error) {
			return NewOrderedDistinct(input, distinctCols, typs)
		},
	}
	distinctNames := []string{"Ordered"}
	orderedColsFraction := []float64{0, 0.5, 1.0}
	for _, hasNulls := range []bool{false, true} {
		for _, newTupleProbability := range []float64{0.001, 0.01, 0.1} {
			for _, nBatches := range []int{1 << 2, 1 << 6} {
				for _, nCols := range []int{2, 4} {
					typs := make([]coltypes.T, nCols)
					for i := range typs {
						typs[i] = coltypes.Int64
					}
					batch := coldata.NewMemBatch(typs)
					batch.SetLength(coldata.BatchSize())
					distinctCols := []uint32{0, 1, 2, 3}[:nCols]
					// We have the following equation:
					//   newTupleProbability = 1 - (1 - newValueProbability) ^ nCols,
					// so applying some manipulations we get:
					//   newValueProbability = 1 - (1 - newTupleProbability) ^ (1 / nCols).
					newValueProbability := 1.0 - math.Pow(1-newTupleProbability, 1.0/float64(nCols))
					for i := range distinctCols {
						col := batch.ColVec(i).Int64()
						col[0] = 0
						for j := uint16(1); j < coldata.BatchSize(); j++ {
							col[j] = col[j-1]
							if rng.Float64() < newValueProbability {
								col[j]++
							}
						}
						nulls := batch.ColVec(i).Nulls()
						if hasNulls {
							nulls.SetNull(0)
						} else {
							nulls.UnsetNulls()
						}
					}
					distinctIdx := 2
					numOrderedCols := int(float64(nCols) * orderedColsFraction[distinctIdx])
					b.Run(
						fmt.Sprintf("%s/hasNulls=%v/newTupleProbability=%.3f/rows=%d/cols=%d/ordCols=%d",
							distinctNames[0], hasNulls, newTupleProbability,
							nBatches*int(coldata.BatchSize()), nCols, numOrderedCols,
						),
						func(b *testing.B) {
							b.SetBytes(int64(8 * nBatches * int(coldata.BatchSize()) * nCols))
							b.ResetTimer()
							for n := 0; n < b.N; n++ {
								// Note that the source will be ordered on all nCols so that the
								// number of distinct tuples doesn't vary between different
								// distinct operator variations.
								source := newFiniteChunksSource(batch, nBatches, nCols)
								distinct, err := distinctConstructors[0](source, distinctCols, numOrderedCols, typs)
								if err != nil {
									b.Fatal(err)
								}
								distinct.Init()
								for b := distinct.Next(ctx); b.Length() > 0; b = distinct.Next(ctx) {
								}
							}
							b.StopTimer()
						})
				}
			}
		}
	}
}
