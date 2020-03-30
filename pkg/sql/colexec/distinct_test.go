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
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

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
					for distinctIdx, distinctConstructor := range distinctConstructors {
						numOrderedCols := int(float64(nCols) * orderedColsFraction[distinctIdx])
						b.Run(
							fmt.Sprintf("%s/hasNulls=%v/newTupleProbability=%.3f/rows=%d/cols=%d/ordCols=%d",
								distinctNames[distinctIdx], hasNulls, newTupleProbability,
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
									distinct, err := distinctConstructor(source, distinctCols, numOrderedCols, typs)
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
}
