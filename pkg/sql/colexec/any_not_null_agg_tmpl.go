// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for any_not_null_agg.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/pkg/errors"
)

func newAnyNotNullAgg(allocator *Allocator, t coltypes.T) (aggregateFunc, error) {
	switch t {
	// {{range .}}
	case _TYPES_T:
		return &anyNotNull_TYPEAgg{allocator: allocator}, nil
		// {{end}}
	default:
		return nil, errors.Errorf("unsupported any not null agg type %s", t)
	}
}

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "duration" package.
var _ duration.Duration

// _GOTYPESLICE is the template Go type slice variable for this operator. It
// will be replaced by the Go slice representation for each type in coltypes.T, for
// example []int64 for coltypes.Int64.
type _GOTYPESLICE interface{}

// _TYPES_T is the template type variable for coltypes.T. It will be replaced by
// coltypes.Foo for each type Foo in the coltypes.T type.
const _TYPES_T = coltypes.Unhandled

// */}}

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.UNSAFEGET

// {{range .}}

// anyNotNull_TYPEAgg implements the ANY_NOT_NULL aggregate, returning the
// first non-null value in the input column.
type anyNotNull_TYPEAgg struct {
	allocator                   *Allocator
	curAgg                      _GOTYPE
	foundNonNullForCurrentGroup bool
}

func (a *anyNotNull_TYPEAgg) Init() {
	a.foundNonNullForCurrentGroup = false
}

func (a *anyNotNull_TYPEAgg) Compute(b coldata.Batch, inputIdxs []uint32, start, end uint16) {
	inputLen := end - start
	// Suppress unused variable error.
	_ = inputLen
	offset := uint16(0)

	vec, sel := b.ColVec(int(inputIdxs[0])), b.Selection()
	col, nulls := vec._TemplateType(), vec.Nulls()

	a.allocator.PerformOperation(
		[]coldata.Vec{vec},
		func() {
			if nulls.MaybeHasNulls() {
				if sel != nil {
					sel = sel[start:end]
					for _, i := range sel {
						_FIND_ANY_NOT_NULL(a, nulls, i, true)
					}
				} else {
					offset = start
					col = execgen.SLICE(col, start, end)
					for execgen.RANGE(i, col, 0, int(inputLen)) {
						_FIND_ANY_NOT_NULL(a, nulls, i, true)
					}
				}
			} else {
				if sel != nil {
					sel = sel[start:end]
					for _, i := range sel {
						_FIND_ANY_NOT_NULL(a, nulls, i, false)
					}
				} else {
					col = execgen.SLICE(col, start, end)
					for execgen.RANGE(i, col, 0, int(inputLen)) {
						_FIND_ANY_NOT_NULL(a, nulls, i, false)
					}
				}
			}
		},
	)
}

func (a *anyNotNull_TYPEAgg) Finalize(output coldata.Vec, outputIdx uint16) {
	if !a.foundNonNullForCurrentGroup {
		output.Nulls().SetNull(uint16(outputIdx))
	} else {
		vec := output._TemplateType()
		execgen.SET(vec, int(outputIdx), a.curAgg)
	}
	a.Init()
}

// TODO(@azhng): fix this
func (a *anyNotNull_TYPEAgg) HandleEmptyInputScalar() {
	//a.nulls.SetNull(0)
}

// {{end}}

// {{/*
// _FIND_ANY_NOT_NULL finds a non-null value for the group that contains the ith
// row. If a non-null value was already found, then it does nothing. If this is
// the first row of a new group, and no non-nulls have been found for the
// current group, then the output for the current group is set to null.
func _FIND_ANY_NOT_NULL(a *anyNotNull_TYPEAgg, nulls *coldata.Nulls, i int, _HAS_NULLS bool) { // */}}
	// {{define "findAnyNotNull" -}}
	var isNull bool
	// {{ if .HasNulls }}
	isNull = nulls.NullAt(offset + uint16(i))
	// {{ else }}
	isNull = false
	// {{ end }}
	if !a.foundNonNullForCurrentGroup && !isNull {
		// If we haven't seen any non-nulls for the current group yet, and the
		// current value is non-null, then we can pick the current value to be the
		// output.
		a.curAgg = execgen.UNSAFEGET(col, int(i))
		a.foundNonNullForCurrentGroup = true
	}
	// {{end}}

	// {{/*
} // */}}
