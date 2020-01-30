// Copyright 2020 The Cockroach Authors.
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
// This file is the execgen template for bool_and_or_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// */}}
	// HACK: crlfmt removes the "*/}}" comment if it's the last line in the import
	// block. This was picked because it sorts after "pkg/sql/colexec/execerror" and
	// has no deps.
	_ "github.com/cockroachdb/cockroach/pkg/util/bufalloc"
)

// {{/*

// _ASSIGN_BOOL_OP is the template boolean operation function for assigning the
// first input to the result of a boolean operation of the second and the third
// inputs.
func _ASSIGN_BOOL_OP(_, _, _ string) {
	execerror.VectorizedInternalPanic("")
}

// */}}

// {{range .}}

func newBool_OP_TYPEAgg() aggregateFunc {
	return &bool_OP_TYPEAgg{}
}

type bool_OP_TYPEAgg struct {
	sawNonNull bool
	curIdx     int
	curAgg     bool
}

func (b *bool_OP_TYPEAgg) Init() {
	b.sawNonNull = false
	// _DEFAULT_VAL indicates whether we are doing an AND aggregate or OR aggregate.
	// For bool_and the _DEFAULT_VAL is true and for bool_or the _DEFAULT_VAL is false.
	b.curAgg = _DEFAULT_VAL
}

func (b *bool_OP_TYPEAgg) Compute(batch coldata.Batch, inputIdxs []uint32, start, end uint16) {
	offset := uint16(0)

	vec, sel := batch.ColVec(int(inputIdxs[0])), batch.Selection()
	col, nulls := vec.Bool(), vec.Nulls()
	if sel != nil {
		sel = sel[start:end]
		for _, i := range sel {
			_ACCUMULATE_BOOLEAN(b, nulls, i)
		}
	} else {
		col = col[start:end]
		offset = start
		for i := range col {
			_ACCUMULATE_BOOLEAN(b, nulls, i)
		}
	}
}

func (b *bool_OP_TYPEAgg) Finalize(output coldata.Vec, outputIdx uint16) {
	vec, nulls := output.Bool(), output.Nulls()
	if !b.sawNonNull {
		nulls.SetNull(outputIdx)
	} else {
		vec[outputIdx] = b.curAgg
	}
	b.Init()
}

// TODO(@azhng): fix this
func (b *bool_OP_TYPEAgg) HandleEmptyInputScalar() {
	//b.nulls.SetNull(0)
}

// {{end}}

// {{/*
// _ACCUMULATE_BOOLEAN aggregates the boolean value at index i into the boolean aggregate.
func _ACCUMULATE_BOOLEAN(b *bool_OP_TYPEAgg, nulls *coldata.Nulls, i int) { // */}}
	// {{define "accumulateBoolean" -}}
	isNull := nulls.NullAt(offset + uint16(i))
	if !isNull {
		// {{with .Global}}
		_ASSIGN_BOOL_OP(b.curAgg, b.curAgg, col[i])
		// {{end}}
		b.sawNonNull = true
	}

	// {{end}}

	// {{/*
} // */}}
