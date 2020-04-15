// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execgen

import "github.com/cockroachdb/cockroach/pkg/sql/colbase/vecerror"

const nonTemplatePanic = "do not call from non-template code"

// Remove unused warnings.
var (
	_ = UNSAFEGET
	_ = COPYVAL
	_ = SET
	_ = SLICE
	_ = COPYSLICE
	_ = APPENDSLICE
	_ = APPENDVAL
	_ = LEN
	_ = ZERO
	_ = RANGE
	_ = WINDOW
)

// UNSAFEGET is a template function. Use this if you are not keeping data around
// (including passing it to SET).
func UNSAFEGET(target, i interface{}) interface{} {
	vecerror.InternalError(nonTemplatePanic)
	return nil
}

// COPYVAL is a template function that can be used to set a scalar to the value
// of another scalar in such a way that the destination won't be modified if the
// source is. You must use this on the result of UNSAFEGET if you wish to store
// that result past the lifetime of the batch you UNSAFEGET'd from.
func COPYVAL(dest, src interface{}) {
	vecerror.InternalError(nonTemplatePanic)
}

// SET is a template function.
func SET(target, i, new interface{}) {
	vecerror.InternalError(nonTemplatePanic)
}

// SLICE is a template function.
func SLICE(target, start, end interface{}) interface{} {
	vecerror.InternalError(nonTemplatePanic)
	return nil
}

// COPYSLICE is a template function.
func COPYSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	vecerror.InternalError(nonTemplatePanic)
}

// APPENDSLICE is a template function.
func APPENDSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	vecerror.InternalError(nonTemplatePanic)
}

// APPENDVAL is a template function.
func APPENDVAL(target, v interface{}) {
	vecerror.InternalError(nonTemplatePanic)
}

// LEN is a template function.
func LEN(target interface{}) interface{} {
	vecerror.InternalError(nonTemplatePanic)
	return nil
}

// ZERO is a template function.
func ZERO(target interface{}) {
	vecerror.InternalError(nonTemplatePanic)
}

// RANGE is a template function.
func RANGE(loopVariableIdent, target, start, end interface{}) bool {
	vecerror.InternalError(nonTemplatePanic)
	return false
}

// WINDOW is a template function.
func WINDOW(target, start, end interface{}) interface{} {
	vecerror.InternalError(nonTemplatePanic)
	return nil
}
