// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// buildLimit adds Limit and Offset and Step operators according to the Limit clause.
//
// parentScope is the scope for the LIMIT/OFFSET expressions; this is not the
// same as inScope, because statements like:
//   SELECT k FROM kv LIMIT k
// are not valid.
func (b *Builder) buildLimit(limit *tree.Limit, parentScope, inScope *scope) {
	if limit.Offset != nil {
		input := inScope.expr.(memo.RelExpr)
		offset := b.resolveAndBuildScalar(
			limit.Offset, types.Int, "OFFSET", tree.RejectSpecial, parentScope,
		)
		inScope.expr = b.factory.ConstructOffset(input, offset, inScope.makeOrderingChoice())
	}
	if limit.Count != nil {
		input := inScope.expr.(memo.RelExpr)
		limit := b.resolveAndBuildScalar(
			limit.Count, types.Int, "LIMIT", tree.RejectSpecial, parentScope,
		)
		inScope.expr = b.factory.ConstructLimit(input, limit, inScope.makeOrderingChoice())
	}
	if limit.Step != nil {
		input := inScope.expr.(memo.RelExpr)
		step := b.resolveAndBuildScalar(
			limit.Step, types.Int, "STEP", tree.RejectSpecial, parentScope,
		)
		inScope.expr = b.factory.ConstructStep(input, step, inScope.makeOrderingChoice())
		//inScope.expr = b.factory.ConstructLimit(input, step, inScope.makeOrderingChoice())
	}
}
