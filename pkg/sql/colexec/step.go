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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
)

type stepOp struct {
	OneInputNode

	step uint64

	seen uint64
}

var _ Operator = &stepOp{}

// NewStepOp returns a new step operator with the given step.
func NewStepOp(input Operator, step uint64) Operator {
	c := &stepOp{
		OneInputNode: NewOneInputNode(input),
		step:         step,
		seen:         0,
	}
	return c
}

func (c *stepOp) Init() {
	c.input.Init()
}

func (c *stepOp) Next(ctx context.Context) coldata.Batch {
	var bat coldata.Batch
	for { // fwaq. ...
		bat = c.input.Next(ctx)
		length := bat.Length()
		if length == 0 {
			return bat
		}

		sel := bat.Selection()

		if sel == nil {
			bat.SetSelection(true)
			sel = bat.Selection()
			for i := uint16(0); i < length; i++ {
				sel[i] = i
			}
		}
		var filteredSel []uint16

		fmt.Println("pre", length, sel[:length], "c.seen:", c.seen, "c.step:", c.step)

		for i := uint16(0); i < length; i++ {
			if (uint64(i)+c.seen)%c.step == 0 {
				filteredSel = append(filteredSel, sel[i])
			}
		}
		c.seen += uint64(length)

		bat.SetLength(uint16(len(filteredSel)))
		copy(sel, filteredSel)

		fmt.Println("post", len(filteredSel), filteredSel, "c.seen:", c.seen, "c.step:", c.step)

		if len(filteredSel) != 0 {
			break
		}
	}

	return bat
}
