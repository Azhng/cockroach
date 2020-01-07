// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// TODO(az@): doc
type stepProcessor struct {
	execinfra.ProcessorBase
	input execinfra.RowSource
	step  int64

	processedRowCnt int64
}

var _ execinfra.Processor = &stepProcessor{}
var _ execinfra.RowSource = &stepProcessor{}

const stepProcName = "step"

func newStepProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	spec *execinfrapb.StepSpec,
) (*stepProcessor, error) {
	n := &stepProcessor{
		input:           input,
		step:            spec.NumOfRows,
		processedRowCnt: int64(0),
	}

	if err := n.Init(
		n,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{n.input}},
	); err != nil {
		return nil, err
	}
	return n, nil
}

// Start is part of the RowSource interface.
func (n *stepProcessor) Start(ctx context.Context) context.Context {
	n.input.Start(ctx)
	return n.StartInternal(ctx, stepProcName)
}

// Next is part of the RowSource interface.
func (n *stepProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for n.State == execinfra.StateRunning {
		row, meta := n.input.Next()

		if meta != nil {
			if meta.Err != nil {
				n.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			n.MoveToDraining(nil /* err */)
			break
		}

		// resets the processed row count
		if n.processedRowCnt == n.step {
			n.processedRowCnt = 0
		}

		if n.processedRowCnt == 0 {
			if outRow := n.ProcessRowHelper(row); outRow != nil {
				n.processedRowCnt++
				return outRow, nil
			}
		}

		if n.processedRowCnt < n.step {
			n.processedRowCnt++
		}
	}
	return nil, n.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (n *stepProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	n.InternalClose()
}
