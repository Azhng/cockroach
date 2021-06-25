// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
)

type statsCollector struct {
	sqlstats.WriterIterator

	// phaseTimes tracks session-level phase times.
	phaseTimes *sessionphase.Times

	// previousPhaseTimes tracks the session-level phase times for the previous
	// query. This enables the `SHOW LAST QUERY STATISTICS` observer statement.
	previousPhaseTimes *sessionphase.Times

	explicit bool

	writeBackSink sqlstats.WriterIterator
}

var _ sqlstats.StatsCollector = &statsCollector{}

// NewStatsCollector returns an instance of sqlstats.StatsCollector.
func NewStatsCollector(
	writer sqlstats.WriterIterator, phaseTime *sessionphase.Times,
) sqlstats.StatsCollector {
	return &statsCollector{
		WriterIterator: writer,
		phaseTimes:     phaseTime.Clone(),
		writeBackSink:  nil,
		explicit:       false,
	}
}

func (s *statsCollector) SetExplicit() {
	s.explicit = true
	s.writeBackSink = s.WriterIterator

	// Detail omitted
	s.WriterIterator = &ssmemstorage.Container{}
}

func (s *statsCollector) Finalize() {
	if s.explicit {
		s.writeBackSink.Merge_todo(s.WriterIterator)
	}
}

// PhaseTimes implements sqlstats.StatsCollector interface.
func (s *statsCollector) PhaseTimes() *sessionphase.Times {
	return s.phaseTimes
}

// PreviousPhaseTimes implements sqlstats.StatsCollector interface.
func (s *statsCollector) PreviousPhaseTimes() *sessionphase.Times {
	return s.previousPhaseTimes
}

// Reset implements sqlstats.StatsCollector interface.
func (s *statsCollector) Reset(writer sqlstats.WriterIterator, phaseTime *sessionphase.Times) {
	previousPhaseTime := s.phaseTimes
	*s = statsCollector{
		WriterIterator:     writer,
		previousPhaseTimes: previousPhaseTime,
		phaseTimes:         phaseTime.Clone(),
	}
}
