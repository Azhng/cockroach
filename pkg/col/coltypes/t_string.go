// Code generated by "stringer -type=T"; DO NOT EDIT.

package coltypes

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Bool-0]
	_ = x[Bytes-1]
	_ = x[Decimal-2]
	_ = x[Int16-3]
	_ = x[Int32-4]
	_ = x[Int64-5]
	_ = x[Float64-6]
	_ = x[Timestamp-7]
	_ = x[Interval-8]
	_ = x[Datum-9]
	_ = x[Unhandled-10]
}

const _T_name = "BoolBytesDecimalInt16Int32Int64Float64TimestampIntervalDatumUnhandled"

var _T_index = [...]uint8{0, 4, 9, 16, 21, 26, 31, 38, 47, 55, 60, 69}

func (i T) String() string {
	if i < 0 || i >= T(len(_T_index)-1) {
		return "T(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _T_name[_T_index[i]:_T_index[i+1]]
}
