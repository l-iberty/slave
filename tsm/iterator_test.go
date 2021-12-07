package util

import (
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/query"
)

// 使用 influx_insepct dumptsm 对 testdata 目录下的 8 个 TSM 进行统计得到的 series 和 point 总数
const (
	seriesN = 1634
	pointN  = 15364000
)

func TestTSMIterator_WalkValues(t *testing.T) {
	tsm := NewTSMIterator("testdata")
	var n int

	for tsm.HasNextKey() {
		tsm.NextKey()
		values := tsm.Values()
		n += len(values)
	}

	if n != pointN {
		t.Errorf("number of points: expected %d, got %d", pointN, n)
	}
}

func TestTSMIterator_Stats(t *testing.T) {
	tsm := NewTSMIterator("testdata")
	stats := query.IteratorStats{}

	stats = tsm.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN)
	}

	stats = tsm.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN)
	}

	tsm.Reset()
	stats = tsm.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN)
	}

	tsm.Reset()
	stats = tsm.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN)
	}

	stats = tsm.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN)
	}
}

func TestTSMIterator_WalkPoints(t *testing.T) {
	tsm := NewTSMIterator("testdata")
	var n int

	for tsm.HasNextKey() {
		tsm.NextKey()
		for tsm.HasNextPoint() {
			tsm.NextPoint()
			n++
		}
	}

	if n != pointN {
		t.Errorf("number of points: expected %d, got %d", pointN, n)
	}
}

func TestTSMIterator_Convert(t *testing.T) {
	tsm := NewTSMIterator("testdata")
	inputs := tsm.ConvertToQueryIterators()
	opt := query.IteratorOptions{}
	itr, err := query.Iterators(inputs).Merge(opt)
	if err != nil {
		panic(err)
	}
	fmt.Printf("iterator stats: %+v\n", itr.Stats())
}
