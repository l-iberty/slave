package util

import (
	"github.com/influxdata/influxdb/query"
	"testing"
)

// 使用 influx_insepct dumptsm 对 testdata 目录下的 8 个 TSM 进行统计得到的 series 和 point 总数
const (
	seriesN = 1634
	pointN  = 15364000
)

func TestTSMIterator_WalkValues(t *testing.T) {
	iter := NewTSMIterator("testdata")
	var n int

	for iter.HasNextKey() {
		iter.NextKey()
		values:= iter.Values()
		n += len(values)
	}

	if n != pointN {
		t.Errorf("number of points: expected %d, got %d", pointN, n)
	}
}

func TestTSMIterator_Stats(t *testing.T) {
	iter := NewTSMIterator("testdata")
	stats := query.IteratorStats{}

	stats = iter.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN)
	}

	stats = iter.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN)
	}

	iter.Reset()
	stats = iter.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN)
	}

	iter.Reset()
	stats = iter.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN)
	}

	stats = iter.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN)
	}
}

func TestTSMIterator_WalkPoints(t *testing.T) {
	iter := NewTSMIterator("testdata")
	var n int

	for iter.HasNextKey() {
		iter.NextKey()
		for iter.HasNextPoint() {
			iter.NextPoint()
			n++
		}
	}

	if n != pointN {
		t.Errorf("number of points: expected %d, got %d", pointN, n)
	}
}
