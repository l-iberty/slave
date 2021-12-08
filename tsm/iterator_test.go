package util

import (
	"github.com/influxdata/influxdb/query"
	"os"
	"testing"
)

const (
	seriesN = 770
	pointN  = 15364000 // 使用 influx_inspect dumptsm 得到
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

func TestTSMIterator_EncodeIterators(t *testing.T) {
	// time.Sleep(30 * time.Second)
	tsm := NewTSMIterator("testdata")
	file, err := os.OpenFile("tmpfile", os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if err = tsm.EncodeIterators(file); err != nil {
		panic(err)
	}
}
