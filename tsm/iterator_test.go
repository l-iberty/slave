package util

import (
	"os"
	"testing"

	"github.com/influxdata/influxdb/query"
)

const (
	seriesN = 770
	pointN  = 15364000 // 使用 influx_inspect dumptsm 得到的 point 总数
)

func Test_TSMFloatIterator_KeyValues(t *testing.T) {
	tsm := NewTSMFloatIterator("testdata")
	var n int

	for {
		key, _ := tsm.NextKey()
		if key == nil {
			break
		}
		values := tsm.Values()
		n += len(values)
	}

	if n != pointN/2 {
		t.Errorf("number of points: expected %d, got %d", pointN/2, n)
	}
}

func Test_TSMFloatIterator_Points(t *testing.T) {
	tsm := NewTSMFloatIterator("testdata")
	var n int

	for {
		p := tsm.NextPoint()
		if p == nil {
			break
		}
		n++
	}

	if n != pointN/2 {
		t.Errorf("number of points: expected %d, got %d", pointN/2, n)
	}
}

func Test_TSMFloatIterator_Stats(t *testing.T) {
	tsm := NewTSMFloatIterator("testdata")
	stats := query.IteratorStats{}

	stats = tsm.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN/2 {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN/2)
	}

	stats = tsm.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN/2 {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN/2)
	}

	tsm.reset()
	stats = tsm.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN/2 {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN/2)
	}

	tsm.reset()
	stats = tsm.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN/2 {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN/2)
	}

	stats = tsm.Stats()
	if stats.SeriesN != seriesN || stats.PointN != pointN/2 {
		t.Errorf("stats.SeriesN got %d, expected: %d\nstats.PointN got %d, expected: %d", stats.SeriesN, seriesN, stats.PointN, pointN/2)
	}
}

func Test_TSMFloatIterator_EncodeIterators(t *testing.T) {
	// time.Sleep(30 * time.Second)
	tsm := NewTSMFloatIterator("testdata")
	file, err := os.OpenFile("tmpfile", os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if err = tsm.EncodeIterators(file); err != nil {
		panic(err)
	}
}
