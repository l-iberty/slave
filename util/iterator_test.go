package util

import (
	"os"
	"testing"
)

const (
	seriesN = 770
	pointN  = 15364000 // 使用 influx_inspect dumptsm 得到的 point 总数
)

func Test_TSMIterator_Encode(t *testing.T) {
	tsm := NewTSMIterator("testdata", 1000)
	file, err := os.OpenFile("tmpfile", os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if err = tsm.Encode(file); err != nil {
		panic(err)
	}

	stats := tsm.Stats()
	if stats.PointN != pointN {
		t.Errorf("PointN got %d, expected: %d", stats.PointN, pointN)
	}
	if stats.SeriesN != seriesN {
		t.Errorf("SeriesN got %d, expected: %d", stats.SeriesN, seriesN)
	}
}
