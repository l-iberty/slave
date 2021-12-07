package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

var (
	ErrNoMorePoints = fmt.Errorf("no more points")
)

type TSMIterator struct {
	mu       sync.RWMutex
	files    []string
	readers  []*tsm1.TSMReader
	tsmIndex int
	keyIndex int

	values   []tsm1.Value // values of current key
	valIndex int

	stats query.IteratorStats
}

func NewTSMIterator(dir string) *TSMIterator {
	iter := &TSMIterator{}

	iter.files = mustGetTsmFilesFromDir(dir)
	for _, f := range iter.files {
		file, err := os.Open(f)
		if err != nil {
			panic(err)
		}
		r, err := tsm1.NewTSMReader(file)
		if err != nil {
			panic(err)
		}
		iter.readers = append(iter.readers, r)
	}

	return iter
}

func mustGetTsmFilesFromDir(dirname string) []string {
	var files []string

	dir, err := ioutil.ReadDir(dirname)
	if err != nil {
		panic(err)
	}

	for _, f := range dir {
		if f.IsDir() {
			continue
		}
		path := filepath.Join(dirname, f.Name())
		files = append(files, path)
	}
	return files
}

func (t *TSMIterator) HasNextKey() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.tsmIndex == len(t.readers)-1 {
		return t.keyIndex < t.readers[t.tsmIndex].KeyCount()
	}
	return t.tsmIndex < len(t.readers)-1
}

func (t *TSMIterator) NextKey() ([]byte, byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.keyIndex == t.readers[t.tsmIndex].KeyCount() {
		t.tsmIndex++
		t.keyIndex = 0
	}
	if t.tsmIndex < 0 || t.tsmIndex > len(t.readers) {
		panic("tsmIndex out of bound")
	}

	key, typ := t.readers[t.tsmIndex].KeyAt(t.keyIndex)
	values, err := t.readers[t.tsmIndex].ReadAll(key)
	if err != nil {
		panic(err)
	}
	t.keyIndex++
	t.values = values
	t.valIndex = 0
	return key, typ
}

func (t *TSMIterator) Values() []tsm1.Value {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.values
}

func (t *TSMIterator) HasNextPoint() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.valIndex < len(t.values)
}

func (t *TSMIterator) NextPoint() query.Point {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.valIndex < 0 || t.valIndex >= len(t.values) {
		panic("valIndex out of bound")
	}

	v := t.values[t.valIndex]
	t.valIndex++

	switch v.(type) {
	case tsm1.FloatValue:
		return &query.FloatPoint{Time: v.UnixNano(), Value: v.Value().(float64)}
	case tsm1.IntegerValue:
		return &query.IntegerPoint{Time: v.UnixNano(), Value: v.Value().(int64)}
	case tsm1.UnsignedValue:
		return &query.UnsignedPoint{Time: v.UnixNano(), Value: v.Value().(uint64)}
	case tsm1.BooleanValue:
		return &query.BooleanPoint{Time: v.UnixNano(), Value: v.Value().(bool)}
	case tsm1.StringValue:
		return &query.StringPoint{Time: v.UnixNano(), Value: v.Value().(string)}
	}

	panic("unknown value type")
}

func (t *TSMIterator) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tsmIndex = 0
	t.keyIndex = 0
	t.values = []tsm1.Value{}
	t.valIndex = 0
	t.stats = query.IteratorStats{}
}

func (t *TSMIterator) Walk() error {
	for t.HasNextKey() {
		t.NextKey()
		values := t.Values()
		t.mu.Lock()
		t.stats.PointN += len(values)
		t.mu.Unlock()
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, r := range t.readers {
		t.stats.SeriesN += r.KeyCount()
	}
	return nil
}

func (t *TSMIterator) Stats() query.IteratorStats {
	t.Reset()
	if err := t.Walk(); err != nil {
		t.Reset()
		return query.IteratorStats{}
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.stats
}

func (t *TSMIterator) Close() error {
	t.Reset()
	return nil
}

func (t *TSMIterator) ConvertToQueryIterators() []query.Iterator {
	var iters []query.Iterator

	floatItr := newFloatIterator()
	integerItr := newIntegerIterator()

	for t.HasNextKey() {
		key, _ := t.NextKey()
		for t.HasNextPoint() {
			p := t.NextPoint()
			switch p.(type) {
			case *query.FloatPoint:
				floatItr.put(key, p.(*query.FloatPoint))
			case *query.IntegerPoint:
				integerItr.put(key, p.(*query.IntegerPoint))
			case *query.UnsignedPoint:
			case *query.StringPoint:
			case *query.BooleanPoint:
			}
		}
	}

	// iters = append(iters, floatItr)
	iters = append(iters, integerItr)

	return iters
}

type floatIterator struct {
	idx    int
	points []*query.FloatPoint
	series map[string]struct{}
}

func newFloatIterator() *floatIterator {
	return &floatIterator{
		points: []*query.FloatPoint{},
		series: make(map[string]struct{}),
	}
}

func (itr *floatIterator) put(key []byte, p *query.FloatPoint) {
	s, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
	itr.series[string(s)] = struct{}{}
	itr.points = append(itr.points, p)
}

func (itr *floatIterator) Stats() query.IteratorStats {
	return query.IteratorStats{
		SeriesN: len(itr.series),
		PointN:  len(itr.points),
	}
}

func (itr *floatIterator) Close() error {
	itr.idx = 0
	itr.points = nil
	itr.series = nil
	return nil
}

func (itr *floatIterator) Next() (*query.FloatPoint, error) {
	if itr.idx < 0 || itr.idx >= len(itr.points) {
		return nil, ErrNoMorePoints
	}
	idx := itr.idx
	itr.idx++
	return itr.points[idx], nil
}

type integerIterator struct {
	idx    int
	points []*query.IntegerPoint
	series map[string]struct{}
}

func newIntegerIterator() *integerIterator {
	return &integerIterator{
		points: []*query.IntegerPoint{},
		series: make(map[string]struct{}),
	}
}

func (itr *integerIterator) put(key []byte, p *query.IntegerPoint) {
	s, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
	itr.series[string(s)] = struct{}{}
	itr.points = append(itr.points, p)
}

func (itr *integerIterator) Stats() query.IteratorStats {
	return query.IteratorStats{
		SeriesN: len(itr.series),
		PointN:  len(itr.points),
	}
}

func (itr *integerIterator) Close() error {
	itr.idx = 0
	itr.points = nil
	itr.series = nil
	return nil
}

func (itr *integerIterator) Next() (*query.IntegerPoint, error) {
	if itr.idx < 0 || itr.idx >= len(itr.points) {
		return nil, ErrNoMorePoints
	}
	idx := itr.idx
	itr.idx++
	return itr.points[idx], nil
}
