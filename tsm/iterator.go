package util

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

const (
	floatPointType   = 0
	integerPointType = 1
)

type TSMIterator struct {
	mu       sync.RWMutex
	files    []string
	readers  []*tsm1.TSMReader
	tsmIndex int
	keyIndex int

	curKey     []byte
	curKeyType byte

	curFloatKey   []byte
	curIntegerKey []byte

	values   []tsm1.Value // values of current key
	valIndex int

	series map[string]struct{}
	pointN int

	floatItr   *floatIterator
	integerItr *integerIterator

	stopC   chan struct{}
	notifyC chan int
}

func NewTSMIterator(dir string) *TSMIterator {
	itr := &TSMIterator{
		series:  make(map[string]struct{}),
		stopC:   make(chan struct{}),
		notifyC: make(chan int),
	}

	itr.files = mustGetTsmFilesFromDir(dir)
	for _, f := range itr.files {
		file, err := os.Open(f)
		if err != nil {
			panic(err)
		}
		r, err := tsm1.NewTSMReader(file)
		if err != nil {
			panic(err)
		}
		itr.readers = append(itr.readers, r)
	}

	return itr
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

	t.curKey, t.curKeyType = t.readers[t.tsmIndex].KeyAt(t.keyIndex)
	values, err := t.readers[t.tsmIndex].ReadAll(t.curKey)
	if err != nil {
		panic(err)
	}
	t.keyIndex++
	t.values = values
	t.valIndex = 0
	return t.curKey, t.curKeyType
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
	t.series = make(map[string]struct{})
	t.pointN = 0
}

func (t *TSMIterator) Walk() {
	for t.HasNextKey() {
		key, _ := t.NextKey()
		s, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		values := t.Values()
		t.mu.Lock()
		t.series[string(s)] = struct{}{}
		t.pointN += len(values)
		t.mu.Unlock()
	}
}

func (t *TSMIterator) Stats() query.IteratorStats {
	t.Reset()
	t.Walk()
	stats := query.IteratorStats{
		SeriesN: len(t.series),
		PointN:  t.pointN,
	}
	t.Reset()
	return stats
}

func (t *TSMIterator) Close() error {
	t.Reset()
	return nil
}

func (t *TSMIterator) stepFloatPoint() {
	for {
		for t.HasNextPoint() {
			p := t.NextPoint()
			switch p.(type) {
			case *query.FloatPoint:
				t.floatItr.entryC <- &floatEntry{key: t.curFloatKey, point: p.(*query.FloatPoint)}
				return
			}
		}
		if t.HasNextKey() {
			t.NextKey()
		} else {
			return
		}
	}
}

func (t *TSMIterator) eventLoop() {
	for {
		select {
		case <-t.stopC:
			return
		case typ := <-t.notifyC:
			log.Printf("eventLoop type: %d", typ)
			t.step(typ)
		}
	}
}

func (t *TSMIterator) EncodeIterators(w io.Writer) error {
	floatEntryC := make(chan *floatEntry, 1000)
	integerEntryC := make(chan *integerEntry, 1000)
	t.floatItr = newFloatIterator(t.notifyC, floatEntryC)
	t.integerItr = newIntegerIterator(t.notifyC, integerEntryC)

	go t.eventLoop()
	defer func() { t.stopC <- struct{}{} }()

	enc := query.NewIteratorEncoder(w)
	itrs := []query.Iterator{t.floatItr, t.integerItr}
	for _, itr := range itrs {
		if err := enc.EncodeIterator(itr); err != nil {
			return err
		}
	}
	return nil
}

type floatEntry struct {
	key   []byte
	point *query.FloatPoint
}

type floatIterator struct {
	points  []*query.FloatPoint
	series  map[string]struct{}
	notifyC chan int
	entryC  chan *floatEntry
}

func newFloatIterator(notifyC chan int, entryC chan *floatEntry) *floatIterator {
	return &floatIterator{
		series:  make(map[string]struct{}),
		notifyC: notifyC,
		entryC:  entryC,
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
	itr.points = nil
	itr.series = nil
	close(itr.notifyC)
	close(itr.entryC)
	return nil
}

func (itr *floatIterator) Next() (*query.FloatPoint, error) {
	itr.notifyC <- floatPointType
	select {
	case e := <-itr.entryC:
		if e != nil {
			s, _ := tsm1.SeriesAndFieldFromCompositeKey(e.key)
			itr.series[string(s)] = struct{}{}
			log.Print("float point")
			return e.point, nil
		}
	}
	return nil, nil
}

type integerEntry struct {
	key   []byte
	point *query.IntegerPoint
}

type integerIterator struct {
	points  []*query.IntegerPoint
	series  map[string]struct{}
	notifyC chan int
	entryC  chan *integerEntry
}

func newIntegerIterator(notifyC chan int, entryC chan *integerEntry) *integerIterator {
	return &integerIterator{
		series:  make(map[string]struct{}),
		notifyC: notifyC,
		entryC:  entryC,
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
	itr.points = nil
	itr.series = nil
	close(itr.notifyC)
	close(itr.entryC)
	return nil
}

func (itr *integerIterator) Next() (*query.IntegerPoint, error) {
	itr.notifyC <- integerPointType
	select {
	case e := <-itr.entryC:
		if e != nil {
			s, _ := tsm1.SeriesAndFieldFromCompositeKey(e.key)
			itr.series[string(s)] = struct{}{}
			log.Print("integer point")
			return e.point, nil
		}
	}
	return nil, nil
}
