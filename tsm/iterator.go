package util

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type TSMFloatIterator struct {
	mu       sync.RWMutex
	files    []string
	readers  []*tsm1.TSMReader
	tsmIndex int

	key      []byte // current key
	keyIndex int
	values   []tsm1.Value // values of current key
	valIndex int

	series map[string]struct{}
	pointN int

	// itr *floatIterator

	stopC   chan struct{}
	notifyC chan struct{}
}

func NewTSMFloatIterator(dir string) *TSMFloatIterator {
	itr := &TSMFloatIterator{
		series:  make(map[string]struct{}),
		stopC:   make(chan struct{}),
		notifyC: make(chan struct{}),
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

func (t *TSMFloatIterator) NextKey() ([]byte, byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nextKey()
}

func (t *TSMFloatIterator) nextKey() ([]byte, byte) {
	var key []byte
	var typ byte

	for {
		if t.keyIndex == t.readers[t.tsmIndex].KeyCount() {
			t.tsmIndex++
			t.keyIndex = 0
		}
		if t.tsmIndex == len(t.readers) {
			return nil, 0
		}

		key, typ = t.readers[t.tsmIndex].KeyAt(t.keyIndex)
		if typ == tsm1.BlockFloat64 {
			break
		}
		t.keyIndex++
	}

	values, err := t.readers[t.tsmIndex].ReadAll(key)
	if err != nil {
		panic(err)
	}
	t.key = key
	t.keyIndex++
	t.values = values
	t.valIndex = 0

	return key, typ
}

func (t *TSMFloatIterator) Values() []tsm1.Value {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.values
}

func (t *TSMFloatIterator) NextPoint() *query.FloatPoint {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.valIndex == len(t.values) {
		key, _ := t.nextKey()
		if key == nil {
			return nil
		}
		t.valIndex = 0
	}

	v := t.values[t.valIndex]
	t.valIndex++
	switch v.(type) {
	case tsm1.FloatValue:
		return &query.FloatPoint{Time: v.UnixNano(), Value: v.Value().(float64)}
	default:
		panic("only FloatValue allowed")
	}
}

func (t *TSMFloatIterator) reset() {
	t.tsmIndex = 0
	t.keyIndex = 0
	t.values = []tsm1.Value{}
	t.valIndex = 0
	t.series = make(map[string]struct{})
	t.pointN = 0
}

func (t *TSMFloatIterator) walk() {
	for {
		key, _ := t.nextKey()
		if key == nil {
			break
		}
		s, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		t.series[string(s)] = struct{}{}
		t.pointN += len(t.values)
	}
}

func (t *TSMFloatIterator) Stats() query.IteratorStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.reset()
	t.walk()
	stats := query.IteratorStats{
		SeriesN: len(t.series),
		PointN:  t.pointN,
	}
	t.reset()
	return stats
}

func (t *TSMFloatIterator) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reset()
	return nil
}

func (t *TSMFloatIterator) step() {
	p := t.NextPoint()
	t.floatItr.entryC <- &floatEntry{key: t.key, point: p}
}

func (t *TSMFloatIterator) eventLoop() {
	for {
		select {
		case <-t.stopC:
			return
		case <-t.notifyC:
			t.step()
		}
	}
}

// func (t *TSMFloatIterator) EncodeIterators(w io.Writer) error {
// 	floatEntryC := make(chan *floatEntry, 1000)
// 	integerEntryC := make(chan *integerEntry, 1000)
// 	t.floatItr = newFloatIterator(t.notifyC, floatEntryC)
// 	t.integerItr = newIntegerIterator(t.notifyC, integerEntryC)
//
// 	go t.eventLoop()
// 	defer func() { t.stopC <- struct{}{} }()
//
// 	enc := query.NewIteratorEncoder(w)
// 	itrs := []query.Iterator{t.floatItr, t.integerItr}
// 	for _, itr := range itrs {
// 		if err := enc.EncodeIterator(itr); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }
//
// type floatEntry struct {
// 	key   []byte
// 	point *query.FloatPoint
// }
//
// type floatIterator struct {
// 	points  []*query.FloatPoint
// 	series  map[string]struct{}
// 	notifyC chan int
// 	entryC  chan *floatEntry
// }
//
// func newFloatIterator(notifyC chan int, entryC chan *floatEntry) *floatIterator {
// 	return &floatIterator{
// 		series:  make(map[string]struct{}),
// 		notifyC: notifyC,
// 		entryC:  entryC,
// 	}
// }
//
// func (itr *floatIterator) put(key []byte, p *query.FloatPoint) {
// 	s, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
// 	itr.series[string(s)] = struct{}{}
// 	itr.points = append(itr.points, p)
// }
//
// func (itr *floatIterator) Stats() query.IteratorStats {
// 	return query.IteratorStats{
// 		SeriesN: len(itr.series),
// 		PointN:  len(itr.points),
// 	}
// }
//
// func (itr *floatIterator) Close() error {
// 	itr.points = nil
// 	itr.series = nil
// 	close(itr.notifyC)
// 	close(itr.entryC)
// 	return nil
// }
//
// func (itr *floatIterator) Next() (*query.FloatPoint, error) {
// 	itr.notifyC <- floatPointType
// 	select {
// 	case e := <-itr.entryC:
// 		if e != nil {
// 			s, _ := tsm1.SeriesAndFieldFromCompositeKey(e.key)
// 			itr.series[string(s)] = struct{}{}
// 			log.Print("float point")
// 			return e.point, nil
// 		}
// 	}
// 	return nil, nil
// }
//
// type integerEntry struct {
// 	key   []byte
// 	point *query.IntegerPoint
// }
//
// type integerIterator struct {
// 	points  []*query.IntegerPoint
// 	series  map[string]struct{}
// 	notifyC chan int
// 	entryC  chan *integerEntry
// }
//
// func newIntegerIterator(notifyC chan int, entryC chan *integerEntry) *integerIterator {
// 	return &integerIterator{
// 		series:  make(map[string]struct{}),
// 		notifyC: notifyC,
// 		entryC:  entryC,
// 	}
// }
//
// func (itr *integerIterator) put(key []byte, p *query.IntegerPoint) {
// 	s, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
// 	itr.series[string(s)] = struct{}{}
// 	itr.points = append(itr.points, p)
// }
//
// func (itr *integerIterator) Stats() query.IteratorStats {
// 	return query.IteratorStats{
// 		SeriesN: len(itr.series),
// 		PointN:  len(itr.points),
// 	}
// }
//
// func (itr *integerIterator) Close() error {
// 	itr.points = nil
// 	itr.series = nil
// 	close(itr.notifyC)
// 	close(itr.entryC)
// 	return nil
// }
//
// func (itr *integerIterator) Next() (*query.IntegerPoint, error) {
// 	itr.notifyC <- integerPointType
// 	select {
// 	case e := <-itr.entryC:
// 		if e != nil {
// 			s, _ := tsm1.SeriesAndFieldFromCompositeKey(e.key)
// 			itr.series[string(s)] = struct{}{}
// 			log.Print("integer point")
// 			return e.point, nil
// 		}
// 	}
// 	return nil, nil
// }
