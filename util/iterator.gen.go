// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: iterator.gen.go.tmpl

package util

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type TSMIterator struct {
	mu       sync.RWMutex
	encoders []IteratorEncoder
}

type IteratorEncoder interface {
	Encode(w io.Writer) error
	Stats() query.IteratorStats
	Series() map[string]struct{}
}

func NewTSMIterator(dir string, batchSize int) *TSMIterator {
	var files []string
	var readers []*tsm1.TSMReader

	files = mustGetTSMFilesFromDir(dir)
	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			panic(err)
		}
		r, err := tsm1.NewTSMReader(file)
		if err != nil {
			panic(err)
		}
		readers = append(readers, r)
	}

	t := &TSMIterator{}
	t.encoders = []IteratorEncoder{
		NewTSMFloatIterator(readers, batchSize),
		NewTSMIntegerIterator(readers, batchSize),
		NewTSMStringIterator(readers, batchSize),
		NewTSMBooleanIterator(readers, batchSize),
	}

	return t
}

func (t *TSMIterator) Encode(w io.Writer) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, enc := range t.encoders {
		if err := enc.Encode(w); err != nil {
			return err
		}
		fmt.Printf("%T: %+v\n", enc, enc.Stats())
	}
	return nil
}

func (t *TSMIterator) Stats() query.IteratorStats {
	stats := query.IteratorStats{}
	series := make(map[string]struct{})
	for _, enc := range t.encoders {
		stats.PointN += enc.Stats().PointN
		for k := range enc.Series() {
			series[k] = struct{}{}
		}
	}
	stats.SeriesN = len(series)
	return stats
}

func mustGetTSMFilesFromDir(dirname string) []string {
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

type TSMFloatIterator struct {
	readers  []*tsm1.TSMReader
	tsmIndex int

	key      []byte // current key
	keyIndex int
	values   []tsm1.Value // values of current key
	valIndex int

	series map[string]struct{}
	pointN int

	itr       *floatIterator
	batchSize int

	stopC   chan struct{}
	notifyC chan struct{}
}

func NewTSMFloatIterator(readers []*tsm1.TSMReader, batchSize int) *TSMFloatIterator {
	itr := &TSMFloatIterator{
		readers:   readers,
		series:    make(map[string]struct{}),
		batchSize: batchSize,
		stopC:     make(chan struct{}),
		notifyC:   make(chan struct{}),
	}
	return itr
}

func (t *TSMFloatIterator) NextKey() ([]byte, byte) {
	return t.nextKey()
}

func (t *TSMFloatIterator) nextKey() ([]byte, byte) {
	var key []byte
	var typ byte

	for {
		if t.tsmIndex == len(t.readers) {
			return nil, 0
		}
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
	return t.values
}

func (t *TSMFloatIterator) Series() map[string]struct{} {
	return t.series
}

func (t *TSMFloatIterator) NextPoint() *query.FloatPoint {
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
	if len(t.series) == 0 || t.pointN == 0 {
		t.walk()
	}
	stats := query.IteratorStats{
		SeriesN: len(t.series),
		PointN:  t.pointN,
	}
	return stats
}

func (t *TSMFloatIterator) Close() error {
	return nil
}

func (t *TSMFloatIterator) step() {
	batch := make([]*floatEntry, 0, t.batchSize)
	for i := 0; i < t.batchSize; i++ {
		p := t.NextPoint()
		if p == nil {
			break
		}
		batch = append(batch, &floatEntry{key: t.key, point: p})

		s, _ := tsm1.SeriesAndFieldFromCompositeKey(t.key)
		t.series[string(s)] = struct{}{}
		t.pointN++
	}
	t.itr.entriesC <- batch
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

func (t *TSMFloatIterator) Encode(w io.Writer) error {
	entriesC := make(chan []*floatEntry)
	t.itr = newFloatIterator(t.notifyC, entriesC)

	go t.eventLoop()
	defer close(t.stopC)

	enc := query.NewIteratorEncoder(w)
	if err := enc.EncodeIterator(t.itr); err != nil {
		return err
	}
	return nil
}

type floatEntry struct {
	key   []byte
	point *query.FloatPoint
}

type floatIterator struct {
	pointN   int
	series   map[string]struct{}
	notifyC  chan struct{}
	entriesC chan []*floatEntry
	batch    []*floatEntry
}

func newFloatIterator(notifyC chan struct{}, entryC chan []*floatEntry) *floatIterator {
	return &floatIterator{
		series:   make(map[string]struct{}),
		notifyC:  notifyC,
		entriesC: entryC,
	}
}

func (itr *floatIterator) Stats() query.IteratorStats {
	return query.IteratorStats{
		SeriesN: len(itr.series),
		PointN:  itr.pointN,
	}
}

func (itr *floatIterator) Close() error {
	itr.pointN = 0
	itr.series = nil
	close(itr.notifyC)
	close(itr.entriesC)
	return nil
}

func (itr *floatIterator) Next() (*query.FloatPoint, error) {
	if len(itr.batch) == 0 {
		itr.notifyC <- struct{}{}
		select {
		case batch := <-itr.entriesC:
			itr.batch = batch
		}
	}
	if len(itr.batch) == 0 {
		return nil, nil
	}
	p := itr.batch[0]
	itr.batch = itr.batch[1:]
	s, _ := tsm1.SeriesAndFieldFromCompositeKey(p.key)
	itr.series[string(s)] = struct{}{}
	itr.pointN++
	return p.point, nil
}

type TSMIntegerIterator struct {
	readers  []*tsm1.TSMReader
	tsmIndex int

	key      []byte // current key
	keyIndex int
	values   []tsm1.Value // values of current key
	valIndex int

	series map[string]struct{}
	pointN int

	itr       *integerIterator
	batchSize int

	stopC   chan struct{}
	notifyC chan struct{}
}

func NewTSMIntegerIterator(readers []*tsm1.TSMReader, batchSize int) *TSMIntegerIterator {
	itr := &TSMIntegerIterator{
		readers:   readers,
		series:    make(map[string]struct{}),
		batchSize: batchSize,
		stopC:     make(chan struct{}),
		notifyC:   make(chan struct{}),
	}
	return itr
}

func (t *TSMIntegerIterator) NextKey() ([]byte, byte) {
	return t.nextKey()
}

func (t *TSMIntegerIterator) nextKey() ([]byte, byte) {
	var key []byte
	var typ byte

	for {
		if t.tsmIndex == len(t.readers) {
			return nil, 0
		}
		if t.keyIndex == t.readers[t.tsmIndex].KeyCount() {
			t.tsmIndex++
			t.keyIndex = 0
		}
		if t.tsmIndex == len(t.readers) {
			return nil, 0
		}

		key, typ = t.readers[t.tsmIndex].KeyAt(t.keyIndex)
		if typ == tsm1.BlockInteger {
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

func (t *TSMIntegerIterator) Values() []tsm1.Value {
	return t.values
}

func (t *TSMIntegerIterator) Series() map[string]struct{} {
	return t.series
}

func (t *TSMIntegerIterator) NextPoint() *query.IntegerPoint {
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
	case tsm1.IntegerValue:
		return &query.IntegerPoint{Time: v.UnixNano(), Value: v.Value().(int64)}
	default:
		panic("only IntegerValue allowed")
	}
}

func (t *TSMIntegerIterator) walk() {
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

func (t *TSMIntegerIterator) Stats() query.IteratorStats {
	if len(t.series) == 0 || t.pointN == 0 {
		t.walk()
	}
	stats := query.IteratorStats{
		SeriesN: len(t.series),
		PointN:  t.pointN,
	}
	return stats
}

func (t *TSMIntegerIterator) Close() error {
	return nil
}

func (t *TSMIntegerIterator) step() {
	batch := make([]*integerEntry, 0, t.batchSize)
	for i := 0; i < t.batchSize; i++ {
		p := t.NextPoint()
		if p == nil {
			break
		}
		batch = append(batch, &integerEntry{key: t.key, point: p})

		s, _ := tsm1.SeriesAndFieldFromCompositeKey(t.key)
		t.series[string(s)] = struct{}{}
		t.pointN++
	}
	t.itr.entriesC <- batch
}

func (t *TSMIntegerIterator) eventLoop() {
	for {
		select {
		case <-t.stopC:
			return
		case <-t.notifyC:
			t.step()
		}
	}
}

func (t *TSMIntegerIterator) Encode(w io.Writer) error {
	entriesC := make(chan []*integerEntry)
	t.itr = newIntegerIterator(t.notifyC, entriesC)

	go t.eventLoop()
	defer close(t.stopC)

	enc := query.NewIteratorEncoder(w)
	if err := enc.EncodeIterator(t.itr); err != nil {
		return err
	}
	return nil
}

type integerEntry struct {
	key   []byte
	point *query.IntegerPoint
}

type integerIterator struct {
	pointN   int
	series   map[string]struct{}
	notifyC  chan struct{}
	entriesC chan []*integerEntry
	batch    []*integerEntry
}

func newIntegerIterator(notifyC chan struct{}, entryC chan []*integerEntry) *integerIterator {
	return &integerIterator{
		series:   make(map[string]struct{}),
		notifyC:  notifyC,
		entriesC: entryC,
	}
}

func (itr *integerIterator) Stats() query.IteratorStats {
	return query.IteratorStats{
		SeriesN: len(itr.series),
		PointN:  itr.pointN,
	}
}

func (itr *integerIterator) Close() error {
	itr.pointN = 0
	itr.series = nil
	close(itr.notifyC)
	close(itr.entriesC)
	return nil
}

func (itr *integerIterator) Next() (*query.IntegerPoint, error) {
	if len(itr.batch) == 0 {
		itr.notifyC <- struct{}{}
		select {
		case batch := <-itr.entriesC:
			itr.batch = batch
		}
	}
	if len(itr.batch) == 0 {
		return nil, nil
	}
	p := itr.batch[0]
	itr.batch = itr.batch[1:]
	s, _ := tsm1.SeriesAndFieldFromCompositeKey(p.key)
	itr.series[string(s)] = struct{}{}
	itr.pointN++
	return p.point, nil
}

type TSMStringIterator struct {
	readers  []*tsm1.TSMReader
	tsmIndex int

	key      []byte // current key
	keyIndex int
	values   []tsm1.Value // values of current key
	valIndex int

	series map[string]struct{}
	pointN int

	itr       *stringIterator
	batchSize int

	stopC   chan struct{}
	notifyC chan struct{}
}

func NewTSMStringIterator(readers []*tsm1.TSMReader, batchSize int) *TSMStringIterator {
	itr := &TSMStringIterator{
		readers:   readers,
		series:    make(map[string]struct{}),
		batchSize: batchSize,
		stopC:     make(chan struct{}),
		notifyC:   make(chan struct{}),
	}
	return itr
}

func (t *TSMStringIterator) NextKey() ([]byte, byte) {
	return t.nextKey()
}

func (t *TSMStringIterator) nextKey() ([]byte, byte) {
	var key []byte
	var typ byte

	for {
		if t.tsmIndex == len(t.readers) {
			return nil, 0
		}
		if t.keyIndex == t.readers[t.tsmIndex].KeyCount() {
			t.tsmIndex++
			t.keyIndex = 0
		}
		if t.tsmIndex == len(t.readers) {
			return nil, 0
		}

		key, typ = t.readers[t.tsmIndex].KeyAt(t.keyIndex)
		if typ == tsm1.BlockString {
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

func (t *TSMStringIterator) Values() []tsm1.Value {
	return t.values
}

func (t *TSMStringIterator) Series() map[string]struct{} {
	return t.series
}

func (t *TSMStringIterator) NextPoint() *query.StringPoint {
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
	case tsm1.StringValue:
		return &query.StringPoint{Time: v.UnixNano(), Value: v.Value().(string)}
	default:
		panic("only StringValue allowed")
	}
}

func (t *TSMStringIterator) walk() {
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

func (t *TSMStringIterator) Stats() query.IteratorStats {
	if len(t.series) == 0 || t.pointN == 0 {
		t.walk()
	}
	stats := query.IteratorStats{
		SeriesN: len(t.series),
		PointN:  t.pointN,
	}
	return stats
}

func (t *TSMStringIterator) Close() error {
	return nil
}

func (t *TSMStringIterator) step() {
	batch := make([]*stringEntry, 0, t.batchSize)
	for i := 0; i < t.batchSize; i++ {
		p := t.NextPoint()
		if p == nil {
			break
		}
		batch = append(batch, &stringEntry{key: t.key, point: p})

		s, _ := tsm1.SeriesAndFieldFromCompositeKey(t.key)
		t.series[string(s)] = struct{}{}
		t.pointN++
	}
	t.itr.entriesC <- batch
}

func (t *TSMStringIterator) eventLoop() {
	for {
		select {
		case <-t.stopC:
			return
		case <-t.notifyC:
			t.step()
		}
	}
}

func (t *TSMStringIterator) Encode(w io.Writer) error {
	entriesC := make(chan []*stringEntry)
	t.itr = newStringIterator(t.notifyC, entriesC)

	go t.eventLoop()
	defer close(t.stopC)

	enc := query.NewIteratorEncoder(w)
	if err := enc.EncodeIterator(t.itr); err != nil {
		return err
	}
	return nil
}

type stringEntry struct {
	key   []byte
	point *query.StringPoint
}

type stringIterator struct {
	pointN   int
	series   map[string]struct{}
	notifyC  chan struct{}
	entriesC chan []*stringEntry
	batch    []*stringEntry
}

func newStringIterator(notifyC chan struct{}, entryC chan []*stringEntry) *stringIterator {
	return &stringIterator{
		series:   make(map[string]struct{}),
		notifyC:  notifyC,
		entriesC: entryC,
	}
}

func (itr *stringIterator) Stats() query.IteratorStats {
	return query.IteratorStats{
		SeriesN: len(itr.series),
		PointN:  itr.pointN,
	}
}

func (itr *stringIterator) Close() error {
	itr.pointN = 0
	itr.series = nil
	close(itr.notifyC)
	close(itr.entriesC)
	return nil
}

func (itr *stringIterator) Next() (*query.StringPoint, error) {
	if len(itr.batch) == 0 {
		itr.notifyC <- struct{}{}
		select {
		case batch := <-itr.entriesC:
			itr.batch = batch
		}
	}
	if len(itr.batch) == 0 {
		return nil, nil
	}
	p := itr.batch[0]
	itr.batch = itr.batch[1:]
	s, _ := tsm1.SeriesAndFieldFromCompositeKey(p.key)
	itr.series[string(s)] = struct{}{}
	itr.pointN++
	return p.point, nil
}

type TSMBooleanIterator struct {
	readers  []*tsm1.TSMReader
	tsmIndex int

	key      []byte // current key
	keyIndex int
	values   []tsm1.Value // values of current key
	valIndex int

	series map[string]struct{}
	pointN int

	itr       *booleanIterator
	batchSize int

	stopC   chan struct{}
	notifyC chan struct{}
}

func NewTSMBooleanIterator(readers []*tsm1.TSMReader, batchSize int) *TSMBooleanIterator {
	itr := &TSMBooleanIterator{
		readers:   readers,
		series:    make(map[string]struct{}),
		batchSize: batchSize,
		stopC:     make(chan struct{}),
		notifyC:   make(chan struct{}),
	}
	return itr
}

func (t *TSMBooleanIterator) NextKey() ([]byte, byte) {
	return t.nextKey()
}

func (t *TSMBooleanIterator) nextKey() ([]byte, byte) {
	var key []byte
	var typ byte

	for {
		if t.tsmIndex == len(t.readers) {
			return nil, 0
		}
		if t.keyIndex == t.readers[t.tsmIndex].KeyCount() {
			t.tsmIndex++
			t.keyIndex = 0
		}
		if t.tsmIndex == len(t.readers) {
			return nil, 0
		}

		key, typ = t.readers[t.tsmIndex].KeyAt(t.keyIndex)
		if typ == tsm1.BlockBoolean {
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

func (t *TSMBooleanIterator) Values() []tsm1.Value {
	return t.values
}

func (t *TSMBooleanIterator) Series() map[string]struct{} {
	return t.series
}

func (t *TSMBooleanIterator) NextPoint() *query.BooleanPoint {
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
	case tsm1.BooleanValue:
		return &query.BooleanPoint{Time: v.UnixNano(), Value: v.Value().(bool)}
	default:
		panic("only BooleanValue allowed")
	}
}

func (t *TSMBooleanIterator) walk() {
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

func (t *TSMBooleanIterator) Stats() query.IteratorStats {
	if len(t.series) == 0 || t.pointN == 0 {
		t.walk()
	}
	stats := query.IteratorStats{
		SeriesN: len(t.series),
		PointN:  t.pointN,
	}
	return stats
}

func (t *TSMBooleanIterator) Close() error {
	return nil
}

func (t *TSMBooleanIterator) step() {
	batch := make([]*booleanEntry, 0, t.batchSize)
	for i := 0; i < t.batchSize; i++ {
		p := t.NextPoint()
		if p == nil {
			break
		}
		batch = append(batch, &booleanEntry{key: t.key, point: p})

		s, _ := tsm1.SeriesAndFieldFromCompositeKey(t.key)
		t.series[string(s)] = struct{}{}
		t.pointN++
	}
	t.itr.entriesC <- batch
}

func (t *TSMBooleanIterator) eventLoop() {
	for {
		select {
		case <-t.stopC:
			return
		case <-t.notifyC:
			t.step()
		}
	}
}

func (t *TSMBooleanIterator) Encode(w io.Writer) error {
	entriesC := make(chan []*booleanEntry)
	t.itr = newBooleanIterator(t.notifyC, entriesC)

	go t.eventLoop()
	defer close(t.stopC)

	enc := query.NewIteratorEncoder(w)
	if err := enc.EncodeIterator(t.itr); err != nil {
		return err
	}
	return nil
}

type booleanEntry struct {
	key   []byte
	point *query.BooleanPoint
}

type booleanIterator struct {
	pointN   int
	series   map[string]struct{}
	notifyC  chan struct{}
	entriesC chan []*booleanEntry
	batch    []*booleanEntry
}

func newBooleanIterator(notifyC chan struct{}, entryC chan []*booleanEntry) *booleanIterator {
	return &booleanIterator{
		series:   make(map[string]struct{}),
		notifyC:  notifyC,
		entriesC: entryC,
	}
}

func (itr *booleanIterator) Stats() query.IteratorStats {
	return query.IteratorStats{
		SeriesN: len(itr.series),
		PointN:  itr.pointN,
	}
}

func (itr *booleanIterator) Close() error {
	itr.pointN = 0
	itr.series = nil
	close(itr.notifyC)
	close(itr.entriesC)
	return nil
}

func (itr *booleanIterator) Next() (*query.BooleanPoint, error) {
	if len(itr.batch) == 0 {
		itr.notifyC <- struct{}{}
		select {
		case batch := <-itr.entriesC:
			itr.batch = batch
		}
	}
	if len(itr.batch) == 0 {
		return nil, nil
	}
	p := itr.batch[0]
	itr.batch = itr.batch[1:]
	s, _ := tsm1.SeriesAndFieldFromCompositeKey(p.key)
	itr.series[string(s)] = struct{}{}
	itr.pointN++
	return p.point, nil
}
