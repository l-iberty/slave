package main

import "testing"

func Test_extractGroupIdAndPeerIdFromFilename(t *testing.T) {
	s1 := "000001_1_100.tsm"
	s2 := "000001_100_1.tsm"
	x, y := extractGroupIdAndPeerIdFromFilename(s1)
	if x != 1 || y != 100 {
		t.Errorf("error")
	}
	x, y = extractGroupIdAndPeerIdFromFilename(s2)
	if x != 100 || y != 1 {
		t.Errorf("error")
	}
}
