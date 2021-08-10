package main

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	HeartbeatInterval = 1 * time.Second
	HeartbeatKey      = "/heartbeat"
)

type Slave struct {
	sync.RWMutex
	id          int
	host        string
	port        int
	masterHosts []string
	workdir     string
	files       map[string]FileStore
	stopC       <-chan struct{}
}

func (s *Slave) putFile(fs FileStore) {
	s.Lock()
	defer s.Unlock()
	s.files[fs.Filename] = fs
}

func (s *Slave) sendHeartbeat() {
	s.Lock()
	defer s.Unlock()

	var files []FileStore
	for _, fs := range s.files {
		files = append(files, fs)
	}
	p := heartbeatPackage{
		SlaveId:    s.id,
		SlaveHost:  s.host,
		FileStores: files,
	}
	reqBody := p.mustMarshal()
	for i := range s.masterHosts {
		url := s.masterHosts[i] + HeartbeatKey
		if ok := http_put(url, reqBody); ok {
			// log.Printf("[slave %d] successfully sending heartbeat to master %s", s.id, s.masterHosts[i])
			return
		}
		// log.Printf("[slave %d] failed sending heartbeat to master %s", s.id, s.masterHosts[i])
	}
}

func newSlave(id int, host string, port int, masterHosts []string, stopC chan struct{}) *Slave {
	s := &Slave{
		id:          id,
		host:        host,
		port:        port,
		masterHosts: masterHosts,
		workdir:     "slave" + strconv.Itoa(id),
		files:       make(map[string]FileStore),
		stopC:       stopC,
	}

	os.Mkdir(s.workdir, os.ModePerm)

	go func() {
		for {
			select {
			case <-time.After(HeartbeatInterval):
				s.sendHeartbeat()
			}
		}
	}()

	go func() {
		l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err != nil {
			panic(err)
		}
		defer l.Close()

		log.Printf("server listening at %d for receiving file", port)

		for {
			conn, err := l.Accept()
			if err != nil {
				panic(err)
			}
			log.Printf("accepted a connection from %s", conn.RemoteAddr())

			go s.startRecvingFile(conn)
		}
	}()

	return s
}

func (s *Slave) startRecvingFile(conn net.Conn) {
	defer conn.Close()
	// receiving filename
	buf := make([]byte, 1024)
	n := mustRead(conn, buf)
	filename := string(buf[:n])
	log.Printf("received filename \"%s\" from %s, responsing \"OK\"", filename, conn.RemoteAddr())
	mustWrite(conn, []byte("OK"))

	// receiving md5 sum
	n = mustRead(conn, buf)
	md5sum := make([]byte, n)
	copy(md5sum, buf[:n])
	log.Printf("[%s - %s] received md5 sum, responsing \"OK\"", conn.RemoteAddr(), filename)
	mustWrite(conn, []byte("OK"))

	// receiving file data
	log.Printf("[%s - %s] receiving file data", conn.RemoteAddr(), filename)
	mymd5sum, err := doRecvFile(s.workdir, filename, conn)
	if err != nil {
		panic(err)
	}
	// verifying md5 sum
	if !reflect.DeepEqual(mymd5sum, md5sum) {
		log.Printf("[%s - %s] ERROR - md5 verification failed, expected %s, got %s",
			conn.RemoteAddr(), filename, hex.EncodeToString(md5sum), hex.EncodeToString(mymd5sum))
		return
	}

	log.Printf("[%s - %s] DONE - md5 verification successful", conn.RemoteAddr(), filename)
	groupId, peerId := extractGroupIdAndPeerIdFromFilename(filename)
	fs := FileStore{
		GroupId:  groupId,
		PeerId:   peerId,
		Filename: filename,
		Md5sum:   hex.EncodeToString(md5sum),
	}
	s.putFile(fs)
}

func http_put(url string, reqBody string) bool {
	req, err := http.NewRequest("PUT", url, strings.NewReader(reqBody))
	if err != nil {
		// log.Printf("failed to create http request: %v", err)
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// log.Printf("http request error: %v", err)
		return false
	}
	defer resp.Body.Close()
	return true
}

func doRecvFile(dir string, filename string, conn net.Conn) ([]byte, error) {
	f, err := os.Create(filepath.Join(dir, filename))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, 10*1024*1024)
	for {
		n, err := conn.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 && err == io.EOF {
			break
		}
		n, err = f.Write(buf[:n])
		if err != nil {
			return nil, err
		}
	}
	md5sum := mustMd5sum(f)
	return md5sum, nil
}

// filename: ****_X_Y.***, X = groupId, Y = peerId
func extractGroupIdAndPeerIdFromFilename(filename string) (int, int) {
	toks := strings.Split(filename, "_")
	groupId, _ := strconv.Atoi(toks[1])
	toks2 := strings.Split(toks[2], ".")
	peerId, _ := strconv.Atoi(toks2[0])
	return groupId, peerId
}

func mustRead(conn net.Conn, buf []byte) int {
	n, err := conn.Read(buf)
	if err != nil {
		panic(err)
	}
	return n
}

func mustWrite(conn net.Conn, buf []byte) int {
	n, err := conn.Write(buf)
	if err != nil {
		panic(err)
	}
	return n
}

func mustMd5sum(f *os.File) []byte {
	if _, err := f.Seek(0, 0); err != nil {
		panic(err)
	}
	md5sum := md5.New()
	if _, err := io.Copy(md5sum, f); err != nil {
		panic(err)
	}
	return md5sum.Sum(nil)
}
