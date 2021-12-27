package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	HeartbeatInterval = 1000 * time.Millisecond
	HeartbeatKey      = "/heartbeat"
	InfoFileName      = "info.json"
	DataFileName      = "data.txt"
	BackupDirName     = "bak"
	DataDirName       = "data"
)

type Slave struct {
	mu          sync.RWMutex
	fileMu      sync.Mutex
	id          int
	host        string
	port        int
	cmdport     int
	masterAddrs []string
	workdir     string
	files       map[string]FileStore
	stopC       chan<- struct{}
}

func NewSlave(id int, host string, port int, cmdport int, masterAddrs []string, stopC chan struct{}) *Slave {
	s := &Slave{
		id:          id,
		host:        host,
		port:        port,
		cmdport:     cmdport,
		masterAddrs: masterAddrs,
		workdir:     "slave" + strconv.Itoa(id),
		files:       make(map[string]FileStore),
		stopC:       stopC,
	}
	os.Mkdir(s.workdir, os.ModePerm)

	return s
}

func (s *Slave) Run() {
	// infoFilePath := filepath.Join(s.workdir, InfoFileName)
	// if pathExist(infoFilePath) {
	// 	infoFile, err := os.OpenFile(infoFilePath, os.O_RDONLY, 0664)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer infoFile.Close()

	// 	dec := json.NewDecoder(infoFile)
	// 	info := map[string]int{}
	// 	if err := dec.Decode(&info); err != nil {
	// 		panic(err)
	// 	}

	// 	dir, err := ioutil.ReadDir(s.workdir)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	for _, fi := range dir {
	// 		if fi.IsDir() {
	// 			continue
	// 		}
	// 		if getExtname(fi.Name()) != ".tsm" {
	// 			continue
	// 		}
	// 		path := filepath.Join(s.workdir, fi.Name())
	// 		file, err := os.OpenFile(path, os.O_RDONLY, 0664)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		fs := FileStore{
	// 			GroupId:  info["groupId"],
	// 			PeerId:   info["peerId"],
	// 			Filename: fi.Name(),
	// 			Md5sum:   hex.EncodeToString(mustMd5sum(file)),
	// 		}
	// 		s.files[fs.Filename] = fs
	// 		file.Close()
	// 	}
	// }

	go func() {
		for {
			select {
			case <-time.After(HeartbeatInterval):
				s.sendHeartbeat()
			}
		}
	}()

	go func() {
		l, err := net.Listen("tcp", ":"+strconv.Itoa(s.port))
		if err != nil {
			panic(err)
		}
		defer l.Close()

		log.Printf("server listening at %d for receiving file", s.port)

		for {
			conn, err := l.Accept()
			if err != nil {
				panic(err)
			}
			log.Printf("accepted a connection from %s (receiving file)", conn.RemoteAddr())

			go s.startRecvingFile(conn)
		}
	}()

	go func() {
		l, err := net.Listen("tcp", ":"+strconv.Itoa(s.cmdport))
		if err != nil {
			panic(err)
		}
		defer l.Close()

		log.Printf("server listening at %d for master's files-copy command", s.cmdport)

		for {
			conn, err := l.Accept()
			if err != nil {
				panic(err)
			}
			log.Printf("accepted a connection from %s (files-copy command)", conn.RemoteAddr())

			go s.startSendingFilesToPeer(conn)
		}
	}()

	go func() {
		for {
			select {
			case <-time.After(5 * time.Second):
				s.backupFileToInfluxDB()
			}
		}
	}()
}

func (s *Slave) sendHeartbeat() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var files []FileStore
	for _, fs := range s.files {
		files = append(files, fs)
	}
	p := heartbeatPackage{
		SlaveId:      s.id,
		SlaveHost:    s.host,
		SlavePort:    s.port,
		SlaveCmdPort: s.cmdport,
		FileStores:   files,
	}
	reqBody := p.mustMarshal()
	var success int
	for i := range s.masterAddrs {
		url := s.masterAddrs[i] + HeartbeatKey
		if err := http_put(url, reqBody); err == nil {
			success++
			return
		}
	}
	if success == 0 {
		log.Printf("WARNING: [slave %d] failed sending heartbeat to masters [%v]", s.id, s.masterAddrs)
	}
}

func (s *Slave) startRecvingFile(conn net.Conn) {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	defer conn.Close()

	// receiving filename
	buf := make([]byte, 1024)
	n := mustRead(conn, buf)
	filename := string(buf[:n])
	filename = correctSeparator(filename)
	dir, basename, groupId, peerId := parseFilename(filename)
	filename = filepath.Join(dir, basename)
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
	fs := FileStore{
		GroupId:  groupId,
		PeerId:   peerId,
		Filename: filename,
		Md5sum:   hex.EncodeToString(md5sum),
	}
	s.mu.Lock()
	s.files[fs.Filename] = fs
	s.mu.Unlock()

	infoFilePath := filepath.Join(s.workdir, InfoFileName)
	if pathExist(infoFilePath) {
		return // nothing needs to do
	}
	infoFile, err := os.OpenFile(infoFilePath, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		panic(err)
	}
	defer infoFile.Close()

	enc := json.NewEncoder(infoFile)
	info := map[string]int{"groupId": groupId, "peerId": peerId}
	if err := enc.Encode(&info); err != nil {
		panic(err)
	}
}

func (s *Slave) startSendingFilesToPeer(conn net.Conn) {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	defer conn.Close()

	s.mu.RLock()
	files := make(map[string]FileStore)
	for _, f := range s.files {
		files[f.Filename] = f
	}
	s.mu.RUnlock()

	// receiving peer slave addr for sending files
	buf := make([]byte, 100)
	n := mustRead(conn, buf)
	data := string(buf[:n])
	toks := strings.Split(data, "#")
	peerAddr, peerTag := toks[0], toks[1]
	peerGroupId, peerId := nodeTag2GroupIdAndPeerId(peerTag)

	// responding "OK
	mustWrite(conn, []byte("OK"))

	for _, f := range files {
		path := filepath.Join(s.workdir, BackupDirName, f.Filename)
		ext := getExtname(f.Filename)
		if ext != ".tsm" {
			continue
		}
		err := sendFile(path, ext, peerAddr, peerGroupId, peerId)
		if err != nil {
			log.Printf("slave %s unable to send file \"%s\" to the remote peer %s", s.host, path, peerAddr)
		}
	}
}

func (s *Slave) backupFileToInfluxDB() {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()

	currentPath := getCurrentPath()
	datadir := filepath.Join(currentPath, s.workdir, "data")
	if !pathExist(datadir) {
		// log.Printf("datadir %s not found", datadir)
		return
	}
	dataFilePath := filepath.Join(currentPath, s.workdir, DataFileName)

	// make a dummy WAL dir, which is needed by influx_inspect
	waldir := filepath.Join(currentPath, s.workdir, "wal")
	os.Mkdir(waldir, os.ModePerm)

	cmd := exec.Command("./bin/influx_inspect", "export", "-datadir", datadir, "-waldir", waldir, "-out", dataFilePath)
	err := cmd.Run()
	if err != nil {
		log.Printf("Failed to export data file: %v", err)
		return
	}
	log.Print("Succeeded to export data file")

	port, err := getInfluxDBServicePort(s.id, "service_port.json")
	if err != nil {
		log.Printf("Failed to get InfluxDB service port: %v", err)
		return
	}
	cmd = exec.Command("./bin/influx", fmt.Sprintf("-port=%d", port), "-import", fmt.Sprintf("-path=%s", dataFilePath))
	err = cmd.Run()
	if err != nil {
		log.Printf("Failed to import file to InfluxDB: %v", err)
		return
	}
	log.Print("Succeeded to import file to InfluxDB")

	// TSM 已被转移到 influxDB, 将 workdir 里的相关文件删除
	// TODO 修改 s.files 里相关文件的记录项
	dir, _ := ioutil.ReadDir(s.workdir)
	for _, f := range dir {
		if f.Name() == DataDirName {
			os.Mkdir(filepath.Join(s.workdir, BackupDirName), os.ModePerm)
			oldpath := filepath.Join(s.workdir, f.Name())
			newpath := filepath.Join(s.workdir, BackupDirName)
			if err := moveAll(oldpath, newpath); err != nil {
				panic(err)
			}
		} else if f.Name() != BackupDirName &&
			f.Name() != InfoFileName {
			os.RemoveAll(filepath.Join(s.workdir, f.Name()))
		}
	}
}

func moveAll(oldpath, newpath string) error {
	cmd := exec.Command("rsync", "-r", oldpath, newpath)
	if err := cmd.Run(); err != nil {
		return err
	}

	if err := os.RemoveAll(oldpath); err != nil {
		return err
	}
	return nil
}

func getCurrentPath() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	return dir
}

func getInfluxDBServicePort(slaveId int, filename string) (int, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0664)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	data := make(map[string]int)
	if err = dec.Decode(&data); err != nil {
		return 0, err
	}

	port, ok := data[strconv.Itoa(slaveId)]
	if !ok {
		return 0, fmt.Errorf("service port not found")
	}
	return port, nil
}

func getExtname(filename string) string {
	toks := strings.Split(filename, ".")
	return "." + toks[len(toks)-1]
}

func pathExist(path string) bool {
	_, err := os.Stat(path)
	if err == nil { // exist
		return true
	}
	if os.IsNotExist(err) { // not exist
		return false
	}
	panic(err)
}

func http_put(url string, reqBody string) error {
	req, err := http.NewRequest("PUT", url, strings.NewReader(reqBody))
	if err != nil {
		// log.Printf("failed to create http request: %v", err)
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// log.Printf("http request error: %v", err)
		return err
	}
	defer resp.Body.Close()
	return nil
}

func doRecvFile(workDir, filename string, conn net.Conn) ([]byte, error) {
	dir := filepath.Join(workDir, filepath.Dir(filename))
	os.MkdirAll(dir, os.ModePerm)

	f, err := os.Create(filepath.Join(workDir, filename))
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

func sendFile(filename, ext, server string, groupId, peerId int) error {
	conn, err := net.Dial("tcp", server)
	if err != nil {
		return err
	}
	defer conn.Close()

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// sending filename
	idx := strings.Index(filename, DataDirName)
	filename = filename[idx:]
	filename = attachGroupIdAndPeerIdToFilename(filename, ext, groupId, peerId)
	log.Printf("[%s - %s] sending filename ", conn.RemoteAddr(), filename)
	mustWrite(conn, []byte(filename))
	// waiting for response
	log.Printf("[%s - %s] waiting for response \"OK\"", conn.RemoteAddr(), filename)
	buf := make([]byte, 10)
	n := mustRead(conn, buf)
	if string(buf[:n]) != "OK" {
		return errors.New("failed sending filename")
	}

	// sending md5 sum
	log.Printf("[%s - %s] sending md5 sum", conn.RemoteAddr(), filename)
	md5sum := mustMd5sum(f)
	mustWrite(conn, md5sum)
	// waiting for response
	log.Printf("[%s - %s] waiting for response \"OK\"", conn.RemoteAddr(), filename)
	n = mustRead(conn, buf)
	if string(buf[:n]) != "OK" {
		return errors.New("failed sending md5 sum")
	}

	// sending file data
	log.Printf("[%s - %s] sending file data", conn.RemoteAddr(), filename)
	if err := doSendFile(f, conn); err != nil {
		return err
	}
	return nil
}

func doSendFile(f *os.File, conn net.Conn) error {
	if _, err := f.Seek(0, 0); err != nil {
		panic(err)
	}
	buf := make([]byte, 10*1024*1024)
	for {
		n, err := f.Read(buf)
		if err != nil {
			if err == io.EOF && n == 0 {
				return nil
			} else {
				return err
			}
		}
		mustWrite(conn, buf[:n])
	}
}

func attachGroupIdAndPeerIdToFilename(filename string, ext string, groupId int, peerId int) string {
	// xxx.tsm -> xxx_<groupId>_<peerId>.tsm
	name := strings.Split(filename, ext)[0]
	return fmt.Sprintf("%s_%d_%d%s", name, groupId, peerId, ext)
}

func parseFilename(filename string) (dir, basename string, groudId int, peerId int) {
	dir = filepath.Dir(filename)
	basename = filepath.Base(filename)
	toks1 := strings.Split(basename, ".") // toks1[0] = xxx_<GID>_<PeerID>, toks1[1] = yyy
	toks2 := strings.Split(toks1[0], "_") // toks2[0] = xxx, toks2[1] = <GID>, toks2[2] = <PeerID>
	basename = toks2[0] + "." + toks1[1]  // basename = xxx.yyy
	groudId, err := strconv.Atoi(toks2[1])
	if err != nil {
		panic(err)
	}
	peerId, err = strconv.Atoi(toks2[2])
	if err != nil {
		panic(err)
	}
	return
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

func correctSeparator(s string) string {
	sep := fmt.Sprintf("%c", filepath.Separator)
	s = strings.ReplaceAll(s, "\\", sep)
	s = strings.ReplaceAll(s, "/", sep)
	return s
}

func nodeTag2GroupIdAndPeerId(tag string) (groupId int, peerId int) {
	var err error
	toks := strings.Split(tag, "_")
	groupId, err = strconv.Atoi(toks[0])
	if err != nil {
		panic(err)
	}
	peerId, err = strconv.Atoi(toks[1])
	if err != nil {
		panic(err)
	}
	return
}
