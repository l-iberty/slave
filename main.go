package main

import (
	"flag"
	"log"
	"strings"
)

func main() {
	id := flag.Int("id", 1, "slave ID")
	host := flag.String("host", "127.0.0.1", "slave host")
	port := flag.Int("port", 7890, "tcp listening port for receiving file")
	cmdport := flag.Int("cmdport", 7880, "tcp listening port for receiving master's files-copy command")
	masterAddrs := flag.String("masterAddrs", "http://127.0.0.1:12380,http://127.0.0.1:22380,http://127.0.0.1:32380", "comma separated master addrs")
	flag.Parse()

	log.Printf("id: %d, addr: %s:%d, masterAddrs: %s", *id, *host, *port, *masterAddrs)

	stopC := make(chan struct{})
	s := NewSlave(*id, *host, *port, *cmdport, strings.Split(*masterAddrs, ","), stopC)
	s.Run()

	select {
	case <-stopC:
		log.Print("stop")
		return
	}
}
