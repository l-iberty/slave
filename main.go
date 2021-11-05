package main

import (
	"flag"
	"log"
	"strings"
)

func main() {
	id := flag.Int("id", 1, "slave ID")
	host := flag.String("host", "192.168.254.129", "slave host")
	port := flag.Int("port", 7890, "tcp listening port for receiving file")
	cmdport := flag.Int("cmdport", 7880, "tcp listening port for receiving master's files-copy command")
	masterAddrs := flag.String("masterAddrs", "http://192.168.254.128:12380,http://192.168.254.128:22380,http://192.168.254.128:32380", "comma separated master addrs")
	flag.Parse()

	log.Printf("id: %d, addr: %s:%d, masterAddrs: %s", *id, *host, *port, *masterAddrs)

	stopC := make(chan struct{})
	newSlave(*id, *host, *port, *cmdport, strings.Split(*masterAddrs, ","), stopC)
	<-stopC
}
