package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
)

func main() {
	id := flag.Int("id", 1, "slave ID")
	host := flag.String("host", "192.168.254.129", "slave host")
	port := flag.Int("port", 7890, "tcp listening port for receiving file")
	masterHost := flag.String("masterHosts", "http://192.168.254.128:12380,http://192.168.254.128:22380,http://192.168.254.128:32380", "comma separated master hosts")
	flag.Parse()

	log.Printf("id: %d, host: %s:%d, masterHosts: %s", *id, *host, *port, *masterHost)

	stopC := make(chan struct{})
	newSlave(*id, fmt.Sprintf("%s:%d", *host, *port), *port, strings.Split(*masterHost, ","), stopC)
	<-stopC
}
