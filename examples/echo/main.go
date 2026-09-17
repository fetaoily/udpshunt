// Command echo is a tiny UDP echo server for testing udpshunt. It replies
// to every datagram with the same payload, so the raw health check and
// end-to-end round trips both work out of the box. Start one instance per
// backend:
//
//	go run ./examples/echo -addr 127.0.0.1:19001
//	go run ./examples/echo -addr 127.0.0.1:19002
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19001", "UDP listen address")
	flag.Parse()

	pc, err := net.ListenPacket("udp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("echo server listening on %s", *addr)

	go func() {
		buf := make([]byte, 65536)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			log.Printf("%s -> %q", from, buf[:n])
			if _, err := pc.WriteTo(buf[:n], from); err != nil {
				return
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	pc.Close()
}
