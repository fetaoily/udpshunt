//go:build linux

package pktio

import (
	"net"
	"testing"
	"time"
)

func TestLinuxBatchHonorsImmediateDeadline(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	c := Wrap(pc)

	done := make(chan error, 1)
	go func() {
		_ = c.SetReadDeadline(time.Now().Add(time.Hour))
		bufs := make([][]byte, 8)
		for i := range bufs {
			bufs[i] = make([]byte, MaxPacketSize)
		}
		_, err := c.ReceiveBatch(bufs, make([]*net.UDPAddr, 8), make([]int, 8))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	// Simulate the shutdown watcher: an already-expired deadline must
	// unblock a pending batch read.
	_ = c.SetReadDeadline(time.Now())
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected deadline error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending ReceiveBatch was not unblocked by the deadline")
	}
}
