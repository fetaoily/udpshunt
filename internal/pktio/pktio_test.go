package pktio

import (
	"net"
	"testing"
	"time"
)

// startEcho binds a UDP echo server and returns its address.
func startEcho(t *testing.T) *net.UDPAddr {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buf := make([]byte, MaxPacketSize)
		for {
			n, from, err := pc.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = pc.WriteToUDP(buf[:n], from)
		}
	}()
	t.Cleanup(func() { pc.Close() })
	return pc.LocalAddr().(*net.UDPAddr)
}

func TestReceiveBatchRoundTrip(t *testing.T) {
	echo := startEcho(t)
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	c := Wrap(pc)

	bufs := make([][]byte, 4)
	for i := range bufs {
		bufs[i] = make([]byte, MaxPacketSize)
	}
	addrs := make([]*net.UDPAddr, 4)
	sizes := make([]int, 4)

	payload := []byte("hello")
	if _, err := pc.WriteToUDP(payload, echo); err != nil {
		t.Fatal(err)
	}
	n, err := c.ReceiveBatch(bufs, addrs, sizes)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("n = %d", n)
	}
	if string(bufs[0][:sizes[0]]) != string(payload) {
		t.Fatalf("payload = %q", bufs[0][:sizes[0]])
	}
	if addrs[0].String() != echo.String() {
		t.Fatalf("addr = %v", addrs[0])
	}
}

func TestReceiveBatchRespectsDeadline(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	c := Wrap(pc)

	if err := c.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	bufs := [][]byte{make([]byte, MaxPacketSize)}
	addrs := make([]*net.UDPAddr, 1)
	sizes := make([]int, 1)
	start := time.Now()
	_, err = c.ReceiveBatch(bufs, addrs, sizes)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("deadline not honored: %v", elapsed)
	}
}
