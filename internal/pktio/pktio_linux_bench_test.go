//go:build linux

package pktio

import (
	"net"
	"testing"
)

// benchPair returns the raw receiver socket, the same receiver wrapped for
// batched reads, and a raw sender socket.
func benchPair(b *testing.B) (recv *net.UDPConn, wrapped *Conn, sender *net.UDPConn) {
	b.Helper()
	rpc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { rpc.Close() })
	spc, err := net.DialUDP("udp", nil, rpc.LocalAddr().(*net.UDPAddr))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { spc.Close() })
	return rpc, Wrap(rpc), spc
}

// drainBatch reads want packets through c into caller-owned buffers, so no
// allocation lands inside the timed loop.
func drainBatch(c *Conn, bufs [][]byte, addrs []*net.UDPAddr, sizes []int, want int) {
	got := 0
	for got < want {
		n, err := c.ReceiveBatch(bufs, addrs, sizes)
		if err != nil {
			continue
		}
		got += n
	}
}

func BenchmarkReceiveBatch64(b *testing.B) {
	_, c, sender := benchPair(b)
	payload := make([]byte, 512)
	bufs := make([][]byte, 64)
	for i := range bufs {
		bufs[i] = make([]byte, MaxPacketSize)
	}
	addrs := make([]*net.UDPAddr, 64)
	sizes := make([]int, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i += 64 {
		end := 64
		if b.N-i < 64 {
			end = b.N - i
		}
		for j := 0; j < end; j++ {
			if _, err := sender.Write(payload); err != nil {
				b.Fatal(err)
			}
		}
		drainBatch(c, bufs, addrs, sizes, end)
	}
}

func BenchmarkReceiveSingleReadFrom(b *testing.B) {
	rpc, _, sender := benchPair(b)
	payload := make([]byte, 512)
	buf := make([]byte, MaxPacketSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sender.Write(payload); err != nil {
			b.Fatal(err)
		}
		if _, _, err := rpc.ReadFromUDP(buf); err != nil {
			b.Fatal(err)
		}
	}
}
