//go:build linux

package pktio

import (
	"net"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// maxBatchSize is the largest batch ReceiveBatch accepts; the reused message
// slices are pre-allocated for it.
const maxBatchSize = 64

// linuxReceiver drains the socket with one recvmmsg per call.
type linuxReceiver struct {
	v4 *ipv4.PacketConn
	v6 *ipv6.PacketConn

	// ReceiveBatch is single-goroutine per Conn; msgs is reused across calls.
	msgs4 []ipv4.Message
	msgs6 []ipv6.Message
}

func newBatchReceiver(pc *net.UDPConn) batchReceiver {
	addr := pc.LocalAddr().(*net.UDPAddr)
	if addr.IP.To4() != nil {
		r := &linuxReceiver{v4: ipv4.NewPacketConn(pc)}
		r.msgs4 = make([]ipv4.Message, maxBatchSize)
		for i := range r.msgs4 {
			r.msgs4[i].Buffers = make([][]byte, 1)
		}
		return r
	}
	r := &linuxReceiver{v6: ipv6.NewPacketConn(pc)}
	r.msgs6 = make([]ipv6.Message, maxBatchSize)
	for i := range r.msgs6 {
		r.msgs6[i].Buffers = make([][]byte, 1)
	}
	return r
}

func (r *linuxReceiver) ReceiveBatch(pc *net.UDPConn, bufs [][]byte, addrs []*net.UDPAddr, sizes []int) (int, error) {
	if r.v4 != nil {
		msgs := r.msgs4[:min(len(bufs), maxBatchSize)]
		for i := range msgs {
			msgs[i].Buffers[0] = bufs[i]
		}
		n, err := r.v4.ReadBatch(msgs, 0)
		for i := 0; i < n; i++ {
			sizes[i] = msgs[i].N
			if a, ok := msgs[i].Addr.(*net.UDPAddr); ok {
				addrs[i] = a
			}
		}
		return n, err
	}
	msgs := r.msgs6[:min(len(bufs), maxBatchSize)]
	for i := range msgs {
		msgs[i].Buffers[0] = bufs[i]
	}
	n, err := r.v6.ReadBatch(msgs, 0)
	for i := 0; i < n; i++ {
		sizes[i] = msgs[i].N
		if a, ok := msgs[i].Addr.(*net.UDPAddr); ok {
			addrs[i] = a
		}
	}
	return n, err
}
