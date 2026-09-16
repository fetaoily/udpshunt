//go:build linux

package pktio

import (
	"net"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// linuxReceiver drains the socket with one recvmmsg per call.
type linuxReceiver struct {
	v4 *ipv4.PacketConn
	v6 *ipv6.PacketConn
}

func newBatchReceiver(pc *net.UDPConn) batchReceiver {
	addr := pc.LocalAddr().(*net.UDPAddr)
	if addr.IP.To4() != nil {
		return &linuxReceiver{v4: ipv4.NewPacketConn(pc)}
	}
	return &linuxReceiver{v6: ipv6.NewPacketConn(pc)}
}

func (r *linuxReceiver) ReceiveBatch(pc *net.UDPConn, bufs [][]byte, addrs []*net.UDPAddr, sizes []int) (int, error) {
	if r.v4 != nil {
		msgs := make([]ipv4.Message, len(bufs))
		for i := range msgs {
			msgs[i].Buffers = [][]byte{bufs[i]}
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
	msgs := make([]ipv6.Message, len(bufs))
	for i := range msgs {
		msgs[i].Buffers = [][]byte{bufs[i]}
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
