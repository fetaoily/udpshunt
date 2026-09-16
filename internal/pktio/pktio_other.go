//go:build !linux

package pktio

import "net"

// singleReceiver reads one packet per call: identical semantics to the
// pre-pktio receive loop.
type singleReceiver struct{}

func newBatchReceiver(*net.UDPConn) batchReceiver { return singleReceiver{} }

func (singleReceiver) MaxBatch() int { return 1 }

func (singleReceiver) ReceiveBatch(pc *net.UDPConn, bufs [][]byte, addrs []*net.UDPAddr, sizes []int) (int, error) {
	n, addr, err := pc.ReadFromUDP(bufs[0])
	if err != nil {
		return 0, err
	}
	addrs[0] = addr
	sizes[0] = n
	return 1, nil
}
