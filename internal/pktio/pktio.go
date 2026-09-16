// Package pktio wraps UDP sockets with platform-optimal receive batching:
// one recvmmsg per wakeup on Linux, one packet per wakeup elsewhere.
package pktio

import (
	"net"
	"time"
)

// MaxPacketSize is the largest UDP payload a buffer must hold.
const MaxPacketSize = 65536

// Conn wraps a frontend UDP socket for batched receiving.
type Conn struct {
	pc    *net.UDPConn
	batch batchReceiver
}

// batchReceiver is the platform receive implementation (build-tagged files).
type batchReceiver interface {
	// ReceiveBatch blocks until at least one packet is available, then
	// reads up to len(bufs) packets. addrs and sizes receive the source
	// address and payload length of every packet. It honors the socket's
	// read deadline.
	ReceiveBatch(pc *net.UDPConn, bufs [][]byte, addrs []*net.UDPAddr, sizes []int) (n int, err error)
}

// Wrap prepares pc for batched receiving.
func Wrap(pc *net.UDPConn) *Conn {
	return &Conn{pc: pc, batch: newBatchReceiver(pc)}
}

// ReceiveBatch reads a platform-optimal batch of packets.
func (c *Conn) ReceiveBatch(bufs [][]byte, addrs []*net.UDPAddr, sizes []int) (int, error) {
	return c.batch.ReceiveBatch(c.pc, bufs, addrs, sizes)
}

// WriteToUDP sends one packet (delegation to the wrapped socket).
func (c *Conn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	return c.pc.WriteToUDP(b, addr)
}

// LocalAddr returns the bound address.
func (c *Conn) LocalAddr() net.Addr { return c.pc.LocalAddr() }

// SetReadDeadline sets the read deadline on the wrapped socket.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.pc.SetReadDeadline(t) }

// Close closes the wrapped socket.
func (c *Conn) Close() error { return c.pc.Close() }
