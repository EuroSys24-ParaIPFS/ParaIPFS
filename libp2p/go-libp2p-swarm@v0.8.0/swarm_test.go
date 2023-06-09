package swarm_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/control"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

	swarm "github.com/libp2p/go-libp2p-swarm"
	. "github.com/libp2p/go-libp2p-swarm/testing"

	logging "github.com/ipfs/go-log"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/stretchr/testify/require"
)

var log = logging.Logger("swarm_test")

func EchoStreamHandler(stream network.Stream) {
	go func() {
		defer stream.Close()

		// pull out the ipfs conn
		c := stream.Conn()
		log.Infof("%s ponging to %s", c.LocalPeer(), c.RemotePeer())

		buf := make([]byte, 4)

		for {
			if _, err := stream.Read(buf); err != nil {
				if err != io.EOF {
					log.Error("ping receive error:", err)
				}
				return
			}

			if !bytes.Equal(buf, []byte("ping")) {
				log.Errorf("ping receive error: ping != %s %v", buf, buf)
				return
			}

			if _, err := stream.Write([]byte("pong")); err != nil {
				log.Error("pond send error:", err)
				return
			}
		}
	}()
}

func makeDialOnlySwarm(t *testing.T) *swarm.Swarm {
	swarm := GenSwarm(t, OptDialOnly)
	swarm.SetStreamHandler(EchoStreamHandler)
	return swarm
}

func makeSwarms(t *testing.T, num int, opts ...Option) []*swarm.Swarm {
	swarms := make([]*swarm.Swarm, 0, num)
	for i := 0; i < num; i++ {
		swarm := GenSwarm(t, opts...)
		swarm.SetStreamHandler(EchoStreamHandler)
		swarms = append(swarms, swarm)
	}
	return swarms
}

func connectSwarms(t *testing.T, ctx context.Context, swarms []*swarm.Swarm) {
	var wg sync.WaitGroup
	connect := func(s *swarm.Swarm, dst peer.ID, addr ma.Multiaddr) {
		// TODO: make a DialAddr func.
		s.Peerstore().AddAddr(dst, addr, peerstore.PermanentAddrTTL)
		if _, err := s.DialPeer(ctx, dst); err != nil {
			t.Fatal("error swarm dialing to peer", err)
		}
		wg.Done()
	}

	log.Info("Connecting swarms simultaneously.")
	for i, s1 := range swarms {
		for _, s2 := range swarms[i+1:] {
			wg.Add(1)
			connect(s1, s2.LocalPeer(), s2.ListenAddresses()[0]) // try the first.
		}
	}
	wg.Wait()

	for _, s := range swarms {
		log.Infof("%s swarm routing table: %s", s.LocalPeer(), s.Peers())
	}
}

func SubtestSwarm(t *testing.T, SwarmNum int, MsgNum int) {
	swarms := makeSwarms(t, SwarmNum, OptDisableReuseport)

	// connect everyone
	connectSwarms(t, context.Background(), swarms)

	// ping/pong
	for _, s1 := range swarms {
		log.Debugf("-------------------------------------------------------")
		log.Debugf("%s ping pong round", s1.LocalPeer())
		log.Debugf("-------------------------------------------------------")

		_, cancel := context.WithCancel(context.Background())
		got := map[peer.ID]int{}
		errChan := make(chan error, MsgNum*len(swarms))
		streamChan := make(chan network.Stream, MsgNum)

		// send out "ping" x MsgNum to every peer
		go func() {
			defer close(streamChan)

			var wg sync.WaitGroup
			send := func(p peer.ID) {
				defer wg.Done()

				// first, one stream per peer (nice)
				stream, err := s1.NewStream(context.Background(), p)
				if err != nil {
					errChan <- err
					return
				}

				// send out ping!
				for k := 0; k < MsgNum; k++ { // with k messages
					msg := "ping"
					log.Debugf("%s %s %s (%d)", s1.LocalPeer(), msg, p, k)
					if _, err := stream.Write([]byte(msg)); err != nil {
						errChan <- err
						continue
					}
				}

				// read it later
				streamChan <- stream
			}

			for _, s2 := range swarms {
				if s2.LocalPeer() == s1.LocalPeer() {
					continue // dont send to self...
				}

				wg.Add(1)
				go send(s2.LocalPeer())
			}
			wg.Wait()
		}()

		// receive "pong" x MsgNum from every peer
		go func() {
			defer close(errChan)
			count := 0
			countShouldBe := MsgNum * (len(swarms) - 1)
			for stream := range streamChan { // one per peer
				// get peer on the other side
				p := stream.Conn().RemotePeer()

				// receive pings
				msgCount := 0
				msg := make([]byte, 4)
				for k := 0; k < MsgNum; k++ { // with k messages

					// read from the stream
					if _, err := stream.Read(msg); err != nil {
						errChan <- err
						continue
					}

					if string(msg) != "pong" {
						errChan <- fmt.Errorf("unexpected message: %s", msg)
						continue
					}

					log.Debugf("%s %s %s (%d)", s1.LocalPeer(), msg, p, k)
					msgCount++
				}

				got[p] = msgCount
				count += msgCount
				stream.Close()
			}

			if count != countShouldBe {
				errChan <- fmt.Errorf("count mismatch: %d != %d", count, countShouldBe)
			}
		}()

		// check any errors (blocks till consumer is done)
		for err := range errChan {
			if err != nil {
				t.Error(err.Error())
			}
		}

		log.Debugf("%s got pongs", s1.LocalPeer())
		if (len(swarms) - 1) != len(got) {
			t.Errorf("got (%d) less messages than sent (%d).", len(got), len(swarms))
		}

		for p, n := range got {
			if n != MsgNum {
				t.Error("peer did not get all msgs", p, n, "/", MsgNum)
			}
		}

		cancel()
		<-time.After(10 * time.Millisecond)
	}

	for _, s := range swarms {
		s.Close()
	}
}

func TestSwarm(t *testing.T) {
	// t.Skip("skipping for another test")
	t.Parallel()

	// msgs := 1000
	msgs := 100
	swarms := 5
	SubtestSwarm(t, swarms, msgs)
}

func TestBasicSwarm(t *testing.T) {
	// t.Skip("skipping for another test")
	t.Parallel()

	msgs := 1
	swarms := 2
	SubtestSwarm(t, swarms, msgs)
}

func TestConnectionGating(t *testing.T) {
	ctx := context.Background()
	tcs := map[string]struct {
		p1Gater func(gater *MockConnectionGater) *MockConnectionGater
		p2Gater func(gater *MockConnectionGater) *MockConnectionGater

		p1ConnectednessToP2 network.Connectedness
		p2ConnectednessToP1 network.Connectedness
		isP1OutboundErr     bool
		disableOnQUIC       bool
	}{
		"no gating": {
			p1ConnectednessToP2: network.Connected,
			p2ConnectednessToP1: network.Connected,
			isP1OutboundErr:     false,
		},
		"p1 gates outbound peer dial": {
			p1Gater: func(c *MockConnectionGater) *MockConnectionGater {
				c.PeerDial = func(p peer.ID) bool { return false }
				return c
			},
			p1ConnectednessToP2: network.NotConnected,
			p2ConnectednessToP1: network.NotConnected,
			isP1OutboundErr:     true,
		},
		"p1 gates outbound addr dialing": {
			p1Gater: func(c *MockConnectionGater) *MockConnectionGater {
				c.Dial = func(p peer.ID, addr ma.Multiaddr) bool { return false }
				return c
			},
			p1ConnectednessToP2: network.NotConnected,
			p2ConnectednessToP1: network.NotConnected,
			isP1OutboundErr:     true,
		},
		"p2 accepts inbound peer dial if outgoing dial is gated": {
			p2Gater: func(c *MockConnectionGater) *MockConnectionGater {
				c.Dial = func(peer.ID, ma.Multiaddr) bool { return false }
				return c
			},
			p1ConnectednessToP2: network.Connected,
			p2ConnectednessToP1: network.Connected,
			isP1OutboundErr:     false,
		},
		"p2 gates inbound peer dial before securing": {
			p2Gater: func(c *MockConnectionGater) *MockConnectionGater {
				c.Accept = func(c network.ConnMultiaddrs) bool { return false }
				return c
			},
			p1ConnectednessToP2: network.NotConnected,
			p2ConnectednessToP1: network.NotConnected,
			isP1OutboundErr:     true,
			// QUIC gates the connection after completion of the handshake
			disableOnQUIC: true,
		},
		"p2 gates inbound peer dial before multiplexing": {
			p1Gater: func(c *MockConnectionGater) *MockConnectionGater {
				c.Secured = func(network.Direction, peer.ID, network.ConnMultiaddrs) bool { return false }
				return c
			},
			p1ConnectednessToP2: network.NotConnected,
			p2ConnectednessToP1: network.NotConnected,
			isP1OutboundErr:     true,
		},
		"p2 gates inbound peer dial after upgrading": {
			p1Gater: func(c *MockConnectionGater) *MockConnectionGater {
				c.Upgraded = func(c network.Conn) (bool, control.DisconnectReason) { return false, 0 }
				return c
			},
			p1ConnectednessToP2: network.NotConnected,
			p2ConnectednessToP1: network.NotConnected,
			isP1OutboundErr:     true,
		},
		"p2 gates outbound dials": {
			p2Gater: func(c *MockConnectionGater) *MockConnectionGater {
				c.PeerDial = func(p peer.ID) bool { return false }
				return c
			},
			p1ConnectednessToP2: network.Connected,
			p2ConnectednessToP1: network.Connected,
			isP1OutboundErr:     false,
		},
	}

	for n, tc := range tcs {
		for _, useQuic := range []bool{false, true} {
			trString := "TCP"
			optTransport := OptDisableQUIC
			if useQuic {
				if tc.disableOnQUIC {
					continue
				}
				trString = "QUIC"
				optTransport = OptDisableTCP
			}
			t.Run(fmt.Sprintf("%s %s", n, trString), func(t *testing.T) {
				p1Gater := DefaultMockConnectionGater()
				p2Gater := DefaultMockConnectionGater()
				if tc.p1Gater != nil {
					p1Gater = tc.p1Gater(p1Gater)
				}
				if tc.p2Gater != nil {
					p2Gater = tc.p2Gater(p2Gater)
				}

				sw1 := GenSwarm(t, OptConnGater(p1Gater), optTransport)
				sw2 := GenSwarm(t, OptConnGater(p2Gater), optTransport)

				p1 := sw1.LocalPeer()
				p2 := sw2.LocalPeer()
				sw1.Peerstore().AddAddr(p2, sw2.ListenAddresses()[0], peerstore.PermanentAddrTTL)
				// 1 -> 2
				_, err := sw1.DialPeer(ctx, p2)

				require.Equal(t, tc.isP1OutboundErr, err != nil, n)
				require.Equal(t, tc.p1ConnectednessToP2, sw1.Connectedness(p2), n)

				require.Eventually(t, func() bool {
					return tc.p2ConnectednessToP1 == sw2.Connectedness(p1)
				}, 2*time.Second, 100*time.Millisecond, n)
			})
		}
	}
}

func TestNoDial(t *testing.T) {
	swarms := makeSwarms(t, 2)

	_, err := swarms[0].NewStream(network.WithNoDial(context.Background(), "swarm test"), swarms[1].LocalPeer())
	if err != network.ErrNoConn {
		t.Fatal("should have failed with ErrNoConn")
	}
}

func TestCloseWithOpenStreams(t *testing.T) {
	ctx := context.Background()
	swarms := makeSwarms(t, 2)
	connectSwarms(t, ctx, swarms)

	s, err := swarms[0].NewStream(ctx, swarms[1].LocalPeer())
	require.NoError(t, err)
	defer s.Close()
	// close swarm before stream.
	require.NoError(t, swarms[0].Close())
}

func TestTypedNilConn(t *testing.T) {
	s := GenSwarm(t)
	defer s.Close()

	// We can't dial ourselves.
	c, err := s.DialPeer(context.Background(), s.LocalPeer())
	require.Error(t, err)
	// If we fail to dial, the connection should be nil.
	require.Nil(t, c)
}

func TestPreventDialListenAddr(t *testing.T) {
	s := GenSwarm(t, OptDialOnly)
	if err := s.Listen(ma.StringCast("/ip4/0.0.0.0/udp/0/quic")); err != nil {
		t.Fatal(err)
	}
	addrs, err := s.InterfaceListenAddresses()
	if err != nil {
		t.Fatal(err)
	}
	var addr ma.Multiaddr
	for _, a := range addrs {
		_, s, err := manet.DialArgs(a)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Split(s, ":")[0] == "127.0.0.1" {
			addr = a
			break
		}
	}
	remote := peer.ID("foobar")
	s.Peerstore().AddAddr(remote, addr, time.Hour)
	_, err = s.DialPeer(context.Background(), remote)
	if !errors.Is(err, swarm.ErrNoGoodAddresses) {
		t.Fatal("expected dial to fail: %w", err)
	}
}
