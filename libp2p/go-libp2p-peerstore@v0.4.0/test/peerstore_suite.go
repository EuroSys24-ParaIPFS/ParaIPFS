package test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"

	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/stretchr/testify/require"
)

var peerstoreSuite = map[string]func(pstore.Peerstore) func(*testing.T){
	"AddrStream":               testAddrStream,
	"GetStreamBeforePeerAdded": testGetStreamBeforePeerAdded,
	"AddStreamDuplicates":      testAddrStreamDuplicates,
	"PeerstoreProtoStore":      testPeerstoreProtoStore,
	"BasicPeerstore":           testBasicPeerstore,
	"Metadata":                 testMetadata,
	"CertifiedAddrBook":        testCertifiedAddrBook,
}

type PeerstoreFactory func() (pstore.Peerstore, func())

func TestPeerstore(t *testing.T, factory PeerstoreFactory) {
	for name, test := range peerstoreSuite {
		// Create a new peerstore.
		ps, closeFunc := factory()

		// Run the test.
		t.Run(name, test(ps))

		// Cleanup.
		if closeFunc != nil {
			closeFunc()
		}
	}
}

func testAddrStream(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		addrs, pid := getAddrs(t, 100), peer.ID("testpeer")
		ps.AddAddrs(pid, addrs[:10], time.Hour)

		ctx, cancel := context.WithCancel(context.Background())
		addrch := ps.AddrStream(ctx, pid)

		// while that subscription is active, publish ten more addrs
		// this tests that it doesnt hang
		for i := 10; i < 20; i++ {
			ps.AddAddr(pid, addrs[i], time.Hour)
		}

		// now receive them (without hanging)
		timeout := time.After(time.Second * 10)
		for i := 0; i < 20; i++ {
			select {
			case <-addrch:
			case <-timeout:
				t.Fatal("timed out")
			}
		}

		// start a second stream
		ctx2, cancel2 := context.WithCancel(context.Background())
		addrch2 := ps.AddrStream(ctx2, pid)

		done := make(chan struct{})
		go func() {
			defer close(done)
			// now send the rest of the addresses
			for _, a := range addrs[20:80] {
				ps.AddAddr(pid, a, time.Hour)
			}
		}()

		// receive some concurrently with the goroutine
		timeout = time.After(time.Second * 10)
		for i := 0; i < 40; i++ {
			select {
			case <-addrch:
			case <-timeout:
			}
		}

		<-done

		// receive some more after waiting for that goroutine to complete
		timeout = time.After(time.Second * 10)
		for i := 0; i < 20; i++ {
			select {
			case <-addrch:
			case <-timeout:
			}
		}

		// now cancel it
		cancel()

		// now check the *second* subscription. We should see 80 addresses.
		for i := 0; i < 80; i++ {
			<-addrch2
		}

		cancel2()

		// and add a few more addresses it doesnt hang afterwards
		for _, a := range addrs[80:] {
			ps.AddAddr(pid, a, time.Hour)
		}
	}
}

func testGetStreamBeforePeerAdded(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		addrs, pid := getAddrs(t, 10), peer.ID("testpeer")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ach := ps.AddrStream(ctx, pid)
		for i := 0; i < 10; i++ {
			ps.AddAddr(pid, addrs[i], time.Hour)
		}

		received := make(map[string]bool)
		var count int

		for i := 0; i < 10; i++ {
			a, ok := <-ach
			if !ok {
				t.Fatal("channel shouldnt be closed yet")
			}
			if a == nil {
				t.Fatal("got a nil address, thats weird")
			}
			count++
			if received[a.String()] {
				t.Fatal("received duplicate address")
			}
			received[a.String()] = true
		}

		select {
		case <-ach:
			t.Fatal("shouldnt have received any more addresses")
		default:
		}

		if count != 10 {
			t.Fatal("should have received exactly ten addresses, got ", count)
		}

		for _, a := range addrs {
			if !received[a.String()] {
				t.Log(received)
				t.Fatalf("expected to receive address %s but didnt", a)
			}
		}
	}
}

func testAddrStreamDuplicates(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		addrs, pid := getAddrs(t, 10), peer.ID("testpeer")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ach := ps.AddrStream(ctx, pid)
		go func() {
			for i := 0; i < 10; i++ {
				ps.AddAddr(pid, addrs[i], time.Hour)
				ps.AddAddr(pid, addrs[rand.Intn(10)], time.Hour)
			}

			// make sure that all addresses get processed before context is cancelled
			time.Sleep(time.Millisecond * 50)
			cancel()
		}()

		received := make(map[string]bool)
		var count int
		for a := range ach {
			if a == nil {
				t.Fatal("got a nil address, thats weird")
			}
			count++
			if received[a.String()] {
				t.Fatal("received duplicate address")
			}
			received[a.String()] = true
		}

		if count != 10 {
			t.Fatal("should have received exactly ten addresses")
		}
	}
}

func testPeerstoreProtoStore(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		p1, protos := peer.ID("TESTPEER"), []string{"a", "b", "c", "d"}

		err := ps.AddProtocols(p1, protos...)
		if err != nil {
			t.Fatal(err)
		}

		out, err := ps.GetProtocols(p1)
		if err != nil {
			t.Fatal(err)
		}

		if len(out) != len(protos) {
			t.Fatal("got wrong number of protocols back")
		}

		sort.Strings(out)
		for i, p := range protos {
			if out[i] != p {
				t.Fatal("got wrong protocol")
			}
		}

		supported, err := ps.SupportsProtocols(p1, "q", "w", "a", "y", "b")
		if err != nil {
			t.Fatal(err)
		}

		if len(supported) != 2 {
			t.Fatal("only expected 2 supported")
		}

		if supported[0] != "a" || supported[1] != "b" {
			t.Fatal("got wrong supported array: ", supported)
		}

		b, err := ps.FirstSupportedProtocol(p1, "q", "w", "a", "y", "b")
		require.NoError(t, err)
		require.Equal(t, "a", b)

		b, err = ps.FirstSupportedProtocol(p1, "q", "x", "z")
		require.NoError(t, err)
		require.Empty(t, b)

		b, err = ps.FirstSupportedProtocol(p1, "a")
		require.NoError(t, err)
		require.Equal(t, "a", b)

		protos = []string{"other", "yet another", "one more"}
		err = ps.SetProtocols(p1, protos...)
		if err != nil {
			t.Fatal(err)
		}

		supported, err = ps.SupportsProtocols(p1, "q", "w", "a", "y", "b")
		if err != nil {
			t.Fatal(err)
		}

		if len(supported) != 0 {
			t.Fatal("none of those protocols should have been supported")
		}

		supported, err = ps.GetProtocols(p1)
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(supported)
		sort.Strings(protos)
		if !reflect.DeepEqual(supported, protos) {
			t.Fatalf("expected previously set protos; expected: %v, have: %v", protos, supported)
		}

		err = ps.RemoveProtocols(p1, protos[:2]...)
		if err != nil {
			t.Fatal(err)
		}

		supported, err = ps.GetProtocols(p1)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(supported, protos[2:]) {
			t.Fatal("expected only one protocol to remain")
		}

		// test bad peer IDs
		badp := peer.ID("")

		err = ps.AddProtocols(badp, protos...)
		if err == nil {
			t.Fatal("expected error when using a bad peer ID")
		}

		_, err = ps.GetProtocols(badp)
		if err == nil || err == pstore.ErrNotFound {
			t.Fatal("expected error when using a bad peer ID")
		}

		_, err = ps.SupportsProtocols(badp, "q", "w", "a", "y", "b")
		if err == nil || err == pstore.ErrNotFound {
			t.Fatal("expected error when using a bad peer ID")
		}

		err = ps.RemoveProtocols(badp)
		if err == nil || err == pstore.ErrNotFound {
			t.Fatal("expected error when using a bad peer ID")
		}
	}
}

func testBasicPeerstore(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		var pids []peer.ID
		addrs := getAddrs(t, 10)

		for _, a := range addrs {
			priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
			if err != nil {
				t.Fatal(err)
			}
			p, err := peer.IDFromPrivateKey(priv)
			if err != nil {
				t.Fatal(err)
			}
			pids = append(pids, p)
			ps.AddAddr(p, a, pstore.PermanentAddrTTL)
		}

		peers := ps.Peers()
		if len(peers) != 10 {
			t.Fatal("expected ten peers, got", len(peers))
		}

		pinfo := ps.PeerInfo(pids[0])
		if !pinfo.Addrs[0].Equal(addrs[0]) {
			t.Fatal("stored wrong address")
		}

		// should fail silently...
		ps.AddAddrs("", addrs, pstore.PermanentAddrTTL)
		ps.Addrs("")
	}
}

func testMetadata(ps pstore.Peerstore) func(t *testing.T) {
	return func(t *testing.T) {
		pids := make([]peer.ID, 10)
		for i := range pids {
			priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
			if err != nil {
				t.Fatal(err)
			}
			p, err := peer.IDFromPrivateKey(priv)
			if err != nil {
				t.Fatal(err)
			}
			pids[i] = p
		}
		for _, p := range pids {
			if err := ps.Put(p, "AgentVersion", "string"); err != nil {
				t.Errorf("failed to put %q: %s", "AgentVersion", err)
			}
			if err := ps.Put(p, "bar", 1); err != nil {
				t.Errorf("failed to put %q: %s", "bar", err)
			}
		}
		for _, p := range pids {
			v, err := ps.Get(p, "AgentVersion")
			if err != nil {
				t.Errorf("failed to find %q: %s", "AgentVersion", err)
				continue
			}
			if v != "string" {
				t.Errorf("expected %q, got %q", "string", p)
				continue
			}

			v, err = ps.Get(p, "bar")
			if err != nil {
				t.Errorf("failed to find %q: %s", "bar", err)
				continue
			}
			if v != 1 {
				t.Errorf("expected %q, got %v", 1, v)
				continue
			}
		}
		if err := ps.Put("", "foobar", "thing"); err == nil {
			t.Errorf("expected error for bad peer ID")
		}
		if _, err := ps.Get("", "foobar"); err == nil || err == pstore.ErrNotFound {
			t.Errorf("expected error for bad peer ID")
		}
	}
}

func testCertifiedAddrBook(ps pstore.Peerstore) func(*testing.T) {
	return func(t *testing.T) {
		_, ok := ps.(pstore.CertifiedAddrBook)
		if !ok {
			t.Error("expected peerstore to implement CertifiedAddrBook interface")
		}
	}
}

func getAddrs(t *testing.T, n int) []ma.Multiaddr {
	var addrs []ma.Multiaddr
	for i := 0; i < n; i++ {
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", i))
		if err != nil {
			t.Fatal(err)
		}

		addrs = append(addrs, a)
	}
	return addrs
}

func TestPeerstoreProtoStoreLimits(t *testing.T, ps pstore.Peerstore, limit int) {
	p := peer.ID("foobar")
	protocols := make([]string, limit)
	for i := 0; i < limit; i++ {
		protocols[i] = fmt.Sprintf("protocol %d", i)
	}

	t.Run("setting protocols", func(t *testing.T) {
		require.NoError(t, ps.SetProtocols(p, protocols...))
		require.EqualError(t, ps.SetProtocols(p, append(protocols, "proto")...), "too many protocols")
	})
	t.Run("adding protocols", func(t *testing.T) {
		p1 := protocols[:limit/2]
		p2 := protocols[limit/2:]
		require.NoError(t, ps.SetProtocols(p, p1...))
		require.NoError(t, ps.AddProtocols(p, p2...))
		require.EqualError(t, ps.AddProtocols(p, "proto"), "too many protocols")
	})
}
