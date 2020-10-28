package dspinner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	bs "github.com/ipfs/go-blockservice"
	mdag "github.com/ipfs/go-merkledag"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipfspin "github.com/ipfs/go-ipfs-pinner"
	"github.com/ipfs/go-ipfs-pinner/ipldpinner"
	util "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"
)

var rand = util.NewTimeSeededRand()

func randNode() (*mdag.ProtoNode, cid.Cid) {
	nd := new(mdag.ProtoNode)
	nd.SetData(make([]byte, 32))
	_, err := io.ReadFull(rand, nd.Data())
	if err != nil {
		panic(err)
	}
	k := nd.Cid()
	return nd, k
}

func assertPinned(t *testing.T, p ipfspin.Pinner, c cid.Cid, failmsg string) {
	_, pinned, err := p.IsPinned(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	if !pinned {
		t.Fatal(failmsg)
	}
}

func assertUnpinned(t *testing.T, p ipfspin.Pinner, c cid.Cid, failmsg string) {
	_, pinned, err := p.IsPinned(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	if pinned {
		t.Fatal(failmsg)
	}
}

func TestPinnerBasic(t *testing.T) {
	ctx := context.Background()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	p := New(dstore, dserv)

	a, ak := randNode()
	err := dserv.Add(ctx, a)
	if err != nil {
		t.Fatal(err)
	}

	// Pin A{}
	err = p.Pin(ctx, a, false)
	if err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, ak, "Failed to find key")

	// create new node c, to be indirectly pinned through b
	c, _ := randNode()
	err = dserv.Add(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	ck := c.Cid()

	// Create new node b, to be parent to a and c
	b, _ := randNode()
	err = b.AddNodeLink("child", a)
	if err != nil {
		t.Fatal(err)
	}

	err = b.AddNodeLink("otherchild", c)
	if err != nil {
		t.Fatal(err)
	}

	err = dserv.Add(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	bk := b.Cid()

	// recursively pin B{A,C}
	err = p.Pin(ctx, b, true)
	if err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, ck, "child of recursively pinned node not found")

	assertPinned(t, p, bk, "Recursively pinned node not found..")

	d, _ := randNode()
	_ = d.AddNodeLink("a", a)
	_ = d.AddNodeLink("c", c)

	e, _ := randNode()
	_ = d.AddNodeLink("e", e)

	// Must be in dagserv for unpin to work
	err = dserv.Add(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	err = dserv.Add(ctx, d)
	if err != nil {
		t.Fatal(err)
	}

	// Add D{A,C,E}
	err = p.Pin(ctx, d, true)
	if err != nil {
		t.Fatal(err)
	}

	dk := d.Cid()
	assertPinned(t, p, dk, "pinned node not found.")

	// Test recursive unpin
	err = p.Unpin(ctx, dk, true)
	if err != nil {
		t.Fatal(err)
	}

	err = p.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	np, err := LoadPinner(dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	// Test directly pinned
	assertPinned(t, np, ak, "Could not find pinned node!")

	// Test recursively pinned
	assertPinned(t, np, bk, "could not find recursively pinned node")

	ipldPinner, expCount, err := ExportToIPLDPinner(dstore, dserv, dserv)
	if err != nil {
		t.Fatal(err)
	}
	if expCount != 2 {
		t.Fatal("expected 2 exported pins, got", expCount)
	}

	assertPinned(t, ipldPinner, ak, "Could not find pinned node!")
	assertPinned(t, ipldPinner, bk, "could not find recursively pinned node")

	impPinner, impCount, err := ImportFromIPLDPinner(dstore, dserv, dserv)
	if err != nil {
		t.Fatal(err)
	}
	if impCount != expCount {
		t.Fatal("expected", expCount, "imported pins, got", impCount)
	}

	assertPinned(t, impPinner, ak, "Could not find pinned node!")
	assertPinned(t, impPinner, bk, "could not find recursively pinned node")
}

func TestIsPinnedLookup(t *testing.T) {
	// Test that lookups work in pins which share
	// the same branches.  For that construct this tree:
	//
	// A5->A4->A3->A2->A1->A0
	//         /           /
	// B-------           /
	//  \                /
	//   C---------------
	//
	// This ensures that IsPinned works for all objects both when they
	// are pinned and once they have been unpinned.
	aBranchLen := 6

	ctx := context.Background()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	// TODO does pinner need to share datastore with blockservice?
	p := New(dstore, dserv)

	aKeys, bk, ck, err := makeTree(ctx, aBranchLen, dserv, p)
	if err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, aKeys[0], "A0 should be pinned")
	assertPinned(t, p, aKeys[1], "A1 should be pinned")
	assertPinned(t, p, ck, "C should be pinned")
	assertPinned(t, p, bk, "B should be pinned")

	// Unpin A5 recursively
	if err = p.Unpin(ctx, aKeys[5], true); err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, aKeys[0], "A0 should still be pinned through B")
	assertUnpinned(t, p, aKeys[4], "A4 should be unpinned")

	// Unpin B recursively
	if err = p.Unpin(ctx, bk, true); err != nil {
		t.Fatal(err)
	}
	assertUnpinned(t, p, bk, "B should be unpinned")
	assertUnpinned(t, p, aKeys[1], "A1 should be unpinned")
	assertPinned(t, p, aKeys[0], "A0 should still be pinned through C")
}

func TestDuplicateSemantics(t *testing.T) {
	ctx := context.Background()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	// TODO does pinner need to share datastore with blockservice?
	p := New(dstore, dserv)

	a, _ := randNode()
	err := dserv.Add(ctx, a)
	if err != nil {
		t.Fatal(err)
	}

	// pin is recursively
	err = p.Pin(ctx, a, true)
	if err != nil {
		t.Fatal(err)
	}

	// pinning directly should fail
	err = p.Pin(ctx, a, false)
	if err == nil {
		t.Fatal("expected direct pin to fail")
	}

	// pinning recursively again should succeed
	err = p.Pin(ctx, a, true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFlush(t *testing.T) {
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)
	p := New(dstore, dserv)
	_, k := randNode()

	p.PinWithMode(k, ipfspin.Recursive)
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertPinned(t, p, k, "expected key to still be pinned")
}

func TestPinRecursiveFail(t *testing.T) {
	ctx := context.Background()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)

	p := New(dstore, dserv)

	a, _ := randNode()
	b, _ := randNode()
	err := a.AddNodeLink("child", b)
	if err != nil {
		t.Fatal(err)
	}

	// NOTE: This isnt a time based test, we expect the pin to fail
	mctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()

	err = p.Pin(mctx, a, true)
	if err == nil {
		t.Fatal("should have failed to pin here")
	}

	err = dserv.Add(ctx, b)
	if err != nil {
		t.Fatal(err)
	}

	err = dserv.Add(ctx, a)
	if err != nil {
		t.Fatal(err)
	}

	// this one is time based... but shouldnt cause any issues
	mctx, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = p.Pin(mctx, a, true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPinUpdate(t *testing.T) {
	ctx := context.Background()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)
	p := New(dstore, dserv)
	n1, c1 := randNode()
	n2, c2 := randNode()

	err := dserv.Add(ctx, n1)
	if err != nil {
		t.Fatal(err)
	}
	if err = dserv.Add(ctx, n2); err != nil {
		t.Fatal(err)
	}

	if err = p.Pin(ctx, n1, true); err != nil {
		t.Fatal(err)
	}

	if err = p.Update(ctx, c1, c2, true); err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, c2, "c2 should be pinned now")
	assertUnpinned(t, p, c1, "c1 should no longer be pinned")

	if err = p.Update(ctx, c2, c1, false); err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, c2, "c2 should be pinned still")
	assertPinned(t, p, c1, "c1 should be pinned now")
}

func makeTree(ctx context.Context, aBranchLen int, dserv ipld.DAGService, p ipfspin.Pinner) (aKeys []cid.Cid, bk cid.Cid, ck cid.Cid, err error) {
	if aBranchLen < 3 {
		err = errors.New("set aBranchLen to at least 3")
		return
	}

	aNodes := make([]*mdag.ProtoNode, aBranchLen)
	aKeys = make([]cid.Cid, aBranchLen)
	for i := 0; i < aBranchLen; i++ {
		a, _ := randNode()
		if i >= 1 {
			if err = a.AddNodeLink("child", aNodes[i-1]); err != nil {
				return
			}
		}

		if err = dserv.Add(ctx, a); err != nil {
			return
		}
		aNodes[i] = a
		aKeys[i] = a.Cid()
	}

	// Pin last A recursively
	if err = p.Pin(ctx, aNodes[aBranchLen-1], true); err != nil {
		return
	}

	// Create node B and add A3 as child
	b, _ := randNode()
	if err = b.AddNodeLink("mychild", aNodes[3]); err != nil {
		return
	}

	// Create C node
	c, _ := randNode()
	// Add A0 as child of C
	if err = c.AddNodeLink("child", aNodes[0]); err != nil {
		return
	}

	// Add C
	if err = dserv.Add(ctx, c); err != nil {
		return
	}
	ck = c.Cid()

	// Add C to B and Add B
	if err = b.AddNodeLink("myotherchild", c); err != nil {
		return
	}
	if err = dserv.Add(ctx, b); err != nil {
		return
	}
	bk = b.Cid()

	// Pin C recursively
	if err = p.Pin(ctx, c, true); err != nil {
		return
	}

	// Pin B recursively
	if err = p.Pin(ctx, b, true); err != nil {
		return
	}

	if err = p.Flush(ctx); err != nil {
		return
	}

	return
}

func makeNodes(count int, dserv ipld.DAGService) []ipld.Node {
	ctx := context.Background()
	nodes := make([]ipld.Node, count)
	for i := 0; i < count; i++ {
		n, _ := randNode()
		err := dserv.Add(ctx, n)
		if err != nil {
			panic(err)
		}
		nodes[i] = n
	}
	return nodes
}

func pinNodes(nodes []ipld.Node, p ipfspin.Pinner, recursive bool) {
	ctx := context.Background()
	var err error

	for i := range nodes {
		err = p.Pin(ctx, nodes[i], recursive)
		if err != nil {
			panic(err)
		}
	}
	err = p.Flush(ctx)
	if err != nil {
		panic(err)
	}
}

func unpinNodes(nodes []ipld.Node, p ipfspin.Pinner) {
	ctx := context.Background()
	var err error

	for i := range nodes {
		err = p.Unpin(ctx, nodes[i].Cid(), true)
		if err != nil {
			panic(err)
		}
	}
	err = p.Flush(ctx)
	if err != nil {
		panic(err)
	}
}

func makeStore() (ds.Datastore, ipld.DAGService) {
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	return dstore, dserv
}

// BenchmarkLoadRebuild loads a pinner that has some number of saved pins, and
// compares the load time when rebuilding indexes to loading without rebuilding
// indexes.
func BenchmarkLoadRebuild(b *testing.B) {
	dstore, dserv := makeStore()
	pinner := New(dstore, dserv)

	nodes := makeNodes(1024, dserv)
	pinNodes(nodes, pinner, true)

	b.Run("RebuildTrue", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dstore.Put(dirtyKey, []byte{1})

			_, err := LoadPinner(dstore, dserv)
			if err != nil {
				panic(err.Error())
			}
		}
	})

	b.Run("RebuildFalse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dstore.Put(dirtyKey, []byte{0})

			_, err := LoadPinner(dstore, dserv)
			if err != nil {
				panic(err.Error())
			}
		}
	})
}

// BenchmarkPinSeries demonstrates creating individual pins. Each run in the
// series shows performance for a larger number of individual pins.
func BenchmarkPinSeries(b *testing.B) {
	for count := 128; count < 16386; count <<= 1 {
		b.Run(fmt.Sprint("PinDS-", count), func(b *testing.B) {
			dstore, dserv := makeStore()
			pinner := New(dstore, dserv)
			benchmarkPin(b, count, pinner, dserv)
		})

		b.Run(fmt.Sprint("PinIPLD-", count), func(b *testing.B) {
			dstore, dserv := makeStore()
			pinner := ipldpinner.New(dstore, dserv, dserv)
			benchmarkPin(b, count, pinner, dserv)
		})
	}
}

func benchmarkPin(b *testing.B, count int, pinner ipfspin.Pinner, dserv ipld.DAGService) {
	ctx := context.Background()
	nodes := makeNodes(count, dserv)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Pin all the nodes one at a time.
		for j := range nodes {
			err := pinner.Pin(ctx, nodes[j], true)
			if err != nil {
				panic(err)
			}
			err = pinner.Flush(ctx)
			if err != nil {
				panic(err)
			}
		}

		// Unpin all nodes so that they can be pinned next iter.
		b.StopTimer()
		unpinNodes(nodes, pinner)
		b.StartTimer()
	}
}

// BenchmarkUnpinSeries demonstrates unpinning individual pins. Each run in the
// series shows performance for a larger number of individual unpins.
func BenchmarkUnpinSeries(b *testing.B) {
	for count := 128; count < 16386; count <<= 1 {
		b.Run(fmt.Sprint("UnpinDS-", count), func(b *testing.B) {
			dstore, dserv := makeStore()
			pinner := New(dstore, dserv)
			benchmarkPin(b, count, pinner, dserv)
		})

		b.Run(fmt.Sprint("UninIPLD-", count), func(b *testing.B) {
			dstore, dserv := makeStore()
			pinner := ipldpinner.New(dstore, dserv, dserv)
			benchmarkPin(b, count, pinner, dserv)
		})
	}
}

func benchmarkUnpin(b *testing.B, count int, pinner ipfspin.Pinner, dserv ipld.DAGService) {
	ctx := context.Background()
	nodes := makeNodes(count, dserv)
	pinNodes(nodes, pinner, true)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range nodes {
			// Unpin nodes one at a time.
			err := pinner.Unpin(ctx, nodes[j].Cid(), true)
			if err != nil {
				panic(err)
			}
			err = pinner.Flush(ctx)
			if err != nil {
				panic(err)
			}
		}
		// Pin all nodes so that they can be unpinned next iter.
		b.StopTimer()
		pinNodes(nodes, pinner, true)
		b.StartTimer()
	}
}

// BenchmarkPinAllSeries shows times to pin all nodes with only one Flush at
// the end.
func BenchmarkPinAllSeries(b *testing.B) {
	for count := 128; count < 16386; count <<= 1 {
		b.Run(fmt.Sprint("PinAllDS-", count), func(b *testing.B) {
			dstore, dserv := makeStore()
			pinner := New(dstore, dserv)
			benchmarkPinAll(b, count, pinner, dserv)
		})

		b.Run(fmt.Sprint("PinAllIPLD-", count), func(b *testing.B) {
			dstore, dserv := makeStore()
			pinner := ipldpinner.New(dstore, dserv, dserv)
			benchmarkPinAll(b, count, pinner, dserv)
		})
	}
}

func benchmarkPinAll(b *testing.B, count int, pinner ipfspin.Pinner, dserv ipld.DAGService) {
	nodes := makeNodes(count, dserv)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pinNodes(nodes, pinner, true)

		b.StopTimer()
		unpinNodes(nodes, pinner)
		b.StartTimer()
	}
}
