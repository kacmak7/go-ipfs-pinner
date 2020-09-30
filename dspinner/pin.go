// Package pin implements structures and methods to keep track of
// which objects a user wants to keep stored locally.
package dspinner

import (
	"context"
	"fmt"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	mdag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-merkledag/dagutils"


	cbor "github.com/polydawn/refmt/cbor"
	ipfspinner "github.com/ipfs/go-ipfs-pinner"
)

var log = logging.Logger("pin")

var pinDatastoreKey = ds.NewKey("/.pins")

var linkDirect, linkRecursive, linkInternal string
func init() {
	directStr, ok := ipfspinner.ModeToString(ipfspinner.Direct)
	if !ok {
		panic("could not find Direct pin enum")
	}
	linkDirect = directStr

	recursiveStr, ok := ipfspinner.ModeToString(ipfspinner.Recursive)
	if !ok {
		panic("could not find Recursive pin enum")
	}
	linkRecursive = recursiveStr

	internalStr, ok := ipfspinner.ModeToString(ipfspinner.Internal)
	if !ok {
		panic("could not find Internal pin enum")
	}
	linkInternal = internalStr
}

// pinner implements the Pinner interface
type pinner struct {
	lock       sync.RWMutex
	recursePin *cid.Set
	directPin  *cid.Set

	dserv       ipld.DAGService
	dstore      ds.Datastore
}

type pin struct {
	depth int
	version int
	codec int
    metadata map[string]interface{}
}

var _  ipfspinner.Pinner = (*pinner)(nil)

type syncDAGService interface {
	ipld.DAGService
	Sync() error
}

// NewPinner creates a new pinner using the given datastore as a backend
func NewPinner(dstore ds.Datastore, serv ipld.DAGService) ipfspinner.Pinner {
	return &pinner{
		dserv:       serv,
		dstore:      dstore,
	}
}

// Pin the given node, optionally recursive
func (p *pinner) Pin(ctx context.Context, node ipld.Node, recurse bool) error {
	err := p.dserv.Add(ctx, node)
	if err != nil {
		return err
	}

	c := node.Cid()

	p.lock.Lock()
	defer p.lock.Unlock()

	if recurse {
		if p.isPinnedWithTypeBool(ctx, c, ipfspinner.Recursive) {
			return nil
		}

		p.lock.Unlock()
		// temporary unlock to fetch the entire graph
		err := mdag.FetchGraph(ctx, c, p.dserv)
		p.lock.Lock()
		if err != nil {
			return err
		}

		if p.isPinnedWithTypeBool(ctx, c, ipfspinner.Recursive) {
			return nil
		}

		if p.directPin.Has(c) {
			p.directPin.Remove(c)
		}

		p.recursePin.Add(c)
	} else {
		if p.recursePin.Has(c) {
			return fmt.Errorf("%s already pinned recursively", c.String())
		}

		p.directPin.Add(c)
	}
	return nil
}

// ErrNotPinned is returned when trying to unpin items which are not pinned.
var ErrNotPinned = fmt.Errorf("not pinned or pinned indirectly")

// Unpin a given key
func (p *pinner) Unpin(ctx context.Context, c cid.Cid, recursive bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.recursePin.Has(c) {
		if !recursive {
			return fmt.Errorf("%s is pinned recursively", c)
		}
		p.recursePin.Remove(c)
		return nil
	}
	if p.directPin.Has(c) {
		p.directPin.Remove(c)
		return nil
	}
	return ErrNotPinned
}

// IsPinned returns whether or not the given key is pinned
// and an explanation of why its pinned
func (p *pinner) IsPinned(ctx context.Context, c cid.Cid) (string, bool, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.isPinnedWithType(ctx, c, ipfspinner.Any)
}

// IsPinnedWithType returns whether or not the given cid is pinned with the
// given pin type, as well as returning the type of pin its pinned with.
func (p *pinner) IsPinnedWithType(ctx context.Context, c cid.Cid, mode ipfspinner.Mode) (string, bool, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.isPinnedWithType(ctx, c, mode)
}

func (p *pinner) isPinnedWithTypeBool(ctx context.Context, c cid.Cid, mode ipfspinner.Mode) bool {
	_, found, _ := p.isPinnedWithType(ctx, c, mode)
	return found
}

func (p *pinner) isPinnedWithType(ctx context.Context, c cid.Cid, mode ipfspinner.Mode) (string, bool, error) {
	switch mode {
	case ipfspinner.Any, ipfspinner.Direct, ipfspinner.Indirect, ipfspinner.Recursive, ipfspinner.Internal:
	default:
		err := fmt.Errorf("invalid Pin Mode '%d', must be one of {%d, %d, %d, %d, %d}",
			mode, ipfspinner.Direct, ipfspinner.Indirect, ipfspinner.Recursive, ipfspinner.Internal, ipfspinner.Any)
		return "", false, err
	}
	if (mode == ipfspinner.Recursive || mode == ipfspinner.Any) && p.recursePin.Has(c) {
		return linkRecursive, true, nil
	}
	if mode == ipfspinner.Recursive {
		return "", false, nil
	}

	if (mode == ipfspinner.Direct || mode == ipfspinner.Any) && p.directPin.Has(c) {
		return linkDirect, true, nil
	}
	if mode == ipfspinner.Direct {
		return "", false, nil
	}

	if mode == ipfspinner.Internal {
		return "", false, nil
	}

	// Default is Indirect
	visitedSet := cid.NewSet()
	for _, rc := range p.recursePin.Keys() {
		has, err := hasChild(ctx, p.dserv, rc, c, visitedSet.Visit)
		if err != nil {
			return "", false, err
		}
		if has {
			return rc.String(), true, nil
		}
	}
	return "", false, nil
}

// CheckIfPinned Checks if a set of keys are pinned, more efficient than
// calling IsPinned for each key, returns the pinned status of cid(s)
func (p *pinner) CheckIfPinned(ctx context.Context, cids ...cid.Cid) ([]ipfspinner.Pinned, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	pinned := make([]ipfspinner.Pinned, 0, len(cids))
	toCheck := cid.NewSet()

	// First check for non-Indirect pins directly
	for _, c := range cids {
		if p.recursePin.Has(c) {
			pinned = append(pinned, ipfspinner.Pinned{Key: c, Mode: ipfspinner.Recursive})
		} else if p.directPin.Has(c) {
			pinned = append(pinned, ipfspinner.Pinned{Key: c, Mode: ipfspinner.Direct})
		} else {
			toCheck.Add(c)
		}
	}

	// Now walk all recursive pins to check for indirect pins
	var checkChildren func(cid.Cid, cid.Cid) error
	checkChildren = func(rk, parentKey cid.Cid) error {
		links, err := ipld.GetLinks(ctx, p.dserv, parentKey)
		if err != nil {
			return err
		}
		for _, lnk := range links {
			c := lnk.Cid

			if toCheck.Has(c) {
				pinned = append(pinned,
					ipfspinner.Pinned{Key: c, Mode: ipfspinner.Indirect, Via: rk})
				toCheck.Remove(c)
			}

			err := checkChildren(rk, c)
			if err != nil {
				return err
			}

			if toCheck.Len() == 0 {
				return nil
			}
		}
		return nil
	}

	for _, rk := range p.recursePin.Keys() {
		err := checkChildren(rk, rk)
		if err != nil {
			return nil, err
		}
		if toCheck.Len() == 0 {
			break
		}
	}

	// Anything left in toCheck is not pinned
	for _, k := range toCheck.Keys() {
		pinned = append(pinned, ipfspinner.Pinned{Key: k, Mode: ipfspinner.NotPinned})
	}

	return pinned, nil
}

// RemovePinWithMode is for manually editing the pin structure.
// Use with care! If used improperly, garbage collection may not
// be successful.
func (p *pinner) RemovePinWithMode(c cid.Cid, mode ipfspinner.Mode) {
	p.lock.Lock()
	defer p.lock.Unlock()
	switch mode {
	case ipfspinner.Direct:
		p.directPin.Remove(c)
	case ipfspinner.Recursive:
		p.recursePin.Remove(c)
	default:
		// programmer error, panic OK
		panic("unrecognized pin type")
	}
}

func cidSetWithValues(cids []cid.Cid) *cid.Set {
	out := cid.NewSet()
	for _, c := range cids {
		out.Add(c)
	}
	return out
}

// LoadPinner loads a pinner and its keysets from the given datastore
func LoadPinner(d ds.Datastore, dserv ipld.DAGService) (*pinner, error) {
	p := new(pinner)

	rootKey, err := d.Get(pinDatastoreKey)
	if err != nil {
		return nil, fmt.Errorf("cannot load pin state: %v", err)
	}
	rootCid, err := cid.Cast(rootKey)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()

	root, err := internal.Get(ctx, rootCid)
	if err != nil {
		return nil, fmt.Errorf("cannot find pinning root object: %v", err)
	}

	rootpb, ok := root.(*mdag.ProtoNode)
	if !ok {
		return nil, mdag.ErrNotProtobuf
	}

	internalset := cid.NewSet()
	internalset.Add(rootCid)
	recordInternal := internalset.Add

	{ // load recursive set
		recurseKeys, err := ipldpinner.loadSet(ctx, internal, rootpb, linkRecursive, recordInternal)
		if err != nil {
			return nil, fmt.Errorf("cannot load recursive pins: %v", err)
		}
		p.recursePin = cidSetWithValues(recurseKeys)
	}

	{ // load direct set
		directKeys, err := ipldpinner.loadSet(ctx, internal, rootpb, linkDirect, recordInternal)
		if err != nil {
			return nil, fmt.Errorf("cannot load direct pins: %v", err)
		}
		p.directPin = cidSetWithValues(directKeys)
	}

	p.internalPin = internalset

	// assign services
	p.dserv = dserv
	p.dstore = d
	p.internal = internal

	return p, nil
}

// DirectKeys returns a slice containing the directly pinned keys
func (p *pinner) DirectKeys(ctx context.Context) ([]cid.Cid, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.directPin.Keys(), nil
}

// RecursiveKeys returns a slice containing the recursively pinned keys
func (p *pinner) RecursiveKeys(ctx context.Context) ([]cid.Cid, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.recursePin.Keys(), nil
}

// Update updates a recursive pin from one cid to another
// this is more efficient than simply pinning the new one and unpinning the
// old one
func (p *pinner) Update(ctx context.Context, from, to cid.Cid, unpin bool) error {
	if from == to {
		// Nothing to do. Don't remove this check or we'll end up
		// _removing_ the pin.
		//
		// See #6648
		return nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.recursePin.Has(from) {
		return fmt.Errorf("'from' cid was not recursively pinned already")
	}

	// Temporarily unlock while we fetch the differences.
	p.lock.Unlock()
	err := dagutils.DiffEnumerate(ctx, p.dserv, from, to)
	p.lock.Lock()

	if err != nil {
		return err
	}

	p.recursePin.Add(to)
	if unpin {
		p.recursePin.Remove(from)
	}
	return nil
}

// Flush encodes and writes pinner keysets to the datastore
func (p *pinner) Flush(ctx context.Context) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	internalset := cid.NewSet()
	recordInternal := internalset.Add

	root := &mdag.ProtoNode{}
	{
		n, err := ipldpinner.storeSet(ctx, p.internal, p.directPin.Keys(), recordInternal)
		if err != nil {
			return err
		}
		if err := root.AddNodeLink(linkDirect, n); err != nil {
			return err
		}
	}

	{
		n, err := ipldpinner.storeSet(ctx, p.internal, p.recursePin.Keys(), recordInternal)
		if err != nil {
			return err
		}
		if err := root.AddNodeLink(linkRecursive, n); err != nil {
			return err
		}
	}

	// add the empty node, its referenced by the pin sets but never created
	err := p.internal.Add(ctx, new(mdag.ProtoNode))
	if err != nil {
		return err
	}

	err = p.internal.Add(ctx, root)
	if err != nil {
		return err
	}

	k := root.Cid()

	internalset.Add(k)

	if syncDServ, ok := p.dserv.(syncDAGService); ok {
		if err := syncDServ.Sync(); err != nil {
			return fmt.Errorf("cannot sync pinned data: %v", err)
		}
	}

	if syncInternal, ok := p.internal.(syncDAGService); ok {
		if err := syncInternal.Sync(); err != nil {
			return fmt.Errorf("cannot sync pinning data: %v", err)
		}
	}

	if err := p.dstore.Put(pinDatastoreKey, k.Bytes()); err != nil {
		return fmt.Errorf("cannot store pin state: %v", err)
	}
	if err := p.dstore.Sync(pinDatastoreKey); err != nil {
		return fmt.Errorf("cannot sync pin state: %v", err)
	}
	p.internalPin = internalset
	return nil
}

// InternalPins returns all cids kept pinned for the internal state of the
// pinner
func (p *pinner) InternalPins(ctx context.Context) ([]cid.Cid, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	var out []cid.Cid
	out = append(out, p.internalPin.Keys()...)
	return out, nil
}

// PinWithMode allows the user to have fine grained control over pin
// counts
func (p *pinner) PinWithMode(c cid.Cid, mode ipfspinner.Mode) {
	p.lock.Lock()
	defer p.lock.Unlock()
	switch mode {
	case ipfspinner.Recursive:
		p.recursePin.Add(c)
	case ipfspinner.Direct:
		p.directPin.Add(c)
	}
}

// hasChild recursively looks for a Cid among the children of a root Cid.
// The visit function can be used to shortcut already-visited branches.
func hasChild(ctx context.Context, ng ipld.NodeGetter, root cid.Cid, child cid.Cid, visit func(cid.Cid) bool) (bool, error) {
	links, err := ipld.GetLinks(ctx, ng, root)
	if err != nil {
		return false, err
	}
	for _, lnk := range links {
		c := lnk.Cid
		if lnk.Cid.Equals(child) {
			return true, nil
		}
		if visit(c) {
			has, err := hasChild(ctx, ng, c, child, visit)
			if err != nil {
				return false, err
			}

			if has {
				return has, nil
			}
		}
	}
	return false, nil
}
