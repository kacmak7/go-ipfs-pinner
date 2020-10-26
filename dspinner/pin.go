// Package pin implements structures and methods to keep track of
// which objects a user wants to keep stored locally.
package dspinner

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	ipfspinner "github.com/ipfs/go-ipfs-pinner"
	"github.com/ipfs/go-ipfs-pinner/dsindex"
	"github.com/ipfs/go-ipfs-pinner/ipldpinner"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	mdag "github.com/ipfs/go-merkledag"
	"github.com/polydawn/refmt/cbor"
)

const (
	loadTimeout = 5 * time.Second

	pinKeyPath   = "/.pins/pin"
	indexKeyPath = "/.pins/index"
)

var (
	// ErrNotPinned is returned when trying to unpin items that are not pinned.
	ErrNotPinned = fmt.Errorf("not pinned or pinned indirectly")

	log = logging.Logger("pin")

	linkDirect, linkRecursive         string
	pinCidIndexPath, pinNameIndexPath string
)

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

	pinCidIndexPath = path.Join(indexKeyPath, "cidindex")
	pinNameIndexPath = path.Join(indexKeyPath, "nameindex")
}

// pinner implements the Pinner interface
type pinner struct {
	lock sync.RWMutex

	recursePin *cid.Set
	directPin  *cid.Set

	dserv  ipld.DAGService
	dstore ds.Datastore

	cidIndex dsindex.Indexer
}

var _ ipfspinner.Pinner = (*pinner)(nil)

type pin struct {
	id       string
	cid      cid.Cid
	metadata map[string]interface{}
	mode     ipfspinner.Mode
	name     string
}

func (p *pin) codec() uint64   { return p.cid.Type() }
func (p *pin) version() uint64 { return p.cid.Version() }
func (p *pin) dsKey() ds.Key {
	return ds.NewKey(path.Join(pinKeyPath, p.id))
}

func newPin(c cid.Cid, mode ipfspinner.Mode, name string) *pin {
	return &pin{
		id:   ds.RandomKey().String(),
		cid:  c,
		name: name,
		mode: mode,
	}
}

type syncDAGService interface {
	ipld.DAGService
	Sync() error
}

// New creates a new pinner using the given datastore as a backend
func New(dstore ds.Datastore, serv ipld.DAGService) ipfspinner.Pinner {
	return &pinner{
		cidIndex:   dsindex.New(dstore, pinCidIndexPath),
		dserv:      serv,
		dstore:     dstore,
		directPin:  cid.NewSet(),
		recursePin: cid.NewSet(),
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

		// TODO: remove this to support multiple pins per CID
		if p.isPinnedWithTypeBool(ctx, c, ipfspinner.Direct) {
			ok, _ := p.removePinsForCid(c, ipfspinner.Direct)
			if !ok {
				// Fix cache
				p.directPin.Remove(c)
			}
		}

		err = p.addPin(c, ipfspinner.Recursive, "")
		if err != nil {
			return err
		}
	} else {
		if p.recursePin.Has(c) {
			return fmt.Errorf("%s already pinned recursively", c.String())
		}

		err = p.addPin(c, ipfspinner.Direct, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *pinner) addPin(c cid.Cid, mode ipfspinner.Mode, name string) error {
	// Create new pin and store in datastore
	pp := newPin(c, mode, name)

	// Serialize pin
	pinData, err := encodePin(pp)
	if err != nil {
		return fmt.Errorf("could not encode pin: %v", err)
	}

	// Store CID index
	err = p.cidIndex.Add(c.String(), pp.id)
	if err != nil {
		return fmt.Errorf("could not add pin cid index: %v", err)
	}

	// Store the pin
	err = p.dstore.Put(pp.dsKey(), pinData)
	if err != nil {
		p.cidIndex.Delete(c.String(), pp.id)
		return err
	}

	// Update cache
	switch mode {
	case ipfspinner.Recursive:
		p.recursePin.Add(c)
	case ipfspinner.Direct:
		p.directPin.Add(c)
	}

	return nil
}

func (p *pinner) removePin(pin *pin) error {
	// Remove pin from datastore
	err := p.dstore.Delete(pin.dsKey())
	if err != nil {
		return err
	}
	// Remove cid index from datastore
	err = p.cidIndex.Delete(pin.cid.String(), pin.id)
	if err != nil {
		return err
	}

	// Update cache
	switch pin.mode {
	case ipfspinner.Recursive:
		p.recursePin.Remove(pin.cid)
	case ipfspinner.Direct:
		p.directPin.Remove(pin.cid)
	}

	return nil
}

// Unpin a given key
func (p *pinner) Unpin(ctx context.Context, c cid.Cid, recursive bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// TODO: use Ls() to lookup pins when new pinning API available
	/*
		matchSpec := map[string][]string {
			"cid": []string{c.String}
		}
		matches := p.Ls(matchSpec)
	*/
	if p.recursePin.Has(c) {
		if !recursive {
			return fmt.Errorf("%s is pinned recursively", c)
		}
	} else if !p.directPin.Has(c) {
		return ErrNotPinned
	}

	ok, err := p.removePinsForCid(c, ipfspinner.Any)
	if err != nil {
		return err
	}
	if !ok {
		log.Error("found CID index with missing pin")
	}
	return nil
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
	case ipfspinner.Recursive:
		if p.recursePin.Has(c) {
			return linkRecursive, true, nil
		}
		return "", false, nil
	case ipfspinner.Direct:
		if p.directPin.Has(c) {
			return linkDirect, true, nil
		}
		return "", false, nil
	case ipfspinner.Internal:
		return "", false, nil
	case ipfspinner.Indirect:
	case ipfspinner.Any:
		if p.recursePin.Has(c) {
			return linkRecursive, true, nil
		}
		if p.directPin.Has(c) {
			return linkDirect, true, nil
		}
	default:
		err := fmt.Errorf(
			"invalid Pin Mode '%d', must be one of {%d, %d, %d, %d, %d}",
			mode, ipfspinner.Direct, ipfspinner.Indirect, ipfspinner.Recursive,
			ipfspinner.Internal, ipfspinner.Any)
		return "", false, err
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
//
// TODO: If a CID is pinned by multiple pins, should they all be reported?
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

	// Check cache to see if CID is pinned
	switch mode {
	case ipfspinner.Direct:
		if !p.directPin.Has(c) {
			return
		}
	case ipfspinner.Recursive:
		if !p.recursePin.Has(c) {
			return
		}
	default:
		// programmer error, panic OK
		panic("unrecognized pin type")
	}

	p.removePinsForCid(c, mode)
}

// removePinsForCid removes all pins for a cid that have the specified mode.
func (p *pinner) removePinsForCid(c cid.Cid, mode ipfspinner.Mode) (bool, error) {
	// Search for pins by CID
	ids, err := p.cidIndex.Search(c.String())
	if err != nil {
		return false, err
	}

	var removed bool

	// Remove the pin with the requested mode
	for _, pid := range ids {
		var pp *pin
		pp, err = p.loadPin(pid)
		if err != nil {
			if err == ds.ErrNotFound {
				continue
			}
			return false, err
		}
		if mode == ipfspinner.Any || pp.mode == mode {
			err = p.removePin(pp)
			if err != nil {
				return false, err
			}
			removed = true
		}
	}
	return removed, nil
}

// loadPin loads a single pin from the datastore.
func (p *pinner) loadPin(pid string) (*pin, error) {
	pinData, err := p.dstore.Get(ds.NewKey(path.Join(pinKeyPath, pid)))
	if err != nil {
		return nil, err
	}
	return decodePin(pid, pinData)
}

// loadAllPins loads all pins from the datastore.
func (p *pinner) loadAllPins() ([]*pin, error) {
	q := query.Query{
		Prefix: pinKeyPath,
	}
	results, err := p.dstore.Query(q)
	if err != nil {
		return nil, err
	}
	ents, err := results.Rest()
	if err != nil {
		return nil, err
	}
	if len(ents) == 0 {
		return nil, nil
	}

	pins := make([]*pin, len(ents))
	for i := range ents {
		var p *pin
		p, err := decodePin(path.Base(ents[i].Key), ents[i].Value)
		if err != nil {
			return nil, err
		}
		pins[i] = p
	}
	return pins, nil
}

// LoadPinner loads a pinner and its keysets from the given datastore
func LoadPinner(dstore ds.Datastore, dserv ipld.DAGService, internal ipld.DAGService) (ipfspinner.Pinner, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), loadTimeout)
	defer cancel()

	if internal != nil {
		ipldPinner, err := ipldpinner.LoadPinner(dstore, dserv, internal)
		switch err {
		case ds.ErrNotFound:
			// No more dag storage; load from datastore
		case nil:
			// Need to convert from dag storage
			return convertIPLDToDSPinner(ctx, ipldPinner, dstore, dserv)
		default:
			return nil, err
		}
	}

	p := New(dstore, dserv).(*pinner)
	err := p.rebuildIndexes(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot rebuild indexes: %v", err)
	}

	return p, nil
}

// rebuildIndexes uses the stored pins to rebuild secondary indexes.  This
// resolves any discrepancy between secondary indexes and pins that could
// result from a program termination between saving the two.
func (p *pinner) rebuildIndexes(ctx context.Context) error {
	pins, err := p.loadAllPins()
	if err != nil {
		return fmt.Errorf("cannot load pins: %v", err)
	}

	p.directPin = cid.NewSet()
	p.recursePin = cid.NewSet()

	// Build temporary in-memory CID index from pins
	dstoreMem := ds.NewMapDatastore()
	tmpCidIndex := dsindex.New(dstoreMem, pinCidIndexPath)
	for _, pp := range pins {
		tmpCidIndex.Add(pp.cid.String(), pp.id)

		// Build up cache
		if pp.mode == ipfspinner.Recursive {
			p.recursePin.Add(pp.cid)
		} else if pp.mode == ipfspinner.Direct {
			p.directPin.Add(pp.cid)
		}
	}

	// Sync the CID index to what was build from pins.  This fixes any invalid
	// indexes, which could happen if ipfs was terminated between writing pin
	// and writing secondary index.
	changed, err := p.cidIndex.SyncTo(tmpCidIndex)
	if err != nil {
		return fmt.Errorf("cannot sync indexes: %v", err)
	}
	if changed {
		log.Error("invalid indexes detected - rebuilt")
	}

	return nil
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

// InternalPins returns all cids kept pinned for the internal state of the
// pinner
func (p *pinner) InternalPins(ctx context.Context) ([]cid.Cid, error) {
	return nil, nil
}

// Update updates a recursive pin from one cid to another.  This is equivalent
// to pinning the new one and unpinning the old one.
//
// TODO: This will not work when multiple pins are supported
func (p *pinner) Update(ctx context.Context, from, to cid.Cid, unpin bool) error {
	if from == to {
		return nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.recursePin.Has(from) {
		return fmt.Errorf("'from' cid was not recursively pinned already")
	}

	err := p.addPin(to, ipfspinner.Recursive, "")
	if err != nil {
		return err
	}

	if !unpin {
		return nil
	}

	ok, err := p.removePinsForCid(from, ipfspinner.Recursive)
	if err != nil {
		return err
	}
	if !ok {
		log.Error("found CID index with missing pin")
	}

	return nil
}

// Flush encodes and writes pinner keysets to the datastore
func (p *pinner) Flush(ctx context.Context) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if syncDServ, ok := p.dserv.(syncDAGService); ok {
		if err := syncDServ.Sync(); err != nil {
			return fmt.Errorf("cannot sync pinned data: %v", err)
		}
	}

	// TODO: is it necessary to keep a list of added pins to sync?
	//if err := p.dstore.Sync(pinKey); err != nil {
	//	return fmt.Errorf("cannot sync pin state: %v", err)
	//}

	return nil
}

// PinWithMode allows the user to have fine grained control over pin
// counts
func (p *pinner) PinWithMode(c cid.Cid, mode ipfspinner.Mode) {
	p.lock.Lock()
	defer p.lock.Unlock()

	ids, _ := p.cidIndex.Search(c.String())
	for i := range ids {
		pp, _ := p.loadPin(ids[i])
		if pp != nil && pp.mode == mode {
			return // already a pin for this CID with this mode
		}
	}

	err := p.addPin(c, mode, "")
	if err != nil {
		return
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

// convertIPLDToDSPinner converts pins stored in mdag based storage to pins
// stores in the datastore. After pins are stored in datastore, then root pin
// key is deleted to unlink the pin data in the mdag store.
func convertIPLDToDSPinner(ctx context.Context, ipldPinner ipfspinner.Pinner, dstore ds.Datastore, dserv ipld.DAGService) (*pinner, error) {
	var err error
	p := New(dstore, dserv).(*pinner)

	// Save pinned CIDs as new pins in datastore.
	rCids, _ := ipldPinner.RecursiveKeys(ctx)
	for i := range rCids {
		err = p.addPin(rCids[i], ipfspinner.Recursive, "")
		if err != nil {
			return nil, err
		}
	}
	dCids, _ := ipldPinner.DirectKeys(ctx)
	for i := range rCids {
		err = p.addPin(dCids[i], ipfspinner.Direct, "")
		if err != nil {
			return nil, err
		}
	}

	// Delete root mdag key from datastore to remove old pin storage.
	pinDatastoreKey := ds.NewKey("/local/pins")
	if err = dstore.Delete(pinDatastoreKey); err != nil {
		return nil, fmt.Errorf("cannot delete old pin state: %v", err)
	}
	if err = dstore.Sync(pinDatastoreKey); err != nil {
		return nil, fmt.Errorf("cannot sync old pin state: %v", err)
	}

	return p, nil
}

func encodePin(p *pin) ([]byte, error) {
	var buf bytes.Buffer
	encoder := cbor.NewMarshaller(&buf)
	pinData := map[string]interface{}{
		"mode": p.mode,
		"cid":  p.cid.String(),
	}
	// Encode optional fields
	if p.name != "" {
		pinData["name"] = p.name
	}
	if len(p.metadata) != 0 {
		pinData["metadata"] = p.metadata
	}

	err := encoder.Marshal(pinData)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodePin(pid string, data []byte) (*pin, error) {
	reader := bytes.NewReader(data)
	decoder := cbor.NewUnmarshaller(cbor.DecodeOptions{}, reader)

	var pinData map[string]interface{}
	err := decoder.Unmarshal(&pinData)
	if err != nil {
		return nil, fmt.Errorf("cannot decode pin: %v", err)
	}

	cidData, ok := pinData["cid"]
	if !ok {
		return nil, fmt.Errorf("missing cid")
	}
	cidStr, ok := cidData.(string)
	if !ok {
		return nil, fmt.Errorf("invalid pin cid data")
	}
	c, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("cannot decode pin cid: %v", err)
	}

	modeData, ok := pinData["mode"]
	if !ok {
		return nil, fmt.Errorf("missing mode")
	}
	mode64, ok := modeData.(uint64)
	if !ok {
		return nil, fmt.Errorf("invalid pin mode data")
	}

	p := &pin{
		id:   pid,
		mode: ipfspinner.Mode(mode64),
		cid:  c,
	}

	// Decode optional data

	meta, ok := pinData["metadata"]
	if ok && meta != nil {
		p.metadata, ok = meta.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot decode metadata")
		}
	}

	name, ok := pinData["name"]
	if ok && name != nil {
		p.name, ok = name.(string)
		if !ok {
			return nil, fmt.Errorf("invalid pin name data")
		}
	}

	return p, nil
}
