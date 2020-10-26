// Package dsindex provides secondary indexing functionality for a datastore.
package dsindex

import (
	"path"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

// Indexer maintains a secondary index.  Each value of the secondary index maps
// to one more primary keys.
type Indexer interface {
	// Add adds the a to the an index
	Add(index, id string) error

	// Has determines if a key is in an index
	Has(index, id string) (bool, error)

	// Delete deletes the specified key from the index.  If the key is not in
	// the datastore, this method returns no error.
	Delete(index, id string) error

	// DeleteAll deletes all keys in the given index.  If a key is not in the
	// datastore, this method returns no error.
	DeleteAll(index string) (count int, err error)

	// Search returns all keys for the given index
	Search(index string) (ids []string, err error)

	// All returns a map of key to secondary index value for all indexed keys
	All() (map[string]string, error)

	// Synchronize the indexes in this Indexer to match those of the given
	// Indexer. The indexPath prefix is not synchronized, only the index/key
	// portion of the indexes.
	SyncTo(reference Indexer) (changed bool, err error)
}

// indexer is a simple implementation of Indexer.  This implementation relies
// on the underlying data store supporting efficent querying by prefix.
//
// TODO: Consider adding caching
type indexer struct {
	dstore    ds.Datastore
	indexPath string
}

// New creates a new datastore index.  All indexes are stored prefixed with the
// specified index path.
func New(dstore ds.Datastore, indexPath string) Indexer {
	return &indexer{
		dstore:    dstore,
		indexPath: indexPath,
	}
}

func (x *indexer) Add(index, id string) error {
	key := ds.NewKey(path.Join(x.indexPath, index, id))
	return x.dstore.Put(key, []byte{})
}

func (x *indexer) Has(index, id string) (bool, error) {
	key := ds.NewKey(path.Join(x.indexPath, index, id))
	return x.dstore.Has(key)
}

func (x *indexer) Delete(index, id string) error {
	key := ds.NewKey(path.Join(x.indexPath, index, id))
	return x.dstore.Delete(key)
}

func (x *indexer) DeleteAll(index string) (int, error) {
	ids, err := x.Search(index)
	if err != nil {
		return 0, err
	}
	for i := range ids {
		err = x.Delete(index, ids[i])
		if err != nil {
			return 0, err
		}
	}
	return len(ids), nil
}

func (x *indexer) Search(index string) ([]string, error) {
	ents, err := x.queryPrefix(path.Join(x.indexPath, index))
	if err != nil {
		return nil, err
	}
	if len(ents) == 0 {
		return nil, nil
	}

	ids := make([]string, len(ents))
	for i := range ents {
		ids[i] = path.Base(ents[i].Key)
	}
	return ids, nil
}

func (x *indexer) All() (map[string]string, error) {
	ents, err := x.queryPrefix(x.indexPath)
	if err != nil {
		return nil, err
	}
	if len(ents) == 0 {
		return nil, nil
	}

	indexes := make(map[string]string, len(ents))
	for i := range ents {
		fullPath := ents[i].Key
		indexes[path.Base(fullPath)] = path.Base(path.Dir(fullPath))
	}

	return indexes, nil
}

func (x *indexer) SyncTo(ref Indexer) (changed bool, err error) {
	var curAll, refAll map[string]string

	refAll, err = ref.All()
	if err != nil {
		return
	}

	curAll, err = x.All()
	if err != nil {
		return
	}

	for k, v := range refAll {
		cv, ok := curAll[k]
		if ok && cv == v {
			// same in both, so delete from both
			delete(curAll, k)
			delete(refAll, k)
		}
	}

	// What remains in curAll are indexes that no longer exist
	for k, v := range curAll {
		err = x.dstore.Delete(ds.NewKey(path.Join(x.indexPath, v, k)))
		if err != nil {
			return
		}
	}

	// What remains in refAll are indexes that need to be added
	for k, v := range refAll {
		err = x.dstore.Put(ds.NewKey(path.Join(x.indexPath, v, k)), nil)
		if err != nil {
			return
		}
	}

	changed = len(refAll) != 0 || len(curAll) != 0
	return
}

func (x *indexer) queryPrefix(prefix string) ([]query.Entry, error) {
	q := query.Query{
		Prefix:   prefix,
		KeysOnly: true,
	}
	results, err := x.dstore.Query(q)
	if err != nil {
		return nil, err
	}
	return results.Rest()
}
