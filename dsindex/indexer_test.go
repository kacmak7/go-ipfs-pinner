package dsindex

import (
	"testing"

	ds "github.com/ipfs/go-datastore"
)

func createIndexer() Indexer {
	dstore := ds.NewMapDatastore()
	nameIndex := New(dstore, "/data/nameindex")

	nameIndex.Add("alice", "a1")
	nameIndex.Add("bob", "b1")
	nameIndex.Add("bob", "b2")
	nameIndex.Add("cathy", "c1")

	return nameIndex
}

func TestAdd(t *testing.T) {
	nameIndex := createIndexer()
	err := nameIndex.Add("someone", "s1")
	if err != nil {
		t.Fatal(err)
	}
	err = nameIndex.Add("someone", "s1")
	if err != nil {
		t.Fatal(err)
	}
	err = nameIndex.Add("", "s1")
	if err != nil {
		t.Fatal(err)
	}
}

func TestHas(t *testing.T) {
	nameIndex := createIndexer()

	ok, err := nameIndex.Has("bob", "b1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing index")
	}

	ok, err = nameIndex.Has("bob", "b3")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should not have index")
	}
}

func TestSearch(t *testing.T) {
	nameIndex := createIndexer()

	ids, err := nameIndex.Search("bob")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatal("wrong number of ids - expected 2 got", ids)
	}
	for _, id := range ids {
		if id != "b1" && id != "b2" {
			t.Fatal("wrong value in id set")
		}
	}
	if ids[0] == ids[1] {
		t.Fatal("dublicate id")
	}

	ids, err = nameIndex.Search("cathy")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "c1" {
		t.Fatal("wrong ids")
	}

	ids, err = nameIndex.Search("amit")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatal("unexpected ids returned")
	}
}

func TestDelete(t *testing.T) {
	nameIndex := createIndexer()

	err := nameIndex.Delete("bob", "b3")
	if err != nil {
		t.Fatal(err)
	}

	err = nameIndex.Delete("alice", "a1")
	if err != nil {
		t.Fatal(err)
	}

	ok, err := nameIndex.Has("alice", "a1")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("index should have been deleted")
	}

	count, err := nameIndex.DeleteAll("bob")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatal("wrong deleted count")
	}
	ok, _ = nameIndex.Has("bob", "b1")
	if ok {
		t.Fatal("index not deleted")
	}
}

func TestAll(t *testing.T) {
	nameIndex := createIndexer()

	all, err := nameIndex.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Fatal("wrong number of indexes")
	}
	for k, v := range all {
		if ok, _ := nameIndex.Has(v, k); !ok {
			t.Fatal("indexes from all do not match what indexer has")
		}
	}
}

func TestSyndTo(t *testing.T) {
	nameIndex := createIndexer()

	dstore := ds.NewMapDatastore()
	refIndex := New(dstore, "/ref")
	refIndex.Add("alice", "a1")
	refIndex.Add("cathy", "zz")
	refIndex.Add("dennis", "d1")

	changed, err := nameIndex.SyncTo(refIndex)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Error("change was not indicated")
	}

	refAll, _ := refIndex.All()
	syncAll, _ := nameIndex.All()

	if len(syncAll) != len(refAll) {
		t.Fatal("wrong number of indexes after sync")
	}
	for k, v := range refAll {
		vSync, ok := syncAll[k]
		if !ok || v != vSync {
			t.Fatal("index", v, "-->", k, "was not synced")
		}
	}
}
