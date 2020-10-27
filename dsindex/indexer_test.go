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

func TestHasKey(t *testing.T) {
	nameIndex := createIndexer()

	ok, err := nameIndex.HasKey("bob", "b1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing index")
	}

	ok, err = nameIndex.HasKey("bob", "b3")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should not have index")
	}
}

func TestHasAny(t *testing.T) {
	nameIndex := createIndexer()

	ok, err := nameIndex.HasAny("nothere")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should return false")
	}

	for _, idx := range []string{"alice", "bob", ""} {
		ok, err = nameIndex.HasAny(idx)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("missing indexe", idx)
		}
	}

	count, err := nameIndex.DeleteAll("")
	if err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatal("expected 4 deletions")
	}

	ok, err = nameIndex.HasAny("")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should return false")
	}
}

func TestForEach(t *testing.T) {
	nameIndex := createIndexer()

	found := make(map[string]struct{})
	err := nameIndex.ForEach("bob", func(idx, id string) bool {
		found[id] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, idx := range []string{"b1", "b2"} {
		_, ok := found[idx]
		if !ok {
			t.Fatal("missing index for key", idx)
		}
	}

	keys := map[string]string{}
	err = nameIndex.ForEach("", func(idx, id string) bool {
		keys[id] = idx
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 4 {
		t.Fatal("expected 4 keys")
	}

	if keys["a1"] != "alice" {
		t.Error("expected a1: alice")
	}
	if keys["b1"] != "bob" {
		t.Error("expected b1: bob")
	}
	if keys["b2"] != "bob" {
		t.Error("expected b2: bob")
	}
	if keys["c1"] != "cathy" {
		t.Error("expected c1: cathy")
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

	ok, err := nameIndex.HasKey("alice", "a1")
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
	ok, _ = nameIndex.HasKey("bob", "b1")
	if ok {
		t.Fatal("index not deleted")
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

	// Create map of id->index in sync target
	syncs := map[string]string{}
	err = nameIndex.ForEach("", func(idx, id string) bool {
		syncs[id] = idx
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// Iterate items in sync source and make sure they appear in target
	var itemCount int
	err = refIndex.ForEach("", func(idx, id string) bool {
		itemCount++
		syncIdx, ok := syncs[id]
		if !ok || idx != syncIdx {
			t.Fatal("index", idx, "-->", id, "was not synced")
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if itemCount != len(syncs) {
		t.Fatal("different number of items in sync source and target")
	}
}
