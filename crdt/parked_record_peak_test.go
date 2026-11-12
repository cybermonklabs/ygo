package crdt

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// rv10Write is an origin that holds the bytes of the write it applied, as a
// server write's origin does.
type rv10Write struct{ written []byte }

// rv9Room is a document set up as a served room is for what it parks: caps
// of 4,096 parked structs and delete ranges, no apply budget, and refused
// drains discarded, so the document records who parked each struct and range.
func rv9Room(t *testing.T) *Doc {
	t.Helper()
	doc := New(WithClientID(1), WithMaxPendingItems(4096), WithMaxPendingDeleteRanges(4096))
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: -1}
	})
	doc.SetDiscardRefusedDrain(true)
	return doc
}

func rv10HeapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// The record of parked structs is a map, and a struct leaves it by a delete
// as it leaves the queue (transactionParked.gone). A Go map does not shrink
// as entries are deleted, so a room whose queue held its cap of 4,096
// structs and was cut to one by a merge kept the map's slots for 4,096
// entries for as long as one struct stayed parked (about 218 KB), until the
// record was made to be replaced by a map made at its size once it falls
// under a quarter of its peak (shrinkParkedBy). Measured here: what the
// record holds against the same entries in a map made at their size.
func TestUnit_DiscardRefusedDrain_TheRecordOfParkedStructsKeepsNoSlotsForItsPeak(t *testing.T) {
	const cap = 4096
	doc := New(WithClientID(1), WithMaxPendingItems(cap))
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: -1}
	})
	doc.SetDiscardRefusedDrain(true)
	sender := &rv10Write{written: []byte{1}}
	items := make([]*Item, 0, cap)
	for i := range cap {
		clock := uint64(2 + 2*i)
		items = append(items, &Item{ID: ID{Client: 9, Clock: clock}, Origin: &ID{Client: 9, Clock: clock - 1}, Content: NewContentString("x")})
	}
	before := rv10HeapAlloc()
	if err := ApplyUpdateV1(doc, craftedUpdate(items...), sender); err != nil {
		t.Fatal(err)
	}
	if got := len(doc.parkedBy); got != cap {
		t.Fatalf("the record holds %d structs, want %d", got, cap)
	}
	cover := &Item{ID: ID{Client: 9, Clock: 1}, Origin: &ID{Client: 9, Clock: 0}, Content: NewContentDeleted(2*cap + 1)}
	if err := ApplyUpdateV1(doc, craftedUpdate(cover), sender); err != nil {
		t.Fatal(err)
	}
	stats := doc.PendingStats()
	withPeak := rv10HeapAlloc()
	rebuilt := make(map[*Item]any, len(doc.parkedBy))
	for k, v := range doc.parkedBy {
		rebuilt[k] = v
	}
	doc.parkedBy = rebuilt
	rebuiltHeap := rv10HeapAlloc()
	t.Logf("queue after the cover: %+v; record %d structs; heap above the start: %d with the record as left, %d with it rebuilt at its size; the record keeps %d bytes more",
		stats, len(doc.parkedBy), int64(withPeak)-int64(before), int64(rebuiltHeap)-int64(before), int64(withPeak)-int64(rebuiltHeap))
	runtime.KeepAlive(doc)
	runtime.KeepAlive(sender)
	if kept := int64(withPeak) - int64(rebuiltHeap); kept > 64<<10 {
		t.Fatalf("a queue of %d struct keeps a record of parked structs %d bytes larger than one made at its size", stats.Items, kept)
	}
}

// What a room keeps after 50,000 one-clock frames that compact into one
// parked range: the heap the document holds, read as the heap that goes once
// the document does, and the capacities of the structures that hold the
// range. A measurement: it fails only on an apply's error.
func TestUnit_DiscardRefusedDrain_WhatTheDocumentKeepsAfterFiftyThousandOneClockFrames(t *testing.T) {
	base := rv10HeapAlloc()
	doc := rv9Room(t)
	empty := rv10HeapAlloc()
	sender := &rv10Write{written: []byte{1}}
	for k := range 50_000 {
		u := deleteSetUpdate(map[ClientID][]DeleteRange{9: {{Clock: uint64(k), Len: 1}}})
		if err := ApplyUpdateV1(doc, u, sender); err != nil {
			t.Fatal(err)
		}
	}
	held := rv10HeapAlloc()
	t.Logf("pending %+v; pendingDs[9] len %d cap %d; record sets %d cap %d; set[0] ranges len %d cap %d; pendingDsMoved %d",
		doc.PendingStats(), len(doc.store.pendingDs.clients[9]), cap(doc.store.pendingDs.clients[9]),
		len(doc.parkedDsBy), cap(doc.parkedDsBy), len(doc.parkedDsBy[0].ds.clients[9]), cap(doc.parkedDsBy[0].ds.clients[9]),
		len(doc.store.pendingDsMoved))
	runtime.KeepAlive(doc)
	doc = nil
	gone := rv10HeapAlloc()
	t.Logf("heap: empty room %d above the start; after the frames %d above the empty room; the document held %d at the end",
		int64(empty)-int64(base), int64(held)-int64(empty), int64(held)-int64(gone))
	runtime.KeepAlive(sender)
}

// ForgetParkedOrigin deletes from the record of parked structs too: once
// the sender of 4,096 parked structs is forgotten, the record of the one
// struct another sender parked is held in a map made at its size.
func TestUnit_DiscardRefusedDrain_ForgettingAnOriginLeavesNoSlotsForItsStructs(t *testing.T) {
	const parked = 4096
	doc := New(WithClientID(1), WithMaxPendingItems(parked+1))
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: -1}
	})
	doc.SetDiscardRefusedDrain(true)
	left, stays := &rv10Write{written: []byte{1}}, &rv10Write{written: []byte{2}}
	items := make([]*Item, 0, parked)
	for i := range parked {
		clock := uint64(2 + 2*i)
		items = append(items, &Item{ID: ID{Client: 9, Clock: clock}, Origin: &ID{Client: 9, Clock: clock - 1}, Content: NewContentString("x")})
	}
	require.NoError(t, ApplyUpdateV1(doc, craftedUpdate(items...), left))
	other := &Item{ID: ID{Client: 10, Clock: 2}, Origin: &ID{Client: 10, Clock: 1}, Content: NewContentString("y")}
	require.NoError(t, ApplyUpdateV1(doc, craftedUpdate(other), stays))
	require.Len(t, doc.parkedBy, parked+1)
	doc.ForgetParkedOrigin(left)
	require.Len(t, doc.parkedBy, 1)

	withPeak := rv10HeapAlloc()
	rebuilt := make(map[*Item]any, len(doc.parkedBy))
	for k, v := range doc.parkedBy {
		rebuilt[k] = v
	}
	doc.parkedBy = rebuilt
	kept := int64(withPeak) - int64(rv10HeapAlloc())
	runtime.KeepAlive(doc)
	t.Logf("the record of 1 struct keeps %d bytes more than one made at its size", kept)
	require.LessOrEqual(t, kept, int64(64<<10), "the record of parked structs keeps the slots of the origin forgotten")
}

// ForgetParkedOrigin cuts the record of parked delete ranges in place, one
// set for each sender: once 1,023 of 1,024 senders are forgotten, the set
// left is held in an array made for no more than about four sets.
func TestUnit_DiscardRefusedDrain_TheRecordOfParkedDeletesKeepsNoSlotsForItsPeak(t *testing.T) {
	doc := rv9Room(t)
	senders := make([]*rv10Write, 1024)
	for k := range senders {
		senders[k] = &rv10Write{written: []byte{1}}
		u := deleteSetUpdate(map[ClientID][]DeleteRange{9: {{Clock: uint64(2 * k), Len: 1}}})
		require.NoError(t, ApplyUpdateV1(doc, u, senders[k]))
	}
	require.Len(t, doc.parkedDsBy, len(senders))
	for _, s := range senders[1:] {
		doc.ForgetParkedOrigin(s)
	}
	require.Len(t, doc.parkedDsBy, 1)
	require.GreaterOrEqual(t, len(doc.parkedDsBy), cap(doc.parkedDsBy)/4,
		"the record of parked delete ranges holds 1 set in an array of %d", cap(doc.parkedDsBy))
}
