package datastructure

import (
	"testing"
	"container/heap"
)

func TestBinaryHeap_PushAndPop(t * testing.T) {
	items := map[int]int64{
        1: 20, 2: 5, 3: 100,
    }

    // Create a priority queue, put the items in it, and
    // establish the priority queue (heap) invariants.
    pq := make(BinaryMinHeap, len(items))
    i := 0
    for id, d := range items {
        pq[i] = &Distance{
			NodeId:   uint64(id),
			Distance: d,
		}
		i++
    }
	heap.Init(&pq)
	item1 := heap.Pop(&pq).(*Distance)
	item2 := heap.Pop(&pq).(*Distance)
	if item1.NodeId != 2 || item2.NodeId != 1 {
		t.Errorf("something is wrong with heap")
	}

	heap.Push(&pq, &Distance{4, 50})
	item3 := heap.Pop(&pq).(*Distance)
	if item3.NodeId != 4 {
		t.Errorf("something is wrong with heap")
	}

	if pq.Len() != 1 {
		t.Errorf("something is wrong with heap")
	}
}