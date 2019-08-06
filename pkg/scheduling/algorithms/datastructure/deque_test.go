package datastructure

import (
	"testing"
)

func TestDeque_NewDeque(t *testing.T) {
	queue := NewDeque(10)
	if queue.size != 10 {
		t.Errorf("Deque.size is %v, but should be 10", queue.size)
	}

	if queue.count != 0 {
		t.Errorf("Deque.count is %v, but should be 0", queue.count)
	}

	if queue.head != 0 || queue.tail != 0 || len(queue.nodes) != 10 || queue.Size() != 0 {
		t.Errorf("something is wrong with initialized deque")
	}
}

func TestDeque_IsEmpty(t *testing.T) {
	queue := NewDeque(5)
	if !queue.IsEmpty() {
		t.Errorf("Deque should be empty after initialization")
	}

	queue.PushEnd(1)
	if queue.IsEmpty() {
		t.Errorf("Deque should not be empty after insertion")
	}

	for i := 2; i < 6; i++ {
		queue.PushEnd(i)
	}
	if queue.IsEmpty() || queue.Size() != 5 {
		t.Errorf("Deque should have size of 5")
	}

	for i := 0; i < 5; i++ {
		queue.PopEnd()
	}
	if !queue.IsEmpty() || queue.Size() != 0 {
		t.Errorf("Deque should be empty now, deque is empty: %v, size is: %v",
			queue.IsEmpty(), queue.Size())
	}

	for i := 0; i < 5; i++ {
		queue.PushEnd(i)
	}
	for i := 0; i < 6; i++ {
		queue.PopFront()
	}
	if !queue.IsEmpty() || queue.Size() != 0 {
		t.Errorf("Deque should be empty now, deque is empty: %v, size is: %v",
			queue.IsEmpty(), queue.Size())
	}
}

func TestDeque_Size(t *testing.T) {
	queue := NewDeque(5)

	for i := 0; i <= 20; i++ {
		queue.PushEnd(i)
	}
	if queue.Size() != 21 || queue.size != 5 || len(queue.nodes) != 25 {
		t.Errorf("Deque is wrong, Size return %v, size field is %v, internal array length is %v",
			queue.Size(), queue.size, len(queue.nodes))
	}

	for i := 0; i < 6; i++ {
		queue.PopFront()
	}
	if queue.Size() != 15 || queue.size != 5 || len(queue.nodes) != 25 {
		t.Errorf("Deque is wrong, Size return %v, size field is %v, internal array length is %v",
			queue.Size(), queue.size, len(queue.nodes))
	}

	for i := 0; i < 10; i++ {
		queue.PushEnd(i)
	}
	if queue.Size() != 25 || queue.size != 5 || len(queue.nodes) != 25 {
		t.Errorf("Deque is wrong, Size return %v, size field is %v, internal array length is %v",
			queue.Size(), queue.size, len(queue.nodes))
	}
	if queue.head != 6 || queue.tail != 6 {
		t.Errorf("Deque is wrong, head is %v, tail is %v", queue.head, queue.tail)
	}

	for i := 0; i < 30; i++ {
		queue.PopEnd()
	}
	if queue.Size() != 0 || queue.size != 5 || len(queue.nodes) != 25 {
		t.Errorf("Deque is wrong, Size return %v, size field is %v, internal array length is %v",
			queue.Size(), queue.size, len(queue.nodes))
	}
	if queue.head != 6 || queue.tail != 6 {
		t.Errorf("Deque is wrong, head is %v, tail is %v", queue.head, queue.tail)
	}
}

func TestDeque_PushFront(t *testing.T) {
	queue := NewDeque(5)
	for i := 0; i < 10; i++ {
		queue.PushFront(i)
	}
	if queue.Size() != 10 || queue.size != 5 || len(queue.nodes) != 10 {
		t.Errorf("Deque is wrong, Size return %v, size field is %v, internal array length is %v",
			queue.Size(), queue.size, len(queue.nodes))
	}
	if queue.head != 5 || queue.tail != 5 {
		t.Errorf("Deque is wrong, head is %v, tail is %v", queue.head, queue.tail)
	}

	if queue.PopFront() != 9 {
		t.Errorf("Deque is wrong")
	}

	if queue.PopEnd() != 0 {
		t.Errorf("Deque is wrong")
	}

	if queue.Size() != 8 || len(queue.nodes) != 10 || queue.head != 6 || queue.tail != 4 {
		t.Errorf("Deque is wrong")
	}

	for i := 0; i < 10; i++ {
		queue.PopEnd()
	}

	if queue.Size() != 0 || len(queue.nodes) != 10 || queue.head != 6 || queue.tail != 6 {
		t.Errorf("Deque is wrong")
	}

	for i := 0; i < 11; i++ {
		queue.PushFront(i)
	}

	if queue.Size() != 11 || len(queue.nodes) != 15 || queue.head != 14 || queue.tail != 10 {
		t.Errorf("Deque is wrong")
	}

	for i := 0; i < 10; i++ {
		queue.PopFront()
	}

	if queue.Size() != 1 || len(queue.nodes) != 15 || queue.head != 9 || queue.tail != 10 {
		t.Errorf("Deque is wrong")
	}
}

func TestDeque_PushEnd(t *testing.T) {
	queue := NewDeque(5)
	for i := 0; i < 10; i++ {
		queue.PushEnd(i)
	}
	if queue.Size() != 10 || queue.size != 5 || len(queue.nodes) != 10 {
		t.Errorf("Deque is wrong, Size return %v, size field is %v, internal array length is %v",
			queue.Size(), queue.size, len(queue.nodes))
	}
	if queue.head != 0 || queue.tail != 0 {
		t.Errorf("Deque is wrong, head is %v, tail is %v", queue.head, queue.tail)
	}

	for i := 0; i < 10; i++ {
		queue.PopFront()
	}
	if queue.Size() != 0 || queue.size != 5 || len(queue.nodes) != 10 {
		t.Errorf("Deque is wrong, Size return %v, size field is %v, internal array length is %v",
			queue.Size(), queue.size, len(queue.nodes))
	}
	if queue.head != 0 || queue.tail != 0 {
		t.Errorf("Deque is wrong, head is %v, tail is %v", queue.head, queue.tail)
	}

	for i := 0; i < 10; i++ {
		queue.PushEnd(i)
	}
	if queue.Size() != 10 || queue.size != 5 || len(queue.nodes) != 10 {
		t.Errorf("Deque is wrong, Size return %v, size field is %v, internal array length is %v",
			queue.Size(), queue.size, len(queue.nodes))
	}
	if queue.head != 0 || queue.tail != 0 {
		t.Errorf("Deque is wrong, head is %v, tail is %v", queue.head, queue.tail)
	}

	for i := 0; i < 10; i++ {
		queue.PopEnd()
	}
	if queue.Size() != 0 || queue.size != 5 || len(queue.nodes) != 10 {
		t.Errorf("Deque is wrong, Size return %v, size field is %v, internal array length is %v",
			queue.Size(), queue.size, len(queue.nodes))
	}
	if queue.head != 0 || queue.tail != 0 {
		t.Errorf("Deque is wrong, head is %v, tail is %v", queue.head, queue.tail)
	}
}

func TestDeque_PopFront(t *testing.T) {
	queue := NewDeque(5)
	for i := 0; i < 5; i++ {
		queue.PopFront()
	}
	if !queue.IsEmpty() || len(queue.nodes) != 5 || queue.head != 0 || queue.tail != 0 {
		t.Errorf("something is wrong with deque")
	}

	for i := 0; i < 5; i++ {
		queue.PushEnd(i)
	}
	r1 := queue.PopFront()
	r2 := queue.PopFront()
	if queue.Size() != 3 || len(queue.nodes) != 5 || queue.head != 2 || queue.tail != 0 ||
		r1 != 0 || r2 != 1 {
		t.Errorf("something is wrong with deque")
	}

	queue.PushEnd(1)
	queue.PushEnd(1)
	queue.PushEnd(1)
	if queue.Size() != 6 || len(queue.nodes) != 10 || queue.head != 0 || queue.tail != 6 {
		t.Errorf("something is wrong with deque")
	}

	r1 = queue.PopFront()
	r2 = queue.PopFront()
	r3 := queue.PopFront()
	r4 := queue.PopFront()
	r5 := queue.PopFront()
	r6 := queue.PopFront()

	if r1 != 2 || r2 != 3 || r3 != 4 || r4 != 1 || r5 != 1 || r6 != 1 {
		t.Errorf("content is not right")
	}
	if queue.Size() != 0 || len(queue.nodes) != 10 || queue.head != 6 || queue.tail != 6 {
		t.Errorf("%v, %v, %v, %v", queue.Size(), len(queue.nodes), queue.head, queue.tail)
	}
}

func TestDeque_PeekFront(t *testing.T) {
	queue := NewDeque(5)
	if queue.PeekFront() != nil {
		t.Errorf("deque should be empty")
	}

	queue.PushEnd(1)
	if queue.PeekFront() != 1 {
		t.Errorf("%v", queue.PeekFront())
	}

	for i := 2; i < 11; i++ {
		queue.PushEnd(i)
	}
	queue.PopFront()
	if queue.Size() != 9 || queue.PeekFront() != 2 {
		t.Errorf("%v, %v", queue.Size(), queue.PeekFront())
	}

	for i := 0; i < 10; i++ {
		queue.PopFront()
	}
	if queue.Size() != 0 || queue.PeekFront() != nil {
		t.Errorf("%v, %v", queue.Size(), queue.PeekFront())
	}
}

func TestDeque_PopEnd(t *testing.T) {
	queue := NewDeque(5)
	for i := 0; i < 5; i++ {
		queue.PopEnd()
	}
	if !queue.IsEmpty() || len(queue.nodes) != 5 || queue.head != 0 || queue.tail != 0 {
		t.Errorf("something is wrong with deque")
	}

	for i := 0; i < 5; i++ {
		queue.PushEnd(i)
	}
	r1 := queue.PopEnd()
	r2 := queue.PopEnd()
	if queue.Size() != 3 || len(queue.nodes) != 5 || queue.head != 0 || queue.tail != 3 ||
		r1 != 4 || r2 != 3 {
		t.Errorf("something is wrong with deque")
	}

	queue.PushEnd(1)
	queue.PushEnd(1)
	queue.PushEnd(1)
	if queue.Size() != 6 || len(queue.nodes) != 10 || queue.head != 0 || queue.tail != 6 {
		t.Errorf("something is wrong with deque")
	}

	r1 = queue.PopEnd()
	r2 = queue.PopEnd()
	r3 := queue.PopEnd()
	r4 := queue.PopEnd()
	r5 := queue.PopEnd()
	r6 := queue.PopEnd()

	if r1 != 1 || r2 != 1 || r3 != 1 || r4 != 2 || r5 != 1 || r6 != 0 {
		t.Errorf("content is not right")
	}
	if queue.Size() != 0 || len(queue.nodes) != 10 || queue.head != 0 || queue.tail != 0 {
		t.Errorf("%v, %v, %v, %v", queue.Size(), len(queue.nodes), queue.head, queue.tail)
	}
}

func TestDeque_PeekEnd(t *testing.T) {
	queue := NewDeque(5)
	if queue.PeekEnd() != nil {
		t.Errorf("deque should be empty")
	}

	queue.PushEnd(1)
	if queue.PeekEnd() != 1 {
		t.Errorf("%v", queue.PeekEnd())
	}

	for i := 2; i < 11; i++ {
		queue.PushEnd(i)
	}
	queue.PopEnd()
	if queue.Size() != 9 || queue.PeekEnd() != 9 {
		t.Errorf("%v, %v", queue.Size(), queue.PeekEnd())
	}

	for i := 0; i < 10; i++ {
		queue.PopEnd()
	}
	if queue.Size() != 0 || queue.PeekEnd() != nil {
		t.Errorf("%v, %v", queue.Size(), queue.PeekEnd())
	}
}
