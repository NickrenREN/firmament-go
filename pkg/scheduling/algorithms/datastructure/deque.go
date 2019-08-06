package datastructure

// a deque which is based on a circular list that resizes as needed.
type Deque struct {
	nodes []interface{}
	size  int
	head  int
	tail  int
	count int
}

// NewQueue returns a new deque with the given initial size.
func NewDeque(size int) *Deque {
	return &Deque{
		nodes: make([]interface{}, size),
		size:  size,
	}
}

func (q *Deque) IsEmpty() bool {
	return q.count == 0
}

func (q *Deque) Size() int {
	return q.count
}

// PushEnd adds an element to the end of deque.
func (q *Deque) PushEnd(n interface{}) {
	if q.head == q.tail && q.count > 0 {
		nodes := make([]interface{}, len(q.nodes)+q.size)
		copy(nodes, q.nodes[q.head:])
		copy(nodes[len(q.nodes)-q.head:], q.nodes[:q.head])
		q.head = 0
		q.tail = len(q.nodes)
		q.nodes = nodes
	}
	q.nodes[q.tail] = n
	q.tail = (q.tail + 1) % len(q.nodes)
	q.count++
}

// PushFront adds an element to the front of deque.
func (q *Deque) PushFront(n interface{}) {
	if q.head == q.tail && q.count > 0 {
		nodes := make([]interface{}, len(q.nodes)+q.size)
		copy(nodes, q.nodes[q.head:])
		copy(nodes[len(q.nodes)-q.head:], q.nodes[:q.head])
		q.head = 0
		q.tail = len(q.nodes)
		q.nodes = nodes
	}

	q.head--
	if q.head < 0 {
		q.head = len(q.nodes) - 1
	}
	q.nodes[q.head] = n
	q.count++
}


// PopFront removes the first element of the deque.
func (q *Deque) PopFront() interface{} {
	if q.count == 0 {
		return nil
	}
	node := q.nodes[q.head]
	q.head = (q.head + 1) % len(q.nodes)
	q.count--
	return node
}

// PeekFront return the first element without removing it.
func (q *Deque) PeekFront() interface{} {
	if q.count == 0 {
		return nil
	}
	return q.nodes[q.head]
}

// PopEnd remove the last element of the deque.
func (q *Deque) PopEnd() interface{} {
	if q.count == 0 {
		return nil
	}
	lastIndex := q.tail - 1
	if lastIndex < 0 {
		lastIndex = len(q.nodes) - 1
	}
	q.tail = lastIndex
	q.count--
	return q.nodes[q.tail]
}

// PeekEnd return the last element without removing it.
func (q *Deque) PeekEnd() interface{} {
	if q.count == 0 {
		return nil
	}
	lastIndex := q.tail - 1
	if lastIndex < 0 {
		lastIndex = len(q.nodes) - 1
	}
	return q.nodes[lastIndex]
}
