package datastructure

type Distance struct {
	NodeId   uint64
	Distance int64
}

type BinaryMinHeap []*Distance

func (pq BinaryMinHeap) Len() int { return len(pq) }

func (pq BinaryMinHeap) Less(i, j int) bool {
	return pq[i].Distance < pq[j].Distance
}

func (pq BinaryMinHeap) Swap(i, j int) {
	if i < 0 || j < 0 {
		return
	}
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *BinaryMinHeap) Push(x interface{}) {
	*pq = append(*pq, x.(*Distance))
}

func (pq *BinaryMinHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	if n == 0 {
		return nil
	}
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}
