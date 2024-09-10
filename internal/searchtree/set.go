package searchtree

type nodeSt[T any] struct {
	value T
	index int
	left  *nodeSt[T]
	right *nodeSt[T]
}

type AddressableSet[T any] struct {
	root  *nodeSt[T]
	store []T
	less  func(a, b T) bool
	equal func(a, b T) bool
}

func NewAddressableSet[T any](less func(a, b T) bool, equal func(a, b T) bool) *AddressableSet[T] {
	return &AddressableSet[T]{
		less:  less,
		equal: equal,
	}
}

func (st *AddressableSet[T]) Insert(value T) (int, bool) {
	n := &nodeSt[T]{value, len(st.store), nil, nil}
	inserted := n

	if st.root == nil {
		st.root = n
	} else {
		inserted = st.insertTree(st.root, n, st.less, st.equal)
	}

	if inserted == n {
		st.store = append(st.store, value)
	}

	return inserted.index, (inserted == n)
}

func (st *AddressableSet[T]) Interate(visit func(value T) bool) {
	st.inOrder(st.root, visit)
}

func (st *AddressableSet[T]) Search(value T) bool {
	return st.search(st.root, value, st.less, st.equal)
}

func (st *AddressableSet[T]) Index(idx int) (bool, T) {
	if len(st.store) < idx {
		var value T
		return false, value
	}

	return true, st.store[idx]
}

func (st *AddressableSet[T]) Clone(from *AddressableSet[T]) {
	from.inOrder(st.root, func(value T) bool {
		st.Insert(value)
		return true
	})
}

func (st *AddressableSet[T]) insertTree(n *nodeSt[T], insert *nodeSt[T], less func(a, b T) bool, equal func(a, b T) bool) *nodeSt[T] {
	ret := insert

	if less(st.store[insert.index], st.store[n.index]) {
		if n.left == nil {
			n.left = insert
		} else {
			ret = st.insertTree(n.left, insert, less, equal)
		}
	} else if equal(st.store[insert.index], st.store[n.index]) {
		ret = n
	} else {
		if n.right == nil {
			n.right = insert
		} else {
			ret = st.insertTree(n.right, insert, less, equal)
		}
	}

	return ret
}

func (st *AddressableSet[T]) inOrder(n *nodeSt[T], visit func(value T) bool) {
	if n != nil {
		st.inOrder(n.left, visit)

		if !visit(st.store[n.index]) {
			return
		}

		st.inOrder(n.right, visit)
	}
}

func (st *AddressableSet[T]) search(n *nodeSt[T], value T, less func(a, b T) bool, equal func(a, b T) bool) bool {
	if n == nil {
		return false
	}

	if equal(value, st.store[n.index]) {
		return true
	}

	if less(value, st.store[n.index]) {
		return st.search(n.left, value, less, equal)
	}

	return st.search(n.right, value, less, equal)
}
