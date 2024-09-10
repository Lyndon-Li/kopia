package searchtree

func NewAddressableMap[T1, T2 any](less func(a, b T1) bool, equal func(a, b T1) bool) *AddressableMap[T1, T2] {
	return &AddressableMap[T1, T2]{
		less:  less,
		equal: equal,
	}
}

type pair[T1, T2 any] struct {
	key   T1
	value T2
}

type node[T1, T2 any] struct {
	pair  pair[T1, T2]
	index int
	left  *node[T1, T2]
	right *node[T1, T2]
}

type AddressableMap[T1, T2 any] struct {
	root  *node[T1, T2]
	store []pair[T1, T2]
	less  func(a, b T1) bool
	equal func(a, b T1) bool
}

func (mp *AddressableMap[T1, T2]) Insert(key T1, value T2) (int, bool) {
	n := &node[T1, T2]{pair[T1, T2]{key, value}, len(mp.store), nil, nil}
	inserted := n

	if mp.root == nil {
		mp.root = n
	} else {
		inserted = mp.insertTree(mp.root, n, mp.less, mp.equal)
	}

	if inserted == n {
		mp.store = append(mp.store, pair[T1, T2]{key, value})
	}

	return inserted.index, (inserted == n)
}

func (mp *AddressableMap[T1, T2]) Interate(visit func(key T1, value T2) bool) {
	mp.inOrder(mp.root, visit)
}

func (mp *AddressableMap[T1, T2]) Search(key T1) (bool, T2) {
	return mp.search(mp.root, key, mp.less, mp.equal)
}

func (mp *AddressableMap[T1, T2]) Index(idx int) (bool, T2) {
	if len(mp.store) < idx {
		var value T2
		return false, value
	}

	return true, mp.store[idx].value
}

func (mp *AddressableMap[T1, T2]) Clone(from *AddressableMap[T1, T2]) {
	from.inOrder(mp.root, func(key T1, value T2) bool {
		mp.Insert(key, value)
		return true
	})
}

func (mp *AddressableMap[T1, T2]) Length() int {
	return len(mp.store)
}

func (mp *AddressableMap[T1, T2]) Delete() {

}

func (mp *AddressableMap[T1, T2]) insertTree(n *node[T1, T2], insert *node[T1, T2], less func(a, b T1) bool, equal func(a, b T1) bool) *node[T1, T2] {
	ret := insert

	if less(mp.store[insert.index].key, mp.store[n.index].key) {
		if n.left == nil {
			n.left = insert
		} else {
			ret = mp.insertTree(n.left, insert, less, equal)
		}
	} else if equal(mp.store[insert.index].key, mp.store[n.index].key) {
		ret = n
	} else {
		if n.right == nil {
			n.right = insert
		} else {
			ret = mp.insertTree(n.right, insert, less, equal)
		}
	}

	return ret
}

func (mp *AddressableMap[T1, T2]) inOrder(n *node[T1, T2], visit func(key T1, value T2) bool) {
	if n != nil {
		mp.inOrder(n.left, visit)

		if !visit(mp.store[n.index].key, mp.store[n.index].value) {
			return
		}

		mp.inOrder(n.right, visit)
	}
}

func (mp *AddressableMap[T1, T2]) search(n *node[T1, T2], key T1, less func(a, b T1) bool, equal func(a, b T1) bool) (bool, T2) {
	var value T2
	if n == nil {
		return false, value
	}

	if equal(key, mp.store[n.index].key) {
		return true, mp.store[n.index].value
	}

	if less(key, mp.store[n.index].key) {
		return mp.search(n.left, key, less, equal)
	}

	return mp.search(n.right, key, less, equal)
}
