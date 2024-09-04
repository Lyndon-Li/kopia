package searchtree

type node[T1, T2 any] struct {
	key   T1
	value T2
	left  *node[T1, T2]
	right *node[T1, T2]
}

type SearchTree[T1, T2 any] struct {
	root  *node[T1, T2]
	less  func(a, b T1) bool
	equal func(a, b T1) bool
}

func NewSearchTree[T1, T2 any](less func(a, b T1) bool, equal func(a, b T1) bool) *SearchTree[T1, T2] {
	return &SearchTree[T1, T2]{
		less:  less,
		equal: equal,
	}
}

func (st *SearchTree[T1, T2]) Insert(key T1, value T2) {
	n := &node[T1, T2]{key, value, nil, nil}

	if st.root == nil {
		st.root = n
	} else {
		insertTree(st.root, n, st.less)
	}
}

func (st *SearchTree[T1, T2]) Interate(visit func(key T1, value T2)) {
	inOrder(st.root, visit)
}

func (st *SearchTree[T1, T2]) Search(key T1) (bool, T2) {
	return search(st.root, key, st.less, st.equal)
}

func insertTree[T1, T2 any](n *node[T1, T2], insert *node[T1, T2], less func(a, b T1) bool) {
	if less(insert.key, n.key) {
		if n.left == nil {
			n.left = insert
		} else {
			insertTree(n.left, insert, less)
		}
	} else {
		if n.right == nil {
			n.right = insert
		} else {
			insertTree(n.right, insert, less)
		}
	}
}

func inOrder[T1, T2 any](n *node[T1, T2], visit func(key T1, value T2)) {
	if n != nil {
		inOrder(n.left, visit)
		visit(n.key, n.value)
		inOrder(n.right, visit)
	}
}

func search[T1, T2 any](n *node[T1, T2], key T1, less func(a, b T1) bool, equal func(a, b T1) bool) (bool, T2) {
	var value T2
	if n == nil {
		return false, value
	}

	if equal(key, n.key) {
		return true, n.value
	}

	if less(key, n.key) {
		return search(n.left, key, less, equal)
	}

	return search(n.right, key, less, equal)
}
