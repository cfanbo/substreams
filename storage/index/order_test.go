package index

import (
	"sort"
	"testing"

	"github.com/twotwotwo/sorts"
)

type kv struct {
	key   string
	value []byte
}

type ByKey []kv

func (b ByKey) Key(idx int) string {
	return b[idx].key
}
func (b ByKey) Len() int {
	return len(b)
}

func (a ByKey) Less(i, j int) bool { return a[i].key < a[j].key }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func BenchmarkOrderMap(b *testing.B) {
	m := make(map[string][]byte, 100000)
	for i := 0; i < 100000; i++ {
		m[string(rune(i)+rune(i+1)+rune(i+2))] = []byte(string(rune(i) + rune(i+3)))
	}

	b.Run("range-and-sort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			arr := make([]kv, 0, len(m))
			for k, v := range m {
				arr = append(arr, kv{k, v})
			}
			sort.Slice(arr, func(i, j int) bool {
				return arr[i].key < arr[j].key
			})
		}
	})

	b.Run("sorts-package", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			arr := make(ByKey, 0, len(m))
			for k, v := range m {
				arr = append(arr, kv{k, v})
			}
			sorts.ByString(arr)
		}
	})

}
