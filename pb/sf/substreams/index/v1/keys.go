package pbsubstreams

func (k *SortedKV) Key(idx int) string {
	return k.Kvs[idx].Key
}
func (k *SortedKV) Len() int {
	return len(k.Kvs)
}
func (k *SortedKV) Less(i, j int) bool { return k.Kvs[i].Key < k.Kvs[j].Key }
func (k *SortedKV) Swap(i, j int)      { k.Kvs[i], k.Kvs[j] = k.Kvs[j], k.Kvs[i] }
