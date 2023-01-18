package utils

func MergeInto[K comparable, V any](dest map[K]V, from map[K]V) {
	if dest == nil {
		dest = make(map[K]V)
	}
	for k, v := range from {
		dest[k] = v
	}
}
