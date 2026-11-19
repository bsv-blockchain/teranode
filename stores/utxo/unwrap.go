package utxo

// Unwrapper is implemented by a Store that wraps another Store, such as the
// logging wrapper in stores/utxo/logger.
type Unwrapper interface {
	// Unwrap returns the wrapped store.
	Unwrap() Store
}

// As looks for a T in s and then in each store s wraps, outermost first, and
// returns the first one found. Use it in place of a plain type assertion when
// checking a store for an optional method or a concrete type, so a wrapper does
// not hide what the wrapped store can do.
func As[T any](s Store) (T, bool) {
	for s != nil {
		if found, ok := s.(T); ok {
			return found, true
		}

		wrapper, ok := s.(Unwrapper)
		if !ok {
			break
		}

		s = wrapper.Unwrap()
	}

	var zero T

	return zero, false
}
