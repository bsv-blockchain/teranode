//nolint:stylecheck
package blockchain_api

func (s FSMStateType) Is(compareState FSMStateType) bool {
	return s.String() == compareState.String()
}
