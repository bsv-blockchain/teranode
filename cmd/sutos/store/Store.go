package store

import "fmt"

func New(engine string) Store {
	switch engine {
	case "memory":
		return NewMemoryStore()
	case "redis":
		return NewRedisStore()
	case "nutcracker":
		return NewNutcrackerStore()
	default:
		panic(fmt.Sprintf("unknown store engine: %s", engine))
	}
}
