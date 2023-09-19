package main

import (
	"context"
	"sync"

	"github.com/libsv/go-bt/v2/chainhash"
)

type Strategy interface {
	Storer(ctx context.Context, id int, wg *sync.WaitGroup, spenderCh chan *chainhash.Hash, counterCh chan int)
	Spender(ctx context.Context, wg *sync.WaitGroup, spenderCh chan *chainhash.Hash, deleterCh chan *chainhash.Hash, counterCh chan int)
	Deleter(ctx context.Context, wg *sync.WaitGroup, deleteCh chan *chainhash.Hash, counterCh chan int)
}
