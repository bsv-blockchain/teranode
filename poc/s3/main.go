package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/blob/s3"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func main() {
	u, err := url.Parse("s3://s3.eu-west-1.amazonaws.com/eu-ubsv-txstore?region=eu-west-1&batch=true&writeKeys=true&sizeInBytes=33554432")
	if err != nil {
		panic(err)
	}

	s, err := s3.New(ulogger.TestLogger{}, u)
	if err != nil {
		panic(err)
	}

	err = s.Set(context.Background(), []byte("testklkljkhkjhjhkjhjkhjkhkhkjhkjhjhjkhkjhjkhjkhjkkh"), []byte("stu"))
	if err != nil {
		panic(err)
	}

	v, err := s.Get(context.Background(), []byte("testklkljkhkjhjhkjhjkhjkhkhkjhkjhjhjkhkjhjkhjkhjkkh"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", string(v))
}
