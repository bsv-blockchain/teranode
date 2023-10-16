package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
)

type SimonStore struct {
	conn       net.Conn
	responseCh chan string
}

func NewSimonStore() Store {
	conn, err := net.Dial("tcp", "localhost:22121")
	if err != nil {
		log.Fatal(err)
	}

	responseCh := make(chan string)

	go func() {
		buf := make([]byte, 1024)

		for {
			if _, err := conn.Read(buf); err != nil {
				break
			}

			responseCh <- string(buf)
		}

		close(responseCh)
	}()

	return &SimonStore{
		conn:       conn,
		responseCh: responseCh,
	}
}

func (s *SimonStore) SetAndGet(ctx context.Context, key string, value string) (string, bool, error) {
	if ok := s.setNx(key, value); ok {
		return value, true, nil
	}

	existing, err := s.get(key)
	if err != nil {
		return "", false, err
	}
	return existing, existing == value, nil
}

func (s *SimonStore) Delete(ctx context.Context, key string) error {
	return s.del(key)
}

func (s *SimonStore) setNx(key string, value string) bool {
	fmt.Fprintf(s.conn, "*3\r\n$5\r\nSETNX\r\n$3\r\n%s\r\n$4\r\n%s\r\n", key, value)

	return <-s.responseCh == ":1\r\n"
}

func (s *SimonStore) get(key string) (string, error) {
	fmt.Fprintf(s.conn, "*2\r\n$3\r\nGET\r\n$3\r\n%s\r\n", key)
	response := <-s.responseCh

	if response == "$-1\r\n" {
		return "", errors.New("key not found")
	}

	return response[1 : len(response)-2], nil
}

func (s *SimonStore) del(key string) error {
	fmt.Fprintf(s.conn, "*2\r\n$3\r\nDEL\r\n$3\r\n%s\r\n", key)
	response := <-s.responseCh
	if response != ":1\r\n" {
		return errors.New(response)
	}

	return nil
}
