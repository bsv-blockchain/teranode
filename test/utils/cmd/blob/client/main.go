//go:build !test_all

//nolint:all
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/http"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// BSERVER_URL="127.0.0.1:8081" go run ./test/utils/cmd/blob/client/main.go
func main() {

	serverURL := os.Getenv("BSERVER_URL")
	// Default serverURL if not set
	if len(serverURL) < 1 {
		serverURL = "127.0.0.1:8081"
	}

	logger := ulogger.New("blob-client-test")

	clientStoreURL, err := url.Parse(fmt.Sprintf("http://%v", serverURL))
	if err != nil {
		panic(err)
	}

	client, err := http.New(logger, clientStoreURL)
	if err != nil {
		panic(err)
	}

	if err := testSetAndGet(client); err != nil {
		// if err := testExists(client); err != nil {
		// if err := testSetTTL(client); err != nil {
		// if err := testSetFromReader(client); err != nil {
		// if err := testWithFilename(client); err != nil {
		panic(err)
	} else {
		slog.Info("OK")
	}
}

func testSetAndGet(client *http.HTTPStore) error {
	slog.Info("testSetAndGet")

	key := []byte("testKey1")
	value := []byte("testValue1")

	if err := client.Set(context.Background(), key, value); err != nil {
		return err
	}

	if retrievedValue, err := client.Get(context.Background(), key); err != nil {
		return err
	} else {
		if !bytes.Equal(value, retrievedValue) {
			return errors.NewUnknownError("inconsistent input/output")
		}
	}

	if err := client.Del(context.Background(), key); err != nil {
		return err
	}

	return nil
}

func testExists(client *http.HTTPStore) error {
	slog.Info("testExists")

	key := []byte("testKey3")
	value := []byte("testValue3")

	if err := client.Set(context.Background(), key, value); err != nil {
		return err
	}

	if exists, err := client.Exists(context.Background(), key); err != nil {
		return err
	} else {
		if !exists {
			return errors.NewUnknownError("test exists but does not exist")
		}
	}

	if err := client.Del(context.Background(), key); err != nil {
		return err
	}

	if exists, err := client.Exists(context.Background(), key); err != nil {
		return err
	} else {
		if exists {
			return errors.NewUnknownError("test not exists but do exist")
		}
	}

	return nil
}

func testSetTTL(client *http.HTTPStore) error {
	slog.Info("testSetTTL")

	key := []byte("testKey2")
	value := []byte("testValue2")

	if err := client.Set(context.Background(), key, value); err != nil {
		return err
	}

	if err := client.SetDAH(context.Background(), key, 1); err != nil {
		return err
	}

	time.Sleep(200 * time.Millisecond)

	if _, err := client.Get(context.Background(), key); err == nil {
		return errors.NewUnknownError("expected error but got nil")
	}

	return nil
}

func testSetFromReader(client *http.HTTPStore) error {
	slog.Info("testSetFromReader")

	key := []byte("testKey4")

	largeData := make([]byte, 10*1024*1024) // 10 MB of data
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	reader := bytes.NewReader(largeData)

	if err := client.SetFromReader(context.Background(), key, io.NopCloser(reader)); err != nil {
		return err
	}

	// Retrieve the data
	retrievedReader, err := client.GetIoReader(context.Background(), key)
	if err != nil {
		return err
	}
	defer retrievedReader.Close()

	retrievedData, err := io.ReadAll(retrievedReader)
	if err != nil {
		return err
	}

	if !bytes.Equal(largeData, retrievedData) {
		return errors.NewUnknownError("inconsistent input/output")
	}

	if err := client.Del(context.Background(), key); err != nil {
		return err
	}

	return nil
}

func testWithFilename(client *http.HTTPStore) error {
	slog.Info("testWithFilename")

	key := []byte("testKey5")
	value := []byte("testValue5")

	if err := client.Set(context.Background(), key, value, options.WithFilename("testFilename")); err != nil {
		return err
	}

	if exists, err := client.Exists(context.Background(), key); err != nil {
		return err
	} else {
		if exists {
			return errors.NewUnknownError("test not exists but do exist")
		}
	}

	if exists, err := client.Exists(context.Background(), key, options.WithFilename("testFilename")); err != nil {
		return err
	} else {
		if !exists {
			return errors.NewUnknownError("test exists but do not exist")
		}
	}

	if err := client.Del(context.Background(), key, options.WithFilename("testFilename")); err != nil {
		return err
	}

	return nil
}
