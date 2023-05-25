//go:build aerospike

package aerospike

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
)

func initTLS() *tls.Config {
	// Try to load system CA certs, otherwise just make an empty pool
	serverPool, err := x509.SystemCertPool()
	if serverPool == nil || err != nil {
		log.Printf("Adding system certificates to the cert pool failed: %s", err)
		serverPool = x509.NewCertPool()
	}

	// // Try to load system CA certs and add them to the system cert pool
	// caCert, err := readFromFile(*rootCA)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// log.Printf("Adding CA certificate to the pool...")
	// serverPool.AppendCertsFromPEM(caCert)

	var clientPool []tls.Certificate

	// Read cert file
	certFileBytes, err := readFromFile("./certificate.pem")
	if err != nil {
		log.Fatal(err)
	}

	// Read key file
	keyFileBytes, err := readFromFile("./key.pem")
	if err != nil {
		log.Fatal(err)
	}

	// Decode PEM data
	keyBlock, _ := pem.Decode(keyFileBytes)
	certBlock, _ := pem.Decode(certFileBytes)

	if keyBlock == nil || certBlock == nil {
		log.Fatalf("Failed to decode PEM data for key or certificate")
	}

	// // Check and Decrypt the the Key Block using passphrase
	// if x509.IsEncryptedPEMBlock(keyBlock) {
	// 	decryptedDERBytes, err := x509.DecryptPEMBlock(keyBlock, []byte(*keyFilePassphrase))
	// 	if err != nil {
	// 		log.Fatalf("Failed to decrypt PEM Block: `%s`", err)
	// 	}

	// 	keyBlock.Bytes = decryptedDERBytes
	// 	keyBlock.Headers = nil
	// }

	// Encode PEM data
	keyPEM := pem.EncodeToMemory(keyBlock)
	certPEM := pem.EncodeToMemory(certBlock)

	if keyPEM == nil || certPEM == nil {
		log.Fatalf("Failed to encode PEM data for key or certificate")
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)

	if err != nil {
		log.Fatalf("Failed to add client certificate and key to the pool: `%s`", err)
	}

	log.Printf("Adding client certificate and key to the pool...")
	clientPool = append(clientPool, cert)

	tlsConfig := &tls.Config{
		Certificates:             clientPool,
		RootCAs:                  serverPool,
		InsecureSkipVerify:       true,
		PreferServerCipherSuites: true,
	}
	tlsConfig.BuildNameToCertificate()

	return tlsConfig
}

// Read content from file
func readFromFile(filePath string) ([]byte, error) {
	dataBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("Failed to read from file `%s`: `%v`", filePath, err)
	}

	data := bytes.TrimSuffix(dataBytes, []byte("\n"))

	return data, nil
}
