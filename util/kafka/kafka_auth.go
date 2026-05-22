// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
)

// ValidateKafkaAuthSettings validates the TLS configuration
func ValidateKafkaAuthSettings(kafkaSettings *settings.KafkaSettings) error {
	if kafkaSettings.EnableTLS {
		// Validate CA certificate file
		if kafkaSettings.TLSCAFile != "" {
			if _, err := os.Stat(kafkaSettings.TLSCAFile); os.IsNotExist(err) {
				return errors.New(errors.ERR_CONFIGURATION, "TLS CA certificate file not found: "+kafkaSettings.TLSCAFile)
			}

			// Validate CA certificate format
			caCert, err := os.ReadFile(kafkaSettings.TLSCAFile)
			if err != nil {
				return errors.New(errors.ERR_CONFIGURATION, "failed to read TLS CA file: "+kafkaSettings.TLSCAFile, err)
			}

			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caCert) {
				return errors.New(errors.ERR_CONFIGURATION, "invalid CA certificate format in file: "+kafkaSettings.TLSCAFile)
			}
		}

		// Validate mutual TLS configuration - both cert and key must be provided together
		if (kafkaSettings.TLSCertFile != "") != (kafkaSettings.TLSKeyFile != "") {
			return errors.New(errors.ERR_CONFIGURATION, "TLS client certificate and key must be provided together (cert: "+kafkaSettings.TLSCertFile+", key: "+kafkaSettings.TLSKeyFile+")")
		}

		// Validate client certificate files if provided
		if kafkaSettings.TLSCertFile != "" {
			if _, err := os.Stat(kafkaSettings.TLSCertFile); os.IsNotExist(err) {
				return errors.New(errors.ERR_CONFIGURATION, "TLS certificate file not found: "+kafkaSettings.TLSCertFile)
			}
		}

		if kafkaSettings.TLSKeyFile != "" {
			if _, err := os.Stat(kafkaSettings.TLSKeyFile); os.IsNotExist(err) {
				return errors.New(errors.ERR_CONFIGURATION, "TLS key file not found: "+kafkaSettings.TLSKeyFile)
			}
		}

		// Validate certificate/key pair compatibility if both are provided
		if kafkaSettings.TLSCertFile != "" && kafkaSettings.TLSKeyFile != "" {
			// Try to load the certificate and key to validate they work together
			_, err := tls.LoadX509KeyPair(kafkaSettings.TLSCertFile, kafkaSettings.TLSKeyFile)
			if err != nil {
				return errors.New(errors.ERR_CONFIGURATION, "failed to load TLS certificate/key pair (cert: "+kafkaSettings.TLSCertFile+", key: "+kafkaSettings.TLSKeyFile+")", err)
			}
		}
	}

	return nil
}
