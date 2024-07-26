package util

import (
	"testing"
)

// Test_NewKafkaConsumer consumers with both manual and autocommits
func Test_NewKafkaConsumer(t *testing.T) {
	workerCh := make(chan KafkaMessage)
	consumer := NewKafkaConsumer(workerCh, true)
	if consumer.autoCommitEnabled != true {
		t.Errorf("Expected autoCommitEnabled to be true, got %v", consumer.autoCommitEnabled)
	}
	consumer = NewKafkaConsumer(workerCh, false)
	if consumer.autoCommitEnabled != false {
		t.Errorf("Expected autoCommitEnabled to be false, got %v", consumer.autoCommitEnabled)
	}

}
