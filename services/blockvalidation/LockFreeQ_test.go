package blockvalidation

import (
	"sync"
	"testing"
)

func TestEnqueueDequeue(t *testing.T) {
	q := NewLockFreeQ[int]() // Assuming your LockFreeQ works with int for this example

	// Enqueue elements
	q.enqueue(1)
	q.enqueue(2)

	// Dequeue and check elements
	if val := q.dequeue(); val == nil || *val != 1 {
		t.Errorf("Expected 1, got %v", val)
	}

	if val := q.dequeue(); val == nil || *val != 2 {
		t.Errorf("Expected 2, got %v", val)
	}

	// Check if queue is empty
	if !q.isEmpty() {
		t.Errorf("Expected queue to be empty")
	}
}

func TestConcurrentEnqueue(t *testing.T) {
	q := NewLockFreeQ[int]()
	var wg sync.WaitGroup
	numWorkers := 100 // Number of concurrent goroutines
	numEnqueues := 10 // Number of enqueues per goroutine

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numEnqueues; j++ {
				q.enqueue(workerID*numEnqueues + j)
			}
		}(i)
	}

	wg.Wait()

	// Assuming your dequeue is not concurrently safe, this part is tricky.
	// We can't guarantee the order of elements, but we can check if all elements are present.
	// This part of the test will need adjustment based on your dequeue method's thread safety.
	seen := make(map[int]bool)
	for i := 0; i < numWorkers*numEnqueues; i++ {
		val := q.dequeue()
		if val == nil {
			t.Fatalf("Expected a value, got nil at iteration %d", i)
		}
		if seen[*val] {
			t.Errorf("Duplicate value detected: %v", *val)
		}
		seen[*val] = true
	}

	if !q.isEmpty() {
		t.Errorf("Expected queue to be empty after all dequeues")
	}
}
