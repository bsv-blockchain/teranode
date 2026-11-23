//go:build linux

package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/stretchr/testify/require"
)

// SubmitMiningSolution and GetCandidateBlock read a job's subtrees after they took the job
// from the store, and a solution already mined can delete every job meanwhile. The hold
// must keep the subtrees mapped across Delete, DeleteAll and an overwrite of the id, and
// let them go once it is released.
func TestHeldJobSubtreesStayMappedAfterTheJobLeavesTheStore(t *testing.T) {
	leave := map[string]func(m *mmapJobServer, id chainhash.Hash){
		"Delete":    func(m *mmapJobServer, id chainhash.Hash) { m.ba.jobStore.Delete(id) },
		"DeleteAll": func(m *mmapJobServer, _ chainhash.Hash) { m.ba.jobStore.DeleteAll() },
		"overwrite": func(m *mmapJobServer, id chainhash.Hash) {
			old := m.ba.jobStore.Get(id).Value()
			m.ba.jobStore.Set(id, &subtreeprocessor.Job{ID: old.ID, MiningCandidate: old.MiningCandidate}, jobTTL)
		},
	}

	for name, leaveStore := range leave {
		t.Run(name, func(t *testing.T) {
			m := newMmapJobServer(t, false)

			id, job := m.storeJob(t)

			hold := m.ba.readerPins.NewHold()
			require.NotNil(t, m.ba.holdJob(hold, id))

			leaveStore(m, id)
			m.reset(t)

			m.requireNotInstalled(t, job)
			m.requireStillMapped(t, job, "a request that took the job")

			hold.Release()
			m.reset(t)

			m.requireClosed(t, job)
		})
	}
}
