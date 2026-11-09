package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestBlockFoundReply_AnswersOnceAndNeverBlocks(t *testing.T) {
	reply := newBlockFoundReply()

	first := errors.NewProcessingError("first")
	reply.answer(first)
	reply.answer(nil) // a second answer must be dropped, and must not block with no reader

	require.True(t, reply.isAnswered())
	require.Equal(t, first, <-reply.ch)

	select {
	case extra := <-reply.ch:
		t.Fatalf("a second answer reached the caller: %v", extra)
	default:
	}
}

func TestBlockFoundReply_NilIsANoOp(t *testing.T) {
	var reply *blockFoundReply

	reply.answer(nil)
	reply.join(newBlockFoundReply())

	require.True(t, reply.isAnswered(), "a nil reply has no caller to answer")
}

func TestBlockFoundReply_JoinedCallerGetsTheSameAnswer(t *testing.T) {
	queued := newBlockFoundReply()
	second := newBlockFoundReply()

	queued.join(second)
	queued.join(queued) // joining itself must not loop

	verdict := errors.NewBlockInvalidError("bad block")
	queued.answer(verdict)

	require.Equal(t, verdict, <-queued.ch)
	require.Equal(t, verdict, <-second.ch)
}

func TestBlockFoundReply_JoinAfterAnswerAnswersAtOnce(t *testing.T) {
	queued := newBlockFoundReply()
	queued.answer(nil)
	require.NoError(t, <-queued.ch)

	late := newBlockFoundReply()
	queued.join(late)

	require.True(t, late.isAnswered())
	require.NoError(t, <-late.ch)
}

func TestMergeReply(t *testing.T) {
	b := newBlockFoundReply()
	require.Same(t, b, mergeReply(nil, b))

	a := newBlockFoundReply()
	require.Same(t, a, mergeReply(a, nil))

	merged := mergeReply(a, b)
	require.Same(t, a, merged)

	merged.answer(nil)
	require.NoError(t, <-a.ch)
	require.NoError(t, <-b.ch)
}
