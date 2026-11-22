package worker

import "notification-service/internal/queue"

// fairShare splits each claim's slots between priorities by weight, so a
// flood of high priority cannot starve low (ADR-022). It is smooth weighted
// round robin (the algorithm nginx uses for upstream weights): every slot
// goes to the priority with the most accumulated credit, which then pays the
// sum of all weights. Over any run of slots each priority gets its weight's
// share, interleaved rather than bunched, and the credit carries across
// claims, so it holds even when a saturated worker claims one row at a time.
//
// Reserved slots are a guarantee, not a cap: a slot reserved for a priority
// with nothing due goes to the rest of the backlog in priority order
// (queue.Quota), so with only high traffic everything goes to high.
//
// One fairShare per channel, used only by that channel's claim loop.
type fairShare struct {
	weights [3]int // by priority rank: high, normal, low
	credit  [3]int
	sum     int
}

func newFairShare(weights [3]int) *fairShare {
	return &fairShare{weights: weights, sum: weights[0] + weights[1] + weights[2]}
}

// quota plans a claim of n slots.
func (f *fairShare) quota(n int) queue.Quota {
	q := queue.Quota{Total: n}
	if f.weights[1] == 0 && f.weights[2] == 0 {
		return q // nothing guaranteed below high: strict priority
	}
	for range n {
		best := -1
		for r, w := range f.weights {
			if w == 0 {
				continue
			}
			f.credit[r] += w
			if best < 0 || f.credit[r] > f.credit[best] {
				best = r
			}
		}
		f.credit[best] -= f.sum
		q.Reserved[best]++
	}
	return q
}
