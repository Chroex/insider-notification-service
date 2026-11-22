package worker

import "testing"

// Over any 10 claimed slots, 70/20/10 weights reserve exactly 7/2/1, even
// when the slots come one claim at a time (the usual case when a worker
// is saturated and frees one slot per finished send).
func TestFairShare_ReservesSlotsByWeightAcrossClaims(t *testing.T) {
	for _, claimSize := range []int{1, 3, 10} {
		f := newFairShare([3]int{70, 20, 10})
		var got [3]int
		for claimed := 0; claimed < 100; {
			n := min(claimSize, 100-claimed)
			q := f.quota(n)
			if q.Total != n || q.Reserved[0]+q.Reserved[1]+q.Reserved[2] != n {
				t.Fatalf("claim of %d: quota %+v does not add up", n, q)
			}
			for p := range got {
				got[p] += q.Reserved[p]
			}
			claimed += n
		}
		if got != [3]int{70, 20, 10} {
			t.Errorf("claims of %d: reserved %v over 100 slots, want [70 20 10]", claimSize, got)
		}
	}
}

// Low gets its turn early instead of after every high: with 70/20/10 the
// first low slot comes within the first 10.
func TestFairShare_InterleavesInsteadOfBunching(t *testing.T) {
	f := newFairShare([3]int{70, 20, 10})
	for i := 1; i <= 10; i++ {
		if f.quota(1).Reserved[2] == 1 {
			return
		}
	}
	t.Error("no low slot in the first 10 single-slot claims")
}

// No weights (or weights only for high) means strict priority: nothing is
// reserved below high, so the claim fills by priority order.
func TestFairShare_StrictWhenLowerWeightsAreZero(t *testing.T) {
	for _, w := range [][3]int{{0, 0, 0}, {1, 0, 0}} {
		f := newFairShare(w)
		for range 20 {
			q := f.quota(5)
			if q.Total != 5 || q.Reserved[1] != 0 || q.Reserved[2] != 0 {
				t.Fatalf("weights %v: quota %+v reserves below high", w, q)
			}
		}
	}
}
