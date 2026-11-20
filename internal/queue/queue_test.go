package queue

import "testing"

func TestQualified(t *testing.T) {
	got := qualified("n", "id, batch_id,\n\tstatus")
	if want := "n.id, n.batch_id, n.status"; got != want {
		t.Errorf("qualified = %q, want %q", got, want)
	}
}
