package httpapi

import "testing"

func TestRatio(t *testing.T) {
	if r := ratio(0, 0); r != nil {
		t.Errorf("ratio(0,0) = %v, want nil (unknown, not 0%% or 100%%)", *r)
	}
	if r := ratio(2, 3); r == nil || *r != 0.6667 {
		t.Errorf("ratio(2,3) = %v, want 0.6667", r)
	}
}

func TestRound1(t *testing.T) {
	v := 12.345
	if r := round1(&v); *r != 12.3 {
		t.Errorf("round1 = %v", *r)
	}
	if round1(nil) != nil {
		t.Error("round1(nil) must stay nil")
	}
}
