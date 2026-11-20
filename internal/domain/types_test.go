package domain

import "testing"

func TestChannelValid(t *testing.T) {
	tests := []struct {
		in   Channel
		want bool
	}{
		{ChannelSMS, true}, {ChannelEmail, true}, {ChannelPush, true},
		{"fax", false}, {"", false}, {"SMS", false},
	}
	for _, tt := range tests {
		if got := tt.in.Valid(); got != tt.want {
			t.Errorf("Channel(%q).Valid() = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestPriorityValid(t *testing.T) {
	tests := []struct {
		in   Priority
		want bool
	}{
		{PriorityHigh, true}, {PriorityNormal, true}, {PriorityLow, true},
		{"urgent", false}, {"", false},
	}
	for _, tt := range tests {
		if got := tt.in.Valid(); got != tt.want {
			t.Errorf("Priority(%q).Valid() = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestStatusRules(t *testing.T) {
	tests := []struct {
		status      Status
		cancellable bool
		terminal    bool
	}{
		{StatusScheduled, true, false},
		{StatusPending, true, false},
		{StatusRetrying, true, false},
		{StatusProcessing, false, false},
		{StatusSent, false, true},
		{StatusFailed, false, true},
		{StatusCancelled, false, true},
	}
	for _, tt := range tests {
		if got := tt.status.Cancellable(); got != tt.cancellable {
			t.Errorf("%s.Cancellable() = %v, want %v", tt.status, got, tt.cancellable)
		}
		if got := tt.status.Terminal(); got != tt.terminal {
			t.Errorf("%s.Terminal() = %v, want %v", tt.status, got, tt.terminal)
		}
		if !tt.status.Valid() {
			t.Errorf("%s.Valid() = false", tt.status)
		}
	}
	if Status("unknown").Valid() {
		t.Error(`Status("unknown").Valid() = true`)
	}
}
