// Package domain holds the core vocabulary of the service: channels,
// priorities, statuses and the rules for moving between statuses. It has no
// dependencies on storage or transport so it can be reasoned about in isolation.
package domain

// Channel is a delivery medium. Values match the CHECK constraint in the
// notifications table.
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel; workers start one pool per entry.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

// Priority controls claim order. While several priorities wait, a channel's
// sends are shared by weight (70/20/10 by default, so low is never starved);
// spare capacity goes to the highest priority (docs/DECISIONS.md, ADR-022).
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

var Priorities = []Priority{PriorityHigh, PriorityNormal, PriorityLow}

// Rank orders priorities for claiming: lower is claimed first. It mirrors
// the priority_rank generated column in the notifications table.
func (p Priority) Rank() int {
	switch p {
	case PriorityHigh:
		return 0
	case PriorityNormal:
		return 1
	default:
		return 2
	}
}

func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

// Status is the lifecycle state of a notification.
type Status string

const (
	// StatusScheduled: created with a future scheduled_at; becomes claimable
	// once next_attempt_at (= scheduled_at) is due.
	StatusScheduled Status = "scheduled"
	// StatusPending: waiting to be claimed by a worker.
	StatusPending Status = "pending"
	// StatusProcessing: claimed by a worker that holds a lease (locked_until).
	StatusProcessing Status = "processing"
	// StatusRetrying: last attempt failed with a retryable error; claimable
	// again once next_attempt_at (the backoff deadline) is due.
	StatusRetrying Status = "retrying"
	// StatusSent: the provider accepted the message (HTTP 202).
	StatusSent Status = "sent"
	// StatusFailed: permanent error or attempts exhausted. Acts as the
	// dead-letter set.
	StatusFailed Status = "failed"
	// StatusCancelled: cancelled through the API before it was sent.
	StatusCancelled Status = "cancelled"
)

var Statuses = []Status{
	StatusScheduled, StatusPending, StatusProcessing, StatusRetrying,
	StatusSent, StatusFailed, StatusCancelled,
}

func (s Status) Valid() bool {
	for _, known := range Statuses {
		if s == known {
			return true
		}
	}
	return false
}

// Terminal reports whether no further transition is possible (except a
// manual retry of a failed notification).
func (s Status) Terminal() bool {
	return s == StatusSent || s == StatusFailed || s == StatusCancelled
}

// Cancellable reports whether the API may cancel a notification in this
// status. A notification in "processing" is owned by a worker and may
// already be at the provider, so it cannot be cancelled.
func (s Status) Cancellable() bool {
	return s == StatusScheduled || s == StatusPending || s == StatusRetrying
}
