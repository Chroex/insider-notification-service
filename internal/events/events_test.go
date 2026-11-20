package events

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"notification-service/internal/domain"
)

func TestFromNotification_ErrorOnlyOnOutcomes(t *testing.T) {
	msg := "provider returned HTTP 503"
	for status, wantErr := range map[domain.Status]bool{
		domain.StatusRetrying:   true,
		domain.StatusFailed:     true,
		domain.StatusProcessing: false, // a new attempt: the old error is not current
		domain.StatusSent:       false,
		domain.StatusPending:    false,
	} {
		n := domain.Notification{ID: uuid.New(), Channel: domain.ChannelSMS, Status: status, LastError: &msg, Attempts: 2}
		ev := FromNotification(n, time.Now())
		if (ev.Error != "") != wantErr {
			t.Errorf("%s: error = %q, want present=%v", status, ev.Error, wantErr)
		}
		if ev.Status != string(status) || ev.Attempt != 2 || ev.At.Location() != time.UTC {
			t.Errorf("%s: event = %+v", status, ev)
		}
	}
}
