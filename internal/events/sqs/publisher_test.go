package sqs

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Octavian-Mihai/shorter-url/internal/events"
)

type fakeAPI struct {
	calls      [][]types.SendMessageBatchRequestEntry
	rejectOnce map[string]bool // body-independent: reject entry Ids on first call
	rejectAll  bool
	err        error
}

func (f *fakeAPI) SendMessageBatch(_ context.Context, in *awssqs.SendMessageBatchInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.calls = append(f.calls, in.Entries)
	out := &awssqs.SendMessageBatchOutput{}
	for _, e := range in.Entries {
		if f.rejectAll || (len(f.calls) == 1 && f.rejectOnce[aws.ToString(e.Id)]) {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("ServiceUnavailable")})
		}
	}
	return out, nil
}
func (f *fakeAPI) ReceiveMessage(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	return &awssqs.ReceiveMessageOutput{}, nil
}
func (f *fakeAPI) DeleteMessageBatch(context.Context, *awssqs.DeleteMessageBatchInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error) {
	return &awssqs.DeleteMessageBatchOutput{}, nil
}

func clicks(n int) []events.Click {
	out := make([]events.Click, n)
	for i := range out {
		out[i] = events.NewClick("slug"+strconv.Itoa(i), time.Now())
	}
	return out
}

func TestPublishChunksByTen(t *testing.T) {
	f := &fakeAPI{}
	if err := NewPublisher(f, "q").Publish(context.Background(), clicks(25)); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 3 || len(f.calls[0]) != 10 || len(f.calls[2]) != 5 {
		t.Errorf("chunks = %d (%v), want 10/10/5", len(f.calls), len(f.calls[0]))
	}
}

func TestPublishRetriesRejectedEntriesOnce(t *testing.T) {
	f := &fakeAPI{rejectOnce: map[string]bool{"1": true, "3": true}}
	if err := NewPublisher(f, "q").Publish(context.Background(), clicks(5)); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 2 || len(f.calls[1]) != 2 {
		t.Errorf("calls = %d, retry size = %d, want a retry of just the 2 rejected", len(f.calls), len(f.calls[1]))
	}
}

func TestPublishFailsWhenStillRejected(t *testing.T) {
	f := &fakeAPI{rejectAll: true}
	if err := NewPublisher(f, "q").Publish(context.Background(), clicks(3)); err == nil {
		t.Fatal("expected error when SQS keeps rejecting")
	}
}

func TestPublishPropagatesAPIError(t *testing.T) {
	boom := errors.New("network")
	if err := NewPublisher(&fakeAPI{err: boom}, "q").Publish(context.Background(), clicks(1)); !errors.Is(err, boom) {
		t.Errorf("err = %v", err)
	}
}

func TestBodyRoundTrip(t *testing.T) {
	f := &fakeAPI{}
	c := clicks(1)
	_ = NewPublisher(f, "q").Publish(context.Background(), c)
	got, err := Decode(aws.ToString(f.calls[0][0].MessageBody))
	if err != nil || got.EventID != c[0].EventID || got.Slug != c[0].Slug {
		t.Fatalf("got %+v, %v", got, err)
	}
	if _, err := Decode("{nope"); err == nil {
		t.Error("expected decode error")
	}
}
