package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/google/uuid"

	"github.com/Octavian-Mihai/shorter-url/internal/events"
)

type fakeSQS struct {
	pages    [][]types.Message // each ReceiveMessage returns the next page
	deleted  [][]string
	failDel  []types.BatchResultErrorEntry
	receives int
}

func (f *fakeSQS) SendMessageBatch(context.Context, *awssqs.SendMessageBatchInput, ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error) {
	return nil, nil
}
func (f *fakeSQS) ReceiveMessage(_ context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	f.receives++
	if len(f.pages) == 0 {
		return &awssqs.ReceiveMessageOutput{}, nil
	}
	p := f.pages[0]
	f.pages = f.pages[1:]
	return &awssqs.ReceiveMessageOutput{Messages: p}, nil
}
func (f *fakeSQS) DeleteMessageBatch(_ context.Context, in *awssqs.DeleteMessageBatchInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error) {
	var hs []string
	for _, e := range in.Entries {
		hs = append(hs, aws.ToString(e.ReceiptHandle))
	}
	f.deleted = append(f.deleted, hs)
	return &awssqs.DeleteMessageBatchOutput{Failed: f.failDel}, nil
}

func msg(handle string, c events.Click) types.Message {
	b, _ := json.Marshal(c)
	return types.Message{Body: aws.String(string(b)), ReceiptHandle: aws.String(handle)}
}

func TestSQSSourceFetchBuffersPagesAndFlagsPoison(t *testing.T) {
	c1 := events.Click{EventID: uuid.NewString(), Slug: "a"}
	f := &fakeSQS{pages: [][]types.Message{
		{msg("h1", c1), {Body: aws.String("{bad"), ReceiptHandle: aws.String("h2")}},
		{},
	}}
	s := NewSQSSource(f, "q", nil)
	d1, err := s.Fetch(context.Background())
	if err != nil || d1.Poison || d1.Click.EventID != c1.EventID {
		t.Fatalf("d1 = %+v, %v", d1, err)
	}
	d2, _ := s.Fetch(context.Background())
	if !d2.Poison {
		t.Error("undecodable body must be flagged poison")
	}
	// empty page (long-poll timeout) must loop, not return
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Fetch(ctx); err == nil && f.receives < 2 {
		t.Error("expected another receive after empty buffer")
	}
}

func TestSQSSourceCommitDeletesInChunksOfTen(t *testing.T) {
	f := &fakeSQS{}
	s := NewSQSSource(f, "q", nil)
	var batch []Delivery
	for i := 0; i < 23; i++ {
		batch = append(batch, Delivery{Token: msg("h"+string(rune('a'+i)), events.Click{})})
	}
	if err := s.Commit(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(f.deleted) != 3 || len(f.deleted[0]) != 10 || len(f.deleted[2]) != 3 {
		t.Errorf("delete calls = %v", f.deleted)
	}
}

func TestSQSSourceCommitSenderFaultIsNotRetried(t *testing.T) {
	f := &fakeSQS{failDel: []types.BatchResultErrorEntry{{Id: aws.String("0"), Code: aws.String("ReceiptHandleIsInvalid"), SenderFault: true}}}
	s := NewSQSSource(f, "q", nil)
	if err := s.Commit(context.Background(), []Delivery{{Token: msg("h", events.Click{})}}); err != nil {
		t.Errorf("stale-handle failure must not wedge the consumer: %v", err)
	}
}

func TestSQSSourceCommitServerFaultIsRetriable(t *testing.T) {
	f := &fakeSQS{failDel: []types.BatchResultErrorEntry{{Id: aws.String("0"), Code: aws.String("ServiceUnavailable")}}}
	s := NewSQSSource(f, "q", nil)
	if err := s.Commit(context.Background(), []Delivery{{Token: msg("h", events.Click{})}}); err == nil {
		t.Error("server-side failure should surface so the consumer retries")
	}
}
