package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Octavian-Mihai/shorter-url/internal/events/sqs"
)

// SQSSource consumes click events from an SQS queue.
//
// SQS has no offsets: "commit" means DeleteMessageBatch. Anything not deleted
// reappears after the visibility timeout (set it comfortably above
// FlushEvery + insert time), which is exactly at-least-once. Several
// consumer instances can poll the same queue; SQS spreads messages between them.
type SQSSource struct {
	api      sqs.API
	queueURL string
	wait     int32
	buf      []Delivery
	log      *slog.Logger
}

var _ Source = (*SQSSource)(nil)

func NewSQSSource(api sqs.API, queueURL string, log *slog.Logger) *SQSSource {
	if log == nil {
		log = slog.Default()
	}
	return &SQSSource{api: api, queueURL: queueURL, wait: 20, log: log}
}

// Fetch returns one message, long-polling SQS (up to 10 per receive) as needed.
func (s *SQSSource) Fetch(ctx context.Context) (Delivery, error) {
	for len(s.buf) == 0 {
		if err := ctx.Err(); err != nil {
			return Delivery{}, err
		}
		out, err := s.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(s.queueURL),
			MaxNumberOfMessages: sqs.MaxBatch,
			WaitTimeSeconds:     s.wait,
		})
		if err != nil {
			return Delivery{}, fmt.Errorf("sqs receive: %w", err)
		}
		for _, m := range out.Messages {
			click, derr := sqs.Decode(aws.ToString(m.Body))
			s.buf = append(s.buf, Delivery{Click: click, Poison: derr != nil, Token: m})
		}
	}
	d := s.buf[0]
	s.buf = s.buf[1:]
	return d, nil
}

// Commit deletes the processed messages, 10 per call.
func (s *SQSSource) Commit(ctx context.Context, batch []Delivery) error {
	for start := 0; start < len(batch); start += sqs.MaxBatch {
		end := min(start+sqs.MaxBatch, len(batch))
		entries := make([]types.DeleteMessageBatchRequestEntry, 0, end-start)
		for i, d := range batch[start:end] {
			m := d.Token.(types.Message)
			entries = append(entries, types.DeleteMessageBatchRequestEntry{
				Id: aws.String(fmt.Sprint(i)), ReceiptHandle: m.ReceiptHandle,
			})
		}
		out, err := s.api.DeleteMessageBatch(ctx, &awssqs.DeleteMessageBatchInput{QueueUrl: aws.String(s.queueURL), Entries: entries})
		if err != nil {
			return fmt.Errorf("sqs delete: %w", err)
		}
		for _, f := range out.Failed {
			if f.SenderFault {
				// e.g. stale receipt handle: the message was already redelivered.
				// Retrying can never succeed, and the idempotent sink absorbs the duplicate.
				s.log.Warn("sqs delete rejected; message will be redelivered and deduplicated", "code", aws.ToString(f.Code))
				continue
			}
			return fmt.Errorf("sqs delete failed: %s: %s", aws.ToString(f.Code), aws.ToString(f.Message))
		}
	}
	return nil
}

func (s *SQSSource) Close() error { return nil }
