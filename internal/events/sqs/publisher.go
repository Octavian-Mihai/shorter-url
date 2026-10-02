// Package sqs implements events.Publisher on Amazon SQS, so the cloud
// deployment can swap Kafka for a managed queue without touching the API.
package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Octavian-Mihai/shorter-url/internal/events"
)

// MaxBatch is SQS's hard limit for SendMessageBatch / DeleteMessageBatch.
const MaxBatch = 10

// API is the subset of *sqs.Client we use (and fake in tests).
type API interface {
	SendMessageBatch(ctx context.Context, in *awssqs.SendMessageBatchInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error)
	ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, opts ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, in *awssqs.DeleteMessageBatchInput, opts ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error)
}

// NewClient builds an SQS client from the default AWS credential chain.
// endpoint is optional and points at a local emulator (ElasticMQ / LocalStack).
func NewClient(ctx context.Context, region, endpoint string) (*awssqs.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("sqs: load aws config: %w", err)
	}
	return awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}

type Publisher struct {
	api      API
	queueURL string
}

var _ events.Publisher = (*Publisher)(nil)

func NewPublisher(api API, queueURL string) *Publisher {
	return &Publisher{api: api, queueURL: queueURL}
}

// Publish sends clicks in chunks of 10. Entries SQS rejects are retried once;
// anything still failing makes Publish return an error (counted by events.Async).
// No ordering is needed: the consumer's sink is idempotent and order-insensitive,
// so a cheap standard queue is sufficient (no FIFO).
func (p *Publisher) Publish(ctx context.Context, batch []events.Click) error {
	for start := 0; start < len(batch); start += MaxBatch {
		end := min(start+MaxBatch, len(batch))
		if err := p.sendChunk(ctx, batch[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) sendChunk(ctx context.Context, chunk []events.Click) error {
	entries := make([]types.SendMessageBatchRequestEntry, len(chunk))
	for i, c := range chunk {
		body, err := json.Marshal(c)
		if err != nil {
			return fmt.Errorf("sqs: encode click: %w", err)
		}
		entries[i] = types.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), MessageBody: aws.String(string(body))}
	}
	for attempt := 0; attempt < 2 && len(entries) > 0; attempt++ {
		out, err := p.api.SendMessageBatch(ctx, &awssqs.SendMessageBatchInput{QueueUrl: aws.String(p.queueURL), Entries: entries})
		if err != nil {
			return fmt.Errorf("sqs send: %w", err)
		}
		if len(out.Failed) == 0 {
			return nil
		}
		failed := map[string]bool{}
		for _, f := range out.Failed {
			failed[aws.ToString(f.Id)] = true
		}
		var retry []types.SendMessageBatchRequestEntry
		for _, e := range entries {
			if failed[aws.ToString(e.Id)] {
				retry = append(retry, e)
			}
		}
		entries = retry
	}
	if len(entries) > 0 {
		return fmt.Errorf("sqs send: %d of %d messages rejected after retry", len(entries), len(chunk))
	}
	return nil
}

// Close is a no-op: the AWS client holds no resources that need flushing.
func (p *Publisher) Close() error { return nil }

// Decode parses a message body back into a click.
func Decode(body string) (events.Click, error) {
	var c events.Click
	if err := json.Unmarshal([]byte(body), &c); err != nil {
		return c, fmt.Errorf("decode click: %w", err)
	}
	return c, nil
}
