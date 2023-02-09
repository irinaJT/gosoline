package blob

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/hashicorp/go-multierror"
	"github.com/justtrackio/gosoline/pkg/cfg"
	gosoS3 "github.com/justtrackio/gosoline/pkg/cloud/aws/s3"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/metric"
)

const (
	metricName      = "BlobBatchRunner"
	operationRead   = "Read"
	operationWrite  = "Write"
	operationCopy   = "Copy"
	operationDelete = "Delete"
)

type BatchRunnerSettings struct {
	ReaderRunnerCount int `cfg:"reader_runner_count" default:"10"`
	WriterRunnerCount int `cfg:"writer_runner_count" default:"10"`
	CopyRunnerCount   int `cfg:"copy_runner_count" default:"10"`
	DeleteRunnerCount int `cfg:"delete_runner_count" default:"10"`
}

var br = struct {
	sync.Mutex
	instance *batchRunner
}{}

func ProvideBatchRunner() kernel.ModuleFactory {
	br.Lock()
	defer br.Unlock()

	return func(ctx context.Context, config cfg.Config, logger log.Logger) (kernel.Module, error) {
		if br.instance != nil {
			return br.instance, nil
		}

		var err error
		br.instance, err = NewBatchRunner(ctx, config, logger)

		return br.instance, err
	}
}

//go:generate mockery --name BatchRunner
type BatchRunner interface {
	Run(ctx context.Context) error
}

type batchRunner struct {
	kernel.ForegroundModule
	kernel.ServiceStage

	logger   log.Logger
	metric   metric.Writer
	client   gosoS3.Client
	channels *BatchRunnerChannels
	settings *BatchRunnerSettings
}

func NewBatchRunner(ctx context.Context, config cfg.Config, logger log.Logger) (*batchRunner, error) {
	settings := &BatchRunnerSettings{}
	config.UnmarshalKey("blob", settings)

	defaultMetrics := getDefaultRunnerMetrics()
	metricWriter := metric.NewWriter(defaultMetrics...)

	s3Client, err := gosoS3.ProvideClient(ctx, config, logger, "default")
	if err != nil {
		return nil, fmt.Errorf("can not create s3 client default: %w", err)
	}

	runner := &batchRunner{
		logger:   logger,
		metric:   metricWriter,
		client:   s3Client,
		channels: ProvideBatchRunnerChannels(config),
		settings: settings,
	}

	return runner, nil
}

func (r *batchRunner) Run(ctx context.Context) error {
	for i := 0; i < r.settings.ReaderRunnerCount; i++ {
		go r.executeRead(ctx)
	}

	for i := 0; i < r.settings.WriterRunnerCount; i++ {
		go r.executeWrite(ctx)
	}

	for i := 0; i < r.settings.CopyRunnerCount; i++ {
		go r.executeCopy(ctx)
	}

	for i := 0; i < r.settings.DeleteRunnerCount; i++ {
		go r.executeDelete(ctx)
	}

	<-ctx.Done()

	return nil
}

func (r *batchRunner) executeRead(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case object := <-r.channels.read:
			var body io.ReadCloser
			var err error

			key := object.GetFullKey()
			exists := true

			input := &s3.GetObjectInput{
				Bucket: object.bucket,
				Key:    aws.String(key),
			}

			out, err := r.client.GetObject(ctx, input)

			if err != nil {
				if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.StatusCode() == 404 {
					exists = false
					err = nil
				}
			} else {
				body = out.Body
			}

			r.writeMetric(operationRead)

			object.Body = StreamReader(body)
			object.Exists = exists
			object.Error = err
			object.wg.Done()
		}
	}
}

func (r *batchRunner) executeWrite(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case object := <-r.channels.write:
			key := object.GetFullKey()
			body := CloseOnce(object.Body.AsReader())

			input := &s3.PutObjectInput{
				ACL:             object.ACL,
				Body:            body,
				Bucket:          object.bucket,
				Key:             aws.String(key),
				ContentEncoding: object.ContentEncoding,
				ContentType:     object.ContentType,
			}

			_, err := r.client.PutObject(ctx, input)

			if err != nil {
				object.Exists = false
				object.Error = err
			} else {
				object.Exists = true
			}

			if err := body.Close(); err != nil {
				object.Error = multierror.Append(object.Error, err)
			}

			r.writeMetric(operationWrite)

			object.wg.Done()
		}
	}
}

func (r *batchRunner) executeCopy(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case object := <-r.channels.copy:
			key := object.GetFullKey()
			source := object.getSource()

			input := &s3.CopyObjectInput{
				ACL:             object.ACL,
				Bucket:          object.bucket,
				Key:             aws.String(key),
				CopySource:      aws.String(source),
				ContentEncoding: object.ContentEncoding,
				ContentType:     object.ContentType,
			}

			_, err := r.client.CopyObject(ctx, input)
			if err != nil {
				object.Error = err
			}

			r.writeMetric(operationCopy)

			object.wg.Done()
		}
	}
}

func (r *batchRunner) executeDelete(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case object := <-r.channels.delete:
			key := object.GetFullKey()

			input := &s3.DeleteObjectInput{
				Bucket: object.bucket,
				Key:    aws.String(key),
			}

			_, err := r.client.DeleteObject(ctx, input)
			if err != nil {
				object.Error = err
			}

			r.writeMetric(operationDelete)

			object.wg.Done()
		}
	}
}

func (r *batchRunner) writeMetric(operation string) {
	r.metric.WriteOne(&metric.Datum{
		MetricName: metricName,
		Priority:   metric.PriorityHigh,
		Dimensions: map[string]string{
			"Operation": operation,
		},
		Unit:  metric.UnitCount,
		Value: 1.0,
	})
}

func getDefaultRunnerMetrics() []*metric.Datum {
	return []*metric.Datum{
		{
			MetricName: metricName,
			Priority:   metric.PriorityHigh,
			Dimensions: map[string]string{
				"Operation": operationRead,
			},
			Unit:  metric.UnitCount,
			Value: 0.0,
		},
		{
			MetricName: metricName,
			Priority:   metric.PriorityHigh,
			Dimensions: map[string]string{
				"Operation": operationWrite,
			},
			Unit:  metric.UnitCount,
			Value: 0.0,
		},
		{
			MetricName: metricName,
			Priority:   metric.PriorityHigh,
			Dimensions: map[string]string{
				"Operation": operationCopy,
			},
			Unit:  metric.UnitCount,
			Value: 0.0,
		},
		{
			MetricName: metricName,
			Priority:   metric.PriorityHigh,
			Dimensions: map[string]string{
				"Operation": operationDelete,
			},
			Unit:  metric.UnitCount,
			Value: 0.0,
		},
	}
}
