package s3proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type s3Cache struct {
	mcore                      *minio.Core
	dynamo                     *dynamodb.DynamoDB
	dynamoTable                string
	dynamoInlineThresholdBytes int64
	prefix                     string
	bucket                     string
	uploadQueue                chan<- backendproxy.UploadReq
	accessLogger               cache.Logger
	errorLogger                cache.Logger
	v2mode                     bool
	updateTimestamps           bool
	objectKey                  func(hash string, kind cache.EntryKind) string
}

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_s3_cache_hits",
		Help: "The total number of s3 backend cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_s3_cache_misses",
		Help: "The total number of s3 backend cache misses",
	})
	histDynamoFetchLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bazel_remote_s3_dynamo_fetch_latency_ms",
		Help:    "The latency of fetches from DynamoDB",
		Buckets: []float64{0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000},
	})
)

// Used in place of minio's verbose "NoSuchKey" error.
var errNotFound = errors.New("NOT FOUND")

// New returns a new instance of the S3-API based cache
func New(
	// S3CloudStorageConfig struct fields:
	Endpoint string,
	Bucket string,
	BucketLookupType minio.BucketLookupType,
	Prefix string,
	Credentials *credentials.Credentials,
	DisableSSL bool,
	UpdateTimestamps bool,
	Region string,
	MaxIdleConns int,
	DynamoTable string,

	storageMode string, accessLogger cache.Logger,
	errorLogger cache.Logger, numUploaders, maxQueuedUploads int) cache.Proxy {

	fmt.Println("Using S3 backend.")

	var minioCore *minio.Core
	var err error

	if Credentials == nil {
		log.Fatalf("Failed to determine s3proxy credentials")
	}

	secure := !DisableSSL
	tr, err := minio.DefaultTransport(secure)
	if err != nil {
		log.Fatalf("Failed to create default minio transport: %v", err)
	}

	tr.MaxIdleConns = MaxIdleConns
	tr.MaxIdleConnsPerHost = MaxIdleConns

	// Initialize minio client with credentials
	opts := &minio.Options{
		Creds:        Credentials,
		BucketLookup: BucketLookupType,

		Region:    Region,
		Secure:    secure,
		Transport: tr,
	}
	minioCore, err = minio.NewCore(Endpoint, opts)
	if err != nil {
		log.Fatalln(err)
	}

	if storageMode != "zstd" && storageMode != "uncompressed" {
		log.Fatalf("Unsupported storage mode for the s3proxy backend: %q, must be one of \"zstd\" or \"uncompressed\"",
			storageMode)
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := dynamodb.New(sess)

	c := &s3Cache{
		mcore:       minioCore,
		dynamo:      svc,
		dynamoTable: DynamoTable,
		// dynamo objects cannot be larger than 400KB. conservatively limit to 300KB.
		dynamoInlineThresholdBytes: 300 * 1024,
		prefix:                     Prefix,
		bucket:                     Bucket,
		accessLogger:               accessLogger,
		errorLogger:                errorLogger,
		v2mode:                     storageMode == "zstd",
		updateTimestamps:           UpdateTimestamps,
	}

	if c.v2mode {
		c.objectKey = func(hash string, kind cache.EntryKind) string {
			return objectKeyV2(c.prefix, hash, kind)
		}
	} else {
		c.objectKey = func(hash string, kind cache.EntryKind) string {
			return objectKeyV1(c.prefix, hash, kind)
		}
	}

	c.uploadQueue = backendproxy.StartUploaders(c, numUploaders, maxQueuedUploads)

	return c
}

func objectKeyV2(prefix string, hash string, kind cache.EntryKind) string {
	var baseKey string
	if kind == cache.CAS {
		// Use "cas.v2" to distinguish new from old format blobs.
		baseKey = path.Join("cas.v2", hash[:2], hash)
	} else {
		baseKey = path.Join(kind.String(), hash[:2], hash)
	}

	if prefix == "" {
		return baseKey
	}

	return path.Join(prefix, baseKey)
}

func objectKeyV1(prefix string, hash string, kind cache.EntryKind) string {
	if prefix == "" {
		return path.Join(kind.String(), hash[:2], hash)
	}

	return path.Join(prefix, kind.String(), hash[:2], hash)
}

// Helper function for logging responses
func logResponse(log cache.Logger, method, bucket, key string, err error) {
	status := "OK"
	if err != nil {
		status = err.Error()
	}

	log.Printf("S3 %s %s %s %s", method, bucket, key, status)
}

// Helper function for logging responses
func logDynamoResponse(log cache.Logger, method, bucket, key string, err error) {
	status := "OK"
	if err != nil {
		status = err.Error()
	}

	log.Printf("DYNAMO %s %s %s %s", method, bucket, key, status)
}

type dynamoItem struct {
	ObjectKey   string
	Data        []byte
	S3Link      string
	LogicalSize int64
	SizeOnDisk  int64
	Kind        cache.EntryKind
	Hash        string
}

func (c *s3Cache) UploadFile(item backendproxy.UploadReq) {
	var body []byte
	s3Link := ""
	if item.LogicalSize < c.dynamoInlineThresholdBytes {
		body = make([]byte, item.LogicalSize)
		_, err := io.ReadFull(item.Rc, body)
		if err != nil {
			c.errorLogger.Printf("failed to read inline data: %v", err)
			_ = item.Rc.Close()
			return
		}
	} else {
		// empty body, data will be stored in s3
		body = nil
		s3Link = c.objectKey(item.Hash, item.Kind)
	}
	dbItem := dynamoItem{
		ObjectKey:   c.objectKey(item.Hash, item.Kind),
		Data:        body,
		S3Link:      s3Link,
		LogicalSize: item.LogicalSize,
		SizeOnDisk:  item.SizeOnDisk,
		Kind:        item.Kind,
		Hash:        item.Hash,
	}

	av, err := dynamodbattribute.MarshalMap(dbItem)
	if err != nil {
		c.errorLogger.Printf("failed to marshal dynamo item: %v", err)
		_ = item.Rc.Close()
		return
	}

	_, err = c.dynamo.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(c.dynamoTable),
		Item:      av,
	})
	if err != nil {
		c.errorLogger.Printf("failed to put dynamo item: %v", err)
		_ = item.Rc.Close()
		return
	}
	logDynamoResponse(c.accessLogger, "UPLOAD", c.bucket, c.objectKey(item.Hash, item.Kind), err)

	if s3Link != "" {
		_, err := c.mcore.PutObject(
			context.Background(),
			c.bucket,                          // bucketName
			c.objectKey(item.Hash, item.Kind), // objectName
			item.Rc,                           // reader
			item.SizeOnDisk,                   // objectSize
			"",                                // md5base64
			"",                                // sha256
			minio.PutObjectOptions{
				UserMetadata: map[string]string{
					"Content-Type": "application/octet-stream",
				},
			}, // metadata
		)
		logResponse(c.accessLogger, "UPLOAD", c.bucket, c.objectKey(item.Hash, item.Kind), err)
		if err != nil {
			c.errorLogger.Printf("failed to put s3 object: %v", err)
			_ = item.Rc.Close()
			return
		}
	}

	_ = item.Rc.Close()
}

func (c *s3Cache) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	if c.uploadQueue == nil {
		_ = rc.Close()
		return
	}

	select {
	case c.uploadQueue <- backendproxy.UploadReq{
		Hash:        hash,
		LogicalSize: logicalSize,
		SizeOnDisk:  sizeOnDisk,
		Kind:        kind,
		Rc:          rc,
	}:
	default:
		c.errorLogger.Printf("too many uploads queued\n")
		_ = rc.Close()
	}
}

func (c *s3Cache) UpdateModificationTimestamp(ctx context.Context, bucket string, object string) {
	src := minio.CopySrcOptions{
		Bucket: bucket,
		Object: object,
	}

	dst := minio.CopyDestOptions{
		Bucket:          bucket,
		Object:          object,
		ReplaceMetadata: true,
	}

	_, err := c.mcore.ComposeObject(context.Background(), dst, src)

	logResponse(c.accessLogger, "COMPOSE", bucket, object, err)
}

func (c *s3Cache) Get(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (io.ReadCloser, int64, error) {
	key := c.objectKey(hash, kind)
	fetchStartTime := time.Now()
	dbItem, err := c.dynamo.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(c.dynamoTable),
		Key: map[string]*dynamodb.AttributeValue{
			"ObjectKey": {S: aws.String(key)},
		},
	})
	histDynamoFetchLatency.Observe(float64(time.Since(fetchStartTime).Milliseconds()))

	if err != nil {
		cacheMisses.Inc()
		logDynamoResponse(c.accessLogger, "DOWNLOAD1", c.dynamoTable, key, err)
		return nil, -1, err
	}
	if dbItem.Item == nil {
		cacheMisses.Inc()
		logDynamoResponse(c.accessLogger, "DOWNLOAD2", c.dynamoTable, key, errNotFound)
		return nil, -1, err
	}

	item := dynamoItem{}
	err = dynamodbattribute.UnmarshalMap(dbItem.Item, &item)
	if err != nil {
		cacheMisses.Inc()
		logDynamoResponse(c.accessLogger, "DOWNLOAD3", c.bucket, c.objectKey(hash, kind), err)
		return nil, -1, err
	}
	if item.Data != nil {
		return io.NopCloser(bytes.NewReader(item.Data)), item.LogicalSize, nil
	}

	rc, info, _, err := c.mcore.GetObject(
		ctx,
		c.bucket,                 // bucketName
		item.S3Link,              // objectName
		minio.GetObjectOptions{}, // opts
	)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			cacheMisses.Inc()
			logResponse(c.accessLogger, "DOWNLOAD", c.bucket, c.objectKey(hash, kind), errNotFound)
			return nil, -1, nil
		}
		cacheMisses.Inc()
		logResponse(c.accessLogger, "DOWNLOAD", c.bucket, c.objectKey(hash, kind), err)
		return nil, -1, err
	}
	cacheHits.Inc()

	if c.updateTimestamps {
		c.UpdateModificationTimestamp(ctx, c.bucket, c.objectKey(hash, kind))
	}

	logResponse(c.accessLogger, "DOWNLOAD", c.bucket, c.objectKey(hash, kind), nil)

	if kind == cache.CAS && c.v2mode {
		return casblob.ExtractLogicalSize(rc)
	}

	return rc, info.Size, nil
}

func (c *s3Cache) Contains(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (bool, int64) {
	size := int64(-1)
	exists := false

	fetchStartTime := time.Now()
	dbItem, err := c.dynamo.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(c.dynamoTable),
		Key: map[string]*dynamodb.AttributeValue{
			"ObjectKey": {S: aws.String(c.objectKey(hash, kind))},
		},
	})
	histDynamoFetchLatency.Observe(float64(time.Since(fetchStartTime).Milliseconds()))

	exists = (err == nil && dbItem.Item != nil)
	if err != nil {
		err = errNotFound
	} else if kind != cache.CAS || !c.v2mode {
		item := dynamoItem{}
		err = dynamodbattribute.UnmarshalMap(dbItem.Item, &item)
		if err != nil {
			exists = false
		} else {
			size = item.LogicalSize
		}
	}

	logDynamoResponse(c.accessLogger, "CONTAINS", c.bucket, c.objectKey(hash, kind), err)

	return exists, size
}
