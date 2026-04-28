package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Client struct {
	client *s3.Client
	bucket string
}

// NewS3Client creates a new S3 client with custom endpoint (can be IP address)
func NewS3Client(endpoint, accessKey, secretKey, region, bucket string) (*S3Client, error) {
	// Custom resolver for endpoint (supports IP addresses)
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               endpoint,
			SigningRegion:     region,
			HostnameImmutable: true, // Important for IP addresses
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true // Required for custom endpoints/IPs
	})

	return &S3Client{
		client: client,
		bucket: bucket,
	}, nil
}

// PutObject uploads an object to S3
func (s *S3Client) PutObject(ctx context.Context, key string, data []byte, contentType string) error {
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	}

	_, err := s.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to put object %s: %w", key, err)
	}

	fmt.Printf("Successfully uploaded %s to bucket %s\n", key, s.bucket)
	return nil
}

// PutObjectFromFile uploads a file to S3
func (s *S3Client) PutObjectFromFile(ctx context.Context, key, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          file,
		ContentLength: aws.Int64(fileInfo.Size()),
	}

	_, err = s.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload file %s: %w", filePath, err)
	}

	fmt.Printf("Successfully uploaded file %s as %s to bucket %s\n", filePath, key, s.bucket)
	return nil
}

// GetObject downloads an object from S3
func (s *S3Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	result, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object data: %w", err)
	}

	fmt.Printf("Successfully downloaded %s from bucket %s (%d bytes)\n", key, s.bucket, len(data))
	return data, nil
}

// GetObjectToFile downloads an object from S3 to a file
func (s *S3Client) GetObjectToFile(ctx context.Context, key, filePath string) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	result, err := s.client.GetObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to get object %s: %w", key, err)
	}
	defer result.Body.Close()

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer file.Close()

	_, err = io.Copy(file, result.Body)
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	fmt.Printf("Successfully downloaded %s to file %s\n", key, filePath)
	return nil
}

// DeleteObject deletes an object from S3
func (s *S3Client) DeleteObject(ctx context.Context, key string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	_, err := s.client.DeleteObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete object %s: %w", key, err)
	}

	fmt.Printf("Successfully deleted %s from bucket %s\n", key, s.bucket)
	return nil
}

// DeleteObjects deletes multiple objects from S3
func (s *S3Client) DeleteObjects(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	var objects []types.ObjectIdentifier
	for _, key := range keys {
		objects = append(objects, types.ObjectIdentifier{
			Key: aws.String(key),
		})
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &types.Delete{
			Objects: objects,
		},
	}

	result, err := s.client.DeleteObjects(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete objects: %w", err)
	}

	fmt.Printf("Successfully deleted %d objects from bucket %s\n", len(result.Deleted), s.bucket)
	return nil
}

// ListObjects lists objects in the bucket
func (s *S3Client) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	}

	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	result, err := s.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	var keys []string
	for _, obj := range result.Contents {
		keys = append(keys, *obj.Key)
	}

	fmt.Printf("Found %d objects in bucket %s\n", len(keys), s.bucket)
	return keys, nil
}

// ObjectExists checks if an object exists in the bucket
// func (s *S3Client) ObjectExists(ctx context.Context, key string) (bool, error) {
// 	input := &s3.HeadObjectInput{
// 		Bucket: aws.String(s.bucket),
// 		Key:    aws.String(key),
// 	}

// 	_, err := s.client.HeadObject(ctx, input)
// 	if err != nil {
// 		var notFound *types.NotFound
// 		if aws.ErrorAs(err, &notFound) {
// 			return false, nil
// 		}
// 		return false, fmt.Errorf("failed to check object existence: %w", err)
// 	}

//		return true, nil
//	}
func filesEqual(path1, path2 string) (bool, error) {
	f1, err := os.Open(path1)
	if err != nil {
		return false, err
	}
	defer f1.Close()

	f2, err := os.Open(path2)
	if err != nil {
		return false, err
	}
	defer f2.Close()

	const bufSize = 4 * 1024 * 1024 // 4 MB
	b1 := make([]byte, bufSize)
	b2 := make([]byte, bufSize)

	for {
		n1, err1 := f1.Read(b1)
		n2, err2 := f2.Read(b2)

		if n1 != n2 || !bytes.Equal(b1[:n1], b2[:n2]) {
			return false, nil
		}

		if err1 == io.EOF && err2 == io.EOF {
			break
		}
		if err1 != nil && err1 != io.EOF {
			return false, err1
		}
		if err2 != nil && err2 != io.EOF {
			return false, err2
		}
	}
	return true, nil
}

func generateFile() {
	fmt.Printf("Generating %d MB random file: %s\n", fileSizeMB, filePath)
	f, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 4*1024*1024) // 4 MB buffer
	remaining := size
	for remaining > 0 {
		n := len(buf)
		if remaining < n {
			n = remaining
		}
		if _, err := rand.Read(buf[:n]); err != nil {
			log.Fatalf("failed to generate random data: %v", err)
		}
		if _, err := f.Write(buf[:n]); err != nil {
			log.Fatalf("failed to write random data: %v", err)
		}
		remaining -= n
	}
	fmt.Println("File generation complete.")
}

var (
	generate *bool
	put      *bool
	get      *bool
	delete   *bool
)

const (
	filePath   = "/mywork/pipeline-all-in-one-v1.6.tar" // local file to upload
	downloaded = "downloaded.tar"
	objectKey  = "pipeline-all-in-one-v1.6.tar"
	fileSizeMB = 512
)
const size = fileSizeMB * 1024 * 1024

func main() {
	// Example usage
	generate = flag.Bool("gen", false, "Generate random data for upload")
	put = flag.Bool("p", false, "do put")
	get = flag.Bool("g", false, "do get")
	delete = flag.Bool("d", false, "do delete")
	flag.Parse()

	fmt.Println("Generation: ", *generate)

	ctx := context.Background()

	client, err := NewS3Client(
		"http://127.0.0.1:8000",
		"minioadmin",
		"minioadmin",
		"us-east-1",
		"test",
	)
	if err != nil {
		log.Fatal(err)
	}

	// Example operations
	demoOperations(ctx, client)
}

func demoOperations(ctx context.Context, client *S3Client) {
	if *generate {
		generateFile()
	}

	if *put {
		err := client.PutObjectFromFile(ctx, objectKey, filePath)
		if err != nil {
			log.Printf("PUT error: %v", err)
			return
		}
	}

	time.Sleep(1 * time.Second)

	if *get {
		fmt.Println("Downloading file from S3...")
		err := client.GetObjectToFile(ctx, objectKey, downloaded)
		if err != nil {
			log.Printf("GET error: %v", err)
			return
		}
		fmt.Println("Verifying file integrity...")
		match, err := filesEqual(filePath, downloaded)
		if err != nil {
			log.Printf("compare error: %v", err)
		} else if match {
			fmt.Println("Files matches.")
		} else {
			fmt.Println("Files does NOT match.")
		}
	}

	if *delete {
		err := client.DeleteObject(ctx, objectKey)
		if err != nil {
			log.Printf("DELETE error: %v", err)
		}
	}
}
