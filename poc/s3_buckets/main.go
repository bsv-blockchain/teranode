package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	if len(os.Args) < 2 {
		exitErrorf(`Usage: %s
[
	list
	create <bucket_name>
	show <bucket_name>
	store <bucket_name> <file_name>
	get <bucket_name> <file_name>
	remove <bucket_name> <file_name>
	delete <bucket_name>
]
			`, os.Args[0])
	}

	// credentials come from the shared credentials file ~/.aws/credentials
	session, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2")},
	)

	if err != nil {
		exitErrorf("Unable to create session, %v", err)
	}

	// Create S3 service client
	svc := s3.New(session)

	switch os.Args[1] {
	case "list":
		listBuckets(svc)

	case "create":
		if len(os.Args) < 3 {
			exitErrorf("Usage: %s create <bucket_name>", os.Args[0])
		}
		bucket := os.Args[2]
		createBucket(svc, bucket)

	case "show":
		if len(os.Args) < 3 {
			exitErrorf("Bucket name required\nUsage: %s show <bucket_name>", os.Args[0])
		}
		bucket := os.Args[2]
		showBucket(svc, bucket)

	case "store":
		if len(os.Args) < 4 {
			exitErrorf("Bucket name and file name required\nUsage: %s store <bucket_name> <file_name>", os.Args[0])
		}
		bucket := os.Args[2]
		filename := os.Args[3]
		storeObject(session, bucket, filename)

	case "get":
		if len(os.Args) < 4 {
			exitErrorf("Bucket name and file name required\nUsage: %s get <bucket_name> <file_name>", os.Args[0])
		}
		bucket := os.Args[2]
		filename := os.Args[3]
		getObject(session, bucket, filename)

	case "remove":
		if len(os.Args) < 4 {
			exitErrorf("Bucket name and file name required\nUsage: %s remove <bucket_name> <file_name>", os.Args[0])
		}
		bucket := os.Args[2]
		filename := os.Args[3]
		removeObject(svc, bucket, filename)

	case "delete":
		if len(os.Args) < 3 {
			exitErrorf("Bucket name required\nUsage: %s delete <bucket_name>", os.Args[0])
		}
		bucket := os.Args[2]
		deleteBucket(svc, bucket)

	default:
		exitErrorf("Unknown command: %s", os.Args[1])
	}
}

func listBuckets(svc *s3.S3) {
	result, err := svc.ListBuckets(nil)
	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	fmt.Println("Buckets:")

	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
}

func createBucket(svc *s3.S3, bucket string) {
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		exitErrorf("Unable to create bucket %q, %v", bucket, err)
	}

	// Wait until bucket is created before finishing
	fmt.Printf("Waiting for bucket %q to be created...\n", bucket)

	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		exitErrorf("Error occurred while waiting for bucket to be created, %v", bucket)
	}

	fmt.Printf("Bucket %q successfully created\n", bucket)
}

func showBucket(svc *s3.S3, bucket string) {
	response, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
	}

	for _, item := range response.Contents {
		fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("")
	}
}

func storeObject(session *session.Session, bucket string, filename string) {
	file, err := os.Open(filename)
	if err != nil {
		exitErrorf("Unable to open file %q, %v", err)
	}

	defer file.Close()

	// Setup the S3 Upload Manager. Also see the SDK doc for the Upload Manager
	// for more information on configuring part size, and concurrency.
	//
	// http://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#NewUploader
	uploader := s3manager.NewUploader(session)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
		Body:   file,
	})
	if err != nil {
		// Print the error and exit.
		exitErrorf("Unable to upload %q to %q, %v", filename, bucket, err)
	}

	fmt.Printf("Successfully uploaded %q to %q\n", filename, bucket)
}

func getObject(session *session.Session, bucket string, filename string) {
	downloader := s3manager.NewDownloader(session)

	file, err := os.Create(filename)
	if err != nil {
		exitErrorf("Unable to create file %q, %v", err)
	}

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(filename),
		})
	if err != nil {
		exitErrorf("Unable to download item %q, %v", filename, err)
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
}

func removeObject(svc *s3.S3, bucket string, filename string) {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(filename)})
	if err != nil {
		exitErrorf("Unable to delete object %q from bucket %q, %v", filename, bucket, err)
	}

	if err := svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	}); err != nil {
		exitErrorf("Error occurred while waiting for object to be deleted, %v", err)
	}

	fmt.Printf("Object %q successfully deleted\n", filename)
}

func deleteBucket(svc *s3.S3, bucket string) {
	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		exitErrorf("Unable to delete bucket %q, %v", bucket, err)
	}

	if err := svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		exitErrorf("Error occurred while waiting for bucket to be deleted, %v", err)
	}

	fmt.Printf("Bucket %q successfully deleted\n", bucket)
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
