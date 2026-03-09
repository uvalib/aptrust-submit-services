//
// simple module to get and set parameter values in the ssm
//

package main

import (
	//"bytes"
	"context"
	"fmt"

	//"fmt"
	"log"
	//"strings"
	"time"

	//"fmt"
	"os"
	//"time"

	//"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type uvaS3Client struct {
	client     *s3.Client
	downloader *manager.Downloader
	uploader   *manager.Uploader
}

func newS3Client() (*uvaS3Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	c := uvaS3Client{}
	c.client = s3.NewFromConfig(cfg)
	c.downloader = manager.NewDownloader(c.client)
	c.uploader = manager.NewUploader(c.client)
	return &c, nil
}

func (c *uvaS3Client) s3List(bucket string, key string) ([]string, error) {

	log.Printf("INFO: s3 list [%s/%s]", bucket, key)
	start := time.Now()

	// query parameters
	params := &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &key,
	}

	// create a paginator
	var limit int32 = 1000
	paginate := s3.NewListObjectsV2Paginator(c.client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = limit
	})

	// make the result set
	result := make([]string, 0)

	// iterate through the pages
	for paginate.HasMorePages() {

		// get the next page
		page, err := paginate.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}

		for _, o := range page.Contents {
			//log.Printf("DEBUG: found [%s]", *o.Key)
			result = append(result, *o.Key)
		}
	}

	duration := time.Since(start)
	log.Printf("INFO: s3 list [%s/%s] complete in %0.2f seconds", bucket, key, duration.Seconds())
	return result, nil
}

func (c *uvaS3Client) s3Exists(bucket string, key string) bool {

	log.Printf("head [%s/%s]", bucket, key)
	start := time.Now()

	_, err := c.client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})

	duration := time.Since(start)
	log.Printf("head [%s/%s] complete in %0.2f seconds (%s)", bucket, key, duration.Seconds(), c.statusText(err))
	return err == nil
}

func (c *uvaS3Client) s3GetAttributes(bucket string, key string, attribs []types.ObjectAttributes) (s3.GetObjectAttributesOutput, error) {

	log.Printf("get attribs [%s/%s]", bucket, key)
	start := time.Now()

	res, err := c.client.GetObjectAttributes(context.TODO(), &s3.GetObjectAttributesInput{
		Bucket:           &bucket,
		Key:              &key,
		ObjectAttributes: attribs,
	})

	duration := time.Since(start)
	log.Printf("get attribs [%s/%s] complete in %0.2f seconds (%s)", bucket, key, duration.Seconds(), c.statusText(err))
	return *res, nil
}

func (c *uvaS3Client) s3Put(bucket string, key string, location string) error {

	// validate inbound parameters
	//if impl.validateS3Obj(obj) == false || len(location) == 0 {
	//	return ErrBadParameter
	//}

	//source := fmt.Sprintf("s3://%s/%s", obj.BucketName(), obj.KeyName())

	//impl.logInfo(fmt.Sprintf("put from %s to %s", location, source))

	// open the file
	file, err := os.Open(location)
	if err != nil {
		// assume the error is file not found... probably reasonable
		return os.ErrNotExist
	}
	defer file.Close()

	// get the filesize
	//s, err := file.Stat()
	//if err != nil {
	//	return err
	//}
	//fileSize := s.Size()

	// Upload the file to S3.
	//start := time.Now()
	_, err = c.uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   file,
	})
	if err != nil {
		return err
	}

	//duration := time.Since(start)
	//impl.logInfo(fmt.Sprintf("put %s complete in %0.2f seconds (%d bytes, %0.2f bytes/sec)", source, duration.Seconds(), fileSize, float64(fileSize)/duration.Seconds()))
	return nil
}

func (c *uvaS3Client) s3Get(bucket string, key string, location string) error {

	source := fmt.Sprintf("s3://%s/%s", bucket, key)
	log.Printf("INFO: getting %s to %s", source, location)

	file, err := os.OpenFile(location, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	start := time.Now()
	fileSize, err := c.downloader.Download(context.TODO(), file, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})

	//	if err != nil {
	//		return err
	//	}

	//	// I think there are times when the download runs out of space but it is not reported as an error so
	//	// we validate the expected file size against the actually downloaded size
	//if obj.Size() != -1 && obj.Size() != fileSize {

	// remove the file
	//	_ = os.Remove(location)
	//	return fmt.Errorf("download failure. expected %d bytes, received %d bytes", obj.Size(), fileSize)
	//}

	duration := time.Since(start)
	log.Printf("INFO: get of %s complete in %0.2f seconds (%d bytes, %0.2f bytes/sec) (%s)", source, duration.Seconds(), fileSize, float64(fileSize)/duration.Seconds(), c.statusText(err))
	return nil
}

func (s *uvaS3Client) statusText(err error) string {
	if err == nil {
		return "ok"
	}
	return "ERR"
}

//
// end of file
//
