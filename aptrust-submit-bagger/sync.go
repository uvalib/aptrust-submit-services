package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/seqsense/s3sync/v2"
)

func syncAssets(bucket string, path string, local string, workers int32) error {

	// ensure the local path exists
	err := os.MkdirAll(local, 0755)
	if err != nil {
		log.Printf("ERROR: creating asset path [%s] (%s)", local, err.Error())
		return err
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return err
	}

	// new sync manager
	syncManager := s3sync.New(cfg, s3sync.WithParallel(int(workers)))

	// our source location
	source := fmt.Sprintf("s3://%s/%s", bucket, path)
	log.Printf("INFO: sync from [%s] -> [%s]", source, local)

	start := time.Now()
	err = syncManager.Sync(context.TODO(), source, local)
	if err != nil {
		log.Printf("ERROR: sync error (%s)", err.Error())
		return err
	}

	stats := syncManager.GetStatistics()
	duration := time.Since(start)
	log.Printf("INFO: sync completed (elapsed %0.2f seconds)", duration.Seconds())
	log.Printf("INFO: %d bytes written, %d files downloaded, %d files deleted", stats.Bytes, stats.Files, stats.DeletedFiles)

	return nil
}

//
// end of file
//
