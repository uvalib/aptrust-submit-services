package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/uvalib/aptrust-submit-bus-definitions/uvaaptsbus"
	//"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func worker(done chan<- bool, cfg *ServiceConfig, busEvent *uvaaptsbus.UvaBusEvent) {

	start := time.Now()
	log.Printf("INFO: worker starting")

	// ensure this is the type of event we want to process
	switch busEvent.EventName {
	case uvaaptsbus.EventBagBuilt:
	default:
		log.Printf("ERROR: unexpected event type (%s), ignoring", busEvent.EventName)
		done <- true
		return
	}

	// make the workflow event
	wf, err := uvaaptsbus.MakeWorkflowEvent(busEvent.Detail)
	if err != nil {
		log.Printf("ERROR: unmarshaling workflow event (%s)", err.Error())
		done <- false
		return
	}

	log.Printf("INFO: event %s/%s", busEvent.String(), wf.String())

	// create our event bus client
	eventBus, _ := NewEventBus(cfg.BusName, cfg.BusEventSource)

	// assets in <cache root>/<clientId>/<submissionId>/...
	bagFile := fmt.Sprintf("%s.tar", wf.BagId)
	assetName := fmt.Sprintf("%s/%s/%s/%s", cfg.LocalAssetCache, busEvent.ClientId, wf.SubmissionId, bagFile)

	// ensure it exists
	if _, err := os.Stat(assetName); errors.Is(err, os.ErrNotExist) {
		log.Printf("ERROR: bag file [%s] does not exist (%s)", assetName, err.Error())
		done <- false
		return
	}

	// create custom credentials
	creds := credentials.NewStaticCredentialsProvider(cfg.APTAccessKey, cfg.APTSecretKey, "")

	// create our s3 helper client
	s3Client, err := newS3Client(&creds)
	if err != nil {
		log.Printf("ERROR: creating s3 client (%s)", err.Error())
		done <- false
		return
	}

	log.Printf("INFO: submitting [%s]...", bagFile)

	// upload to the APTrust deposit bucket
	err = s3Client.s3Put(cfg.APTBucket, bagFile, assetName)
	if err != nil {
		log.Printf("ERROR: uploading bag file [%s] (%s)", assetName, err.Error())
		done <- false
		return
	}

	// get the object ETAG so we can refer to it later...
	res, err := s3Client.s3Head(cfg.APTBucket, bagFile)
	if err != nil {
		log.Printf("ERROR: getting attributes for [%s] (%s)", bagFile, err.Error())
		done <- false
		return
	}

	// remove the leading and trailing quote
	etag := strings.Trim(*res.ETag, "\"")

	log.Printf("INFO: etag for [%s] => (%s)", bagFile, etag)

	// we are done, publish the appropriate event and terminate
	extra := BagSubmittedEventExtraPayload{ETag: etag}
	e, _ := json.Marshal(extra)
	_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventBagSubmitted, busEvent.ClientId, wf.SubmissionId, wf.BagId, string(e))

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %0.2f seconds)", duration.Seconds())
	done <- true
}

//
// end of file
//
