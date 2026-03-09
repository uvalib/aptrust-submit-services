package main

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/uvalib/aptrust-submit-bus-definitions/uvaaptsbus"
	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func worker(done chan<- bool, cfg *ServiceConfig, busEvent *uvaaptsbus.UvaBusEvent) {

	start := time.Now()
	log.Printf("INFO: worker starting")

	// ensure this is the type of event we want to process
	switch busEvent.EventName {
	case uvaaptsbus.EventSubmissionValidate:
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

	log.Printf("INFO: EVENT %s / %s", busEvent.String(), wf.String())

	// create our event bus client
	eventBus, _ := NewEventBus(cfg.BusName, cfg.BusEventSource)

	// create the data access object
	dao, err := uvaaptsdao.NewDao(cfg.DbHost, cfg.DbPort, cfg.DbUser, cfg.DbPassword, cfg.DbName)
	if err != nil {
		log.Printf("ERROR: connecting to the database (%s)", err.Error())
		done <- false
		return
	}

	// cleanup on exit
	defer dao.Close()

	// get all the files for this submission
	files, err := dao.GetFilesBySubmission(wf.SubmissionId)
	if err != nil {
		log.Printf("ERROR: getting submission file set (%s)", err.Error())
		done <- false
		return
	}

	// create our s3 helper client
	s3Client, err := newS3Client()
	if err != nil {
		log.Printf("ERROR: creating s3 client (%s)", err.Error())
		done <- false
		return
	}

	// S3 assets in <bucket>/<clientId>/<submissionId>/...
	submissionKeyPrefix := fmt.Sprintf("%s/%s", busEvent.ClientId, wf.SubmissionId)

	// for every file in the submission, attempt to match the S3 signature with the one
	// reported in the submitted manifest...
	checksumFailures := 0
	for _, f := range files {
		key := fmt.Sprintf("%s/%s/%s", submissionKeyPrefix, f.BagName, f.Name)
		log.Printf("DEBUG: validating submission file [%s]...", key)

		reqAttr := []types.ObjectAttributes{types.ObjectAttributesChecksum}
		resAttr, err := s3Client.s3GetAttributes(cfg.InboundBucket, key, reqAttr)
		if err != nil {
			log.Printf("ERROR: getting attributes for [%s] (%s)", key, err.Error())
			done <- false
			return
		}

		// did we get some checksum values
		if resAttr.Checksum != nil && len(*resAttr.Checksum.ChecksumSHA256) != 0 {
			if validateChecksum(*resAttr.Checksum.ChecksumSHA256, f.Hash) == false {
				checksumFailures++
				log.Printf("ERROR: checksum failure for [%s]", key)
			}
		} else {
			log.Printf("WARNING: no SHA256 checksum [%s], no validation done", key)
		}
	}

	// we are done, publish the appropriate event and terminate
	if checksumFailures > 0 {
		_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionValidateFail, busEvent.ClientId, wf.SubmissionId, wf.BagId)
	} else {
		_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionReconcile, busEvent.ClientId, wf.SubmissionId, wf.BagId)
	}

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %0.2f seconds)\n", duration.Seconds())
	done <- true
}

//
// end of file
//
