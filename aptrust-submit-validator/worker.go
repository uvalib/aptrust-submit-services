package main

import (
	"fmt"
	"log"
	"strings"
	"time"

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

	log.Printf("INFO: event %s/%s", busEvent.String(), wf.String())

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
	s3Client, err := newS3Client(nil)
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

		res, err := s3Client.s3Head(cfg.InboundBucket, key)
		if err != nil {
			log.Printf("ERROR: getting attributes for [%s] (%s)", key, err.Error())
			done <- false
			return
		}

		// trim leading and trailing quote characters
		str := strings.Trim(*res.ETag, "\"")

		// the ETag for smaller files is the md5 fingerprint, for a multipart upload it is
		// different so we cannot be sure of a failure so just ignore it
		if validateChecksum(str, f.Hash) == false {
			if strings.Contains(str, "-") == true {
				log.Printf("INFO: checksum difference for [%s]", key)
				log.Printf("INFO: expected [%s], reported [%s] (looks like a multipart, ignoring)", f.Hash, str)
			} else {
				log.Printf("ERROR: checksum failure for [%s]", key)
				log.Printf("ERROR: expected [%s], reported [%s]", f.Hash, str)
				checksumFailures++
			}
		}
	}

	// we are done, publish the appropriate event and terminate
	if checksumFailures > 0 {
		_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionValidateFail, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
	} else {
		_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionReconcile, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
	}

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %0.2f seconds)", duration.Seconds())
	done <- true
}

//
// end of file
//
