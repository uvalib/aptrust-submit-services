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
	log.Printf("INFO: processing submission [%s]", wf.SubmissionId)

	// create the data access object
	dao, err := uvaaptsdao.NewDao(cfg.DbHost, cfg.DbPort, cfg.DbUser, cfg.DbPassword, cfg.DbName)
	if err != nil {
		log.Printf("ERROR: connecting to the database (%s)", err.Error())
		done <- false
		return
	}

	// cleanup on exit
	defer dao.Close()

	// create our s3 helper client
	s3Client, err := newS3Client(nil)
	if err != nil {
		log.Printf("ERROR: creating s3 client (%s)", err.Error())
		done <- false
		return
	}

	// create our event bus client
	eventBus, _ := NewEventBus(cfg.BusName, cfg.BusEventSource)

	// S3 assets in <bucket>/<clientId>/<submissionId>/...
	submissionKeyPrefix := fmt.Sprintf("%s/%s", busEvent.ClientId, wf.SubmissionId)

	// get a complete list of all the files included in the specified submission
	suppliedFiles, err := s3Client.s3List(cfg.InboundBucket, submissionKeyPrefix)
	if err != nil {
		log.Printf("ERROR: listing submission assets (%s)", err.Error())
		done <- false
		return
	}

	// get all the bags and manifests included in the supplied files
	manifestList := findIncludedManifests(submissionKeyPrefix, suppliedFiles)

	// get an enumeration of all the files specified in the manifests
	itemizedFiles := make([]ManifestRow, 0)
	for _, manifest := range manifestList {
		rows, err := manifestContents(s3Client, cfg.InboundBucket, submissionKeyPrefix, manifest)
		if err != nil {
			failureReason := fmt.Sprintf("manifest %s is bad or missing", manifest)
			log.Printf("ERROR: %s", failureReason)
			_ = recordFailure(dao, wf.SubmissionId, failureReason)
			logAndPublishFailure(eventBus, busEvent.ClientId, wf.SubmissionId)
			duration := time.Since(start)
			log.Printf("INFO: worker terminating (elapsed %d ms)", duration.Milliseconds())
			done <- true // this is an acceptable failure so we do not want to reprocess this message
			return
		}
		itemizedFiles = append(itemizedFiles, rows...)
	}

	log.Printf("INFO: %d files located in the submission", len(suppliedFiles))
	log.Printf("INFO: %d files enumerated in %d manifests", len(itemizedFiles), len(manifestList))

	// our enumerated files and the supplied list should be the same size
	if len(itemizedFiles)+len(manifestList) != len(suppliedFiles) {
		failureReason := fmt.Sprintf("manifests do not match submission")
		log.Printf("ERROR: %s", failureReason)
		_ = recordFailure(dao, wf.SubmissionId, failureReason)
		logAndPublishFailure(eventBus, busEvent.ClientId, wf.SubmissionId)
		duration := time.Since(start)
		log.Printf("INFO: worker terminating (elapsed %d ms)", duration.Milliseconds())
		done <- true // this is an acceptable failure so we do not want to reprocess this message
		return
	}

	// for every file in the submission, attempt to match the S3 signature with the one
	// reported in the submitted manifest...
	checksumFailures := 0
	for _, f := range itemizedFiles {
		key := fmt.Sprintf("%s/%s/%s", submissionKeyPrefix, f.bag, f.file)
		log.Printf("DEBUG: validating submission file [%s]...", key)

		res, err := s3Client.s3Head(cfg.InboundBucket, key)
		if err != nil {
			log.Printf("ERROR: getting attributes for [%s] (%s)", key, err.Error())
			done <- false
			return
		}

		// trim leading and trailing quote characters
		reportedHash := strings.Trim(*res.ETag, "\"")

		// the ETag for smaller files is the md5 fingerprint, for a multipart upload it is
		// different so we cannot be sure of a failure so just ignore it
		if reportedHash != f.hash {
			if strings.Contains(reportedHash, "-") == true {
				log.Printf("INFO: checksum difference for [%s]", key)
				log.Printf("INFO: expected [%s], reported [%s] (looks like a multipart, ignoring)", f.hash, reportedHash)
			} else {
				failureReason := fmt.Sprintf("checksum failure for [%s]; expected [%s], reported [%s]", key, f.hash, reportedHash)
				log.Printf("ERROR: %s", failureReason)
				_ = recordFailure(dao, wf.SubmissionId, failureReason)
				checksumFailures++
			}
		}
	}

	// no checksum failures, lets build the database
	if checksumFailures == 0 {

		// create the bags
		err = createDBBags(dao, manifestList, wf.SubmissionId)
		if err != nil {
			done <- false
			return
		}

		// create the files
		err = createDBFiles(dao, itemizedFiles, wf.SubmissionId)
		if err != nil {
			done <- false
			return
		}

		// we are done, publish the appropriate event and terminate
		log.Printf("INFO: no problems found for submission [%s]", wf.SubmissionId)
		_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionReconcile, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
	} else {
		logAndPublishFailure(eventBus, busEvent.ClientId, wf.SubmissionId)
	}

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %d ms)", duration.Milliseconds())
	done <- true
}

func logAndPublishFailure(eventBus uvaaptsbus.UvaBus, cid string, sid string) {
	log.Printf("WARNING: submission [%s] FAILS validation", sid)
	_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionValidateFail, cid, sid, "", "")
}

//
// end of file
//
