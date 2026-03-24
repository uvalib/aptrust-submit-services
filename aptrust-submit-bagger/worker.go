package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/uvalib/aptrust-submit-bus-definitions/uvaaptsbus"
	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
	//"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func worker(done chan<- bool, cfg *ServiceConfig, busEvent *uvaaptsbus.UvaBusEvent) {

	start := time.Now()
	log.Printf("INFO: worker starting")

	// ensure this is the type of event we want to process
	switch busEvent.EventName {
	case uvaaptsbus.EventBagInitiate:
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

	// get the submission
	sub, err := dao.GetSubmissionByIdentifier(wf.SubmissionId)
	if err != nil {
		log.Printf("ERROR: getting submission details (%s)", err.Error())
		done <- false
		return
	}

	// S3 assets in <bucket>/<clientId>/<submissionId>/<bag name>/...
	bagKey := fmt.Sprintf("%s/%s/%s", busEvent.ClientId, wf.SubmissionId, wf.BagId)

	// local assets in <cache root>/<clientId>/<submissionId>/<bag name>/...
	localSubmissionRoot := fmt.Sprintf("%s/%s/%s", cfg.LocalAssetCache, busEvent.ClientId, wf.SubmissionId)
	localBagRoot := fmt.Sprintf("%s/%s", localSubmissionRoot, wf.BagId)
	localBagName := fmt.Sprintf("%s/%s.tar", localSubmissionRoot, wf.BagId)
	localSyncRoot := fmt.Sprintf("%s/data", localBagRoot)

	// do the sync
	err = syncAssets(cfg.InboundBucket, bagKey, localSyncRoot, cfg.SyncWorkers)
	if err != nil {
		done <- false
		return
	}

	// check the local assets for title and description files
	title, description := processAptMetaContent(localSyncRoot, wf.SubmissionId)

	// build the bagging attribute structure
	attribs := BaggingAttributes{
		BagGroupIdentifier: sub.CollectionName,
		Date:               time.Now().UTC().Format("2006-01-02"),
		Description:        description,
		SenderDescription:  "",
		SenderIdentifier:   wf.BagId,
		Storage:            sub.Storage,
		Title:              title,
	}
	// do the bagging
	err = bagAssets(localSubmissionRoot, wf.BagId, localBagName, attribs)
	if err != nil {
		done <- false
		return
	}

	// we are done, publish the appropriate event and terminate
	_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventBagBuilt, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %0.2f seconds)", duration.Seconds())
	done <- true
}

func processAptMetaContent(root string, sid string) (string, string) {

	titleFile := path.Join(root, titleFileName)
	descriptionFile := path.Join(root, descriptionFileName)
	title := sid
	description := sid

	b, err := os.ReadFile(titleFile)
	if err == nil {
		title = strings.TrimRight(string(b), "\r\n")
		log.Printf("INFO: using included title [%s]", title)
	}

	b, err = os.ReadFile(descriptionFile)
	if err == nil {
		description = strings.TrimRight(string(b), "\r\n")
		log.Printf("INFO: using included description [%s]", description)
	}

	// remove these files (if they exist), they dont go to APTrust
	_ = os.Remove(titleFile)
	_ = os.Remove(descriptionFile)

	return title, description
}

//
// end of file
//
