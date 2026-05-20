//
//
//

package main

import (
	"log"
	"path/filepath"
	"regexp"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func ignoreLibraOpenAllow(conflictSeries *ConflictSeries) (*ConflictSeries, error) {

	log.Printf("INFO: evaluating libra-open allow rules")

	for ix, csc := range conflictSeries.conflicts {

		// no processing required for already ignored files
		if csc.localFile.ignored == true {
			continue
		}

		for iy, pc := range csc.possibleConflicts {

			// no processing required for already ignored conflicting files
			if pc.ignored == true {
				continue
			}

			// if the names are not identical, we can ignore this conflict
			bn := filepath.Base(pc.file.Name)
			if csc.localFile.file.Name != bn {
				log.Printf("INFO: file names differ, ignoring conflict")
				conflictSeries.conflicts[ix].possibleConflicts[iy].ignored = true
				continue
			}

			sourceBagContents, err := conflictSeries.dao.GetFilesBySubmissionAndBagName(csc.localFile.file.Submission, csc.localFile.file.BagName)
			if err != nil {
				log.Printf("ERROR: getting submission/bag files (%s)", err.Error())
				return nil, err
			}

			conflictBagContents, err := conflictSeries.dao.GetAptFilesByBag(pc.file.BagName)
			if err != nil {
				log.Printf("ERROR: getting APTrust cache files (%s)", err.Error())
				return nil, err
			}

			//
			// if the conflict is the result of 2 differently named but otherwise identical bags,
			// we must not ignore it
			//

			if libraOpenAllow(sourceBagContents, conflictBagContents) == true {
				conflictSeries.conflicts[ix].possibleConflicts[iy].ignored = true
			}
		}
	}

	return conflictSeries, nil
}

func libraOpenAllow(sourceBagContents []uvaaptsdao.File, conflictBagContents []uvaaptsdao.File) bool {

	// the files we need to compare
	sourceBagFiles := make(map[string]string)
	conflictBagFiles := make(map[string]string)

	// use regex grammar
	sourceIgnore := []string{"aptrust-description.txt", "aptrust-title.txt", "native-payload.json"}
	conflictIgnore := []string{"aptrust-info.txt", "bag-info.txt", "embargo.json", "rights.json", "work.json", "visibility.json", "author.*\\.json", "fileset.*\\.json"}

	for _, f := range sourceBagContents {
		if matches(f.Name, sourceIgnore) == false {
			log.Printf("INFO: source bag filtered hash/file [%s/%s]", f.Hash, f.Name)
			sourceBagFiles[f.Name] = f.Hash
		}
	}

	for _, f := range conflictBagContents {
		bn := filepath.Base(f.Name)
		if matches(bn, conflictIgnore) == false {
			log.Printf("INFO: conflict bag filtered hash/file [%s/%s]", f.Hash, bn)
			conflictBagFiles[bn] = f.Hash
		}
	}

	// if we have same number of files we may not be able to ignore it
	if len(sourceBagFiles) == len(conflictBagFiles) {
		for fname, srcHash := range sourceBagFiles {
			dstHash, ok := conflictBagFiles[fname]
			if ok == false {
				log.Printf("INFO: bag contents differ (%s), ignoring conflict", fname)
				// item in source bag does not appear in the conflicting bag, we can ignore
				return true
			}
			if srcHash != dstHash {
				log.Printf("INFO: bag contents hashes differ (%s), ignoring conflict", fname)
				// item in source bag has a different hash than the one in the conflicting bag,
				// we can ignore
				return true
			}
		}
		// bags are basically identical, dont ignore this
		log.Printf("WARNING: bag contents appear identical, must not ignore this conflict")
		return false
	}

	// we can ignore this
	log.Printf("INFO: bag sizes differ, ignoring conflict")
	return true
}

func matches(str string, matchset []string) bool {
	for _, m := range matchset {
		match, _ := regexp.MatchString(m, str)
		if match {
			return true
		}
	}
	return false
}

//
// end of file
//
