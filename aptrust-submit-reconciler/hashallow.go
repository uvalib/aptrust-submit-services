//
//
//

package main

import (
	"errors"
	"log"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func ignoreHashAllow(conflictSeries *ConflictSeries) (*ConflictSeries, error) {

	// sanity check
	if conflictSeries.outstanding() == false {
		return conflictSeries, nil
	}

	// get our hash allow list set
	hashAllowList, err := conflictSeries.dao.GetHashAllowList()
	if err != nil {
		if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
			log.Printf("ERROR: getting hash allow list (%s)", err.Error())
			return nil, err
		}
	}

	// if we have allowed hashes, we may be able to ignore files in the conflict set
	if hashAllowList != nil && len(hashAllowList) > 0 {

		log.Printf("INFO: %d hash allow/ignore entries", len(hashAllowList))

		for ix, csc := range conflictSeries.conflicts {
			w := inHashAllowList(hashAllowList, csc.localFile.file.Hash)
			if w != nil {
				log.Printf("INFO: hash found in allow list, ignoring [%s] (%s)", csc.localFile.file.Name, w.Comment)
				conflictSeries.conflicts[ix].localFile.ignored = true
			}
		}
	}

	return conflictSeries, nil
}

func inHashAllowList(allowlist []uvaaptsdao.HashAllowEntry, hash string) *uvaaptsdao.HashAllowEntry {
	for _, w := range allowlist {
		if w.Hash == hash {
			return &w
		}
	}
	return nil
}

//
// end of file
//
