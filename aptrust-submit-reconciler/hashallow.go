//
//
//

package main

import (
	"errors"
	"log"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func supressHashAllow(dao *uvaaptsdao.Dao, conflicts []uvaaptsdao.File) ([]uvaaptsdao.File, error) {

	// sanity check
	if len(conflicts) == 0 {
		return conflicts, nil
	}

	// get our hash allow list set
	hashAllowList, err := dao.GetHashAllowList()
	if err != nil {
		if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
			log.Printf("ERROR: getting hash allow list (%s)", err.Error())
			return nil, err
		}
	}

	// if we have allowed hashes, we may be able to remove files from the conflict set
	if hashAllowList != nil && len(hashAllowList) > 0 {

		log.Printf("INFO: %d hash allow/ignore entries", len(hashAllowList))

		remaining := make([]uvaaptsdao.File, 0)
		for _, f := range conflicts {
			w := inHashAllowList(hashAllowList, f.Hash)
			if w != nil {
				log.Printf("INFO: hash found in allow list, ignoring [%s] (%s)", f.Name, w.Comment)
			} else {
				remaining = append(remaining, f)
			}
		}
		return remaining, nil
	}

	return conflicts, nil
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
