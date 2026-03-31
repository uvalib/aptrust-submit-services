//
//
//

package main

import (
	"errors"
	"log"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func supressWhitelisted(dao *uvaaptsdao.Dao, conflicts []uvaaptsdao.File) ([]uvaaptsdao.File, error) {

	// sanity check
	if len(conflicts) == 0 {
		return conflicts, nil
	}

	// get our whitelisted file set
	whitelist, err := dao.GetWhitelistedFiles()
	if err != nil {
		if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
			log.Printf("ERROR: getting whitelist fileset (%s)", err.Error())
			return nil, err
		}
	}

	// if we have whitelisted files, we may be able to remove files from the conflict set
	if whitelist != nil && len(whitelist) > 0 {
		remaining := make([]uvaaptsdao.File, 0)
		for _, f := range conflicts {
			w := inWhitelist(whitelist, f.Hash)
			if w != nil {
				log.Printf("INFO: hash found in whitelist fileset, ignoring [%s] (%s)", f.Name, w.Comment)
			} else {
				remaining = append(remaining, f)
			}
		}
		return remaining, nil
	}

	return conflicts, nil
}

func inWhitelist(whitelist []uvaaptsdao.WhitelistedFile, hash string) *uvaaptsdao.WhitelistedFile {
	for _, w := range whitelist {
		if w.Hash == hash {
			return &w
		}
	}
	return nil
}

//
// end of file
//
