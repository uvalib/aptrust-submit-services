package main

import (
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

// ensure the files supplied and the files itemized in the manifest(s) match exactly, recording a
// failure for each discrepancy. Returns the number of failures recorded
func enumerationFailures(dao *uvaaptsdao.Dao, sid string, supplied []string, manifests []string, itemized []ManifestRow, prefix string) int {

	failures := 0

	// the set of supplied files (relative to the submission), excluding the manifest files
	suppliedSet := make(map[string]bool)
	for _, s := range supplied {
		fn := strings.TrimPrefix(s, prefix+"/")
		if slices.Contains(manifests, fn) == false {
			suppliedSet[fn] = true
		}
	}

	// go through the items specified and log each one that was not supplied or is duplicated.
	// named the same way as the S3 key so the comparison is exact
	itemizedSet := make(map[string]bool)
	for _, i := range itemized {
		fn := fmt.Sprintf("%s/%s", i.bag, i.file)
		if itemizedSet[fn] == true {
			failures += recordEnumerationFailure(dao, sid, fmt.Sprintf("%s appears more than once in a manifest", fn))
			continue
		}
		itemizedSet[fn] = true
		if suppliedSet[fn] == false {
			failures += recordEnumerationFailure(dao, sid, fmt.Sprintf("%s appears in a manifest but was NOT supplied", fn))
		}
	}

	// go through the files supplied and log each one that was not included in the manifest(s)
	for _, s := range supplied {
		fn := strings.TrimPrefix(s, prefix+"/")
		if suppliedSet[fn] == true && itemizedSet[fn] == false {
			failures += recordEnumerationFailure(dao, sid, fmt.Sprintf("%s was supplied but does not appear in a manifest", fn))
		}
	}

	return failures
}

func recordEnumerationFailure(dao *uvaaptsdao.Dao, sid string, reason string) int {
	log.Printf("ERROR: %s", reason)
	_ = recordFailure(dao, sid, reason)
	return 1
}

func recordFailure(dao *uvaaptsdao.Dao, sid string, reason string) error {
	return dao.AddFailure(sid, reason)
}

//
// end of file
//
