//
//
//

package main

import (
	"log"
	"strings"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func supressBagDuplicates(dao *uvaaptsdao.Dao, conflictSet []ConflictTuple) ([]ConflictTuple, error) {

	// sanity check
	if len(conflictSet) == 0 {
		return conflictSet, nil
	}

	remainingSet := make([]ConflictTuple, 0)

	// see if we can remove any conflicts due to bag duplicates
	for _, cfs := range conflictSet {

		log.Printf("DEBUG: evaluating <%s:%s> for bag duplicates", cfs.local.BagName, cfs.local.Name)

		remaining := make([]uvaaptsdao.File, 0)
		for _, c := range cfs.conflicts {
			if sameBag(cfs.local.BagName, c.BagName) == true {
				log.Printf("INFO: duplicate hash from duplicate bag, ignoring <%s:%s>", c.BagName, c.Name)
			} else {
				remaining = append(remaining, c)
			}
		}

		if len(remaining) > 0 {
			ct := ConflictTuple{
				local:     cfs.local,
				conflicts: remaining,
			}
			remainingSet = append(remainingSet, ct)
		}
	}
	return remainingSet, nil
}

// APTrust bag names have the source organization prepended so we look for the local bag name
// as the suffix
func sameBag(localBn string, aptBn string) bool {
	return strings.HasSuffix(aptBn, localBn)
}

//
// end of file
//
