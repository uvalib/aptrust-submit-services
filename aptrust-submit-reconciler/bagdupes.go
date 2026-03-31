//
//
//

package main

import (
	"log"
	"strings"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func supressBagDuplicates(dao *uvaaptsdao.Dao, conflicts []uvaaptsdao.File) ([]uvaaptsdao.File, error) {

	// sanity check
	if len(conflicts) == 0 {
		return conflicts, nil
	}

	// see if we can remove any conflicts due to bag duplicates
	remaining := make([]uvaaptsdao.File, 0)
	for _, cf := range conflicts {

		// initially assume the conflict is with an APTrust file
		aptConflicts, err := dao.GetAptFilesByHash(cf.Hash)
		if err != nil {
			//if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
			return nil, err
			//}
		}
		if len(aptConflicts) > 0 {
			for _, aptcf := range aptConflicts {
				if sameBag(cf.BagName, aptcf.BagName) == true {
					log.Printf("INFO: duplicate hash from duplicate bag, ignoring [%s]", cf.Name)
				} else {
					remaining = append(remaining, cf)
				}
			}
		} else {
			remaining = append(remaining, cf)
		}
	}
	return remaining, nil
}

// APTrust bag names have the source organization prepended so we look for the local bag name
// as the suffix
func sameBag(localBn string, aptBn string) bool {
	return strings.HasSuffix(aptBn, localBn)
}

//
// end of file
//
