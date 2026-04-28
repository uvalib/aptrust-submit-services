//
//
//

package main

import (
	"errors"
	"log"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func ignoreBagAllow(conflictSeries *ConflictSeries) (*ConflictSeries, error) {

	// sanity check
	if conflictSeries.outstanding() == false {
		return conflictSeries, nil
	}

	// get our bag allow list set
	bagAllowList, err := conflictSeries.dao.GetBagAllowList()
	if err != nil {
		if errors.As(err, &uvaaptsdao.ErrBagNotFound) == false {
			log.Printf("ERROR: getting bag allow list (%s)", err.Error())
			return nil, err
		}
	}

	// if we have allowed bags, we may be able to remove files from the conflict set
	if bagAllowList != nil && len(bagAllowList) > 0 {

		log.Printf("INFO: %d bag allow/ignore entries", len(bagAllowList))

		// see if we can remove any conflicts due to bag allows
		for ix, csc := range conflictSeries.conflicts {

			// no processing required for already ignored files
			if csc.localFile.ignored == true {
				continue
			}

			log.Printf("DEBUG: evaluating <%s:%s> for bag allow", csc.localFile.file.BagName, csc.localFile.file.Name)

			for iy, pc := range csc.possibleConflicts {
				if inBagAllowList(bagAllowList, pc.file.BagName) == true {
					log.Printf("INFO: conflict in bag allow/ignore, ignoring <%s:%s>", pc.file.BagName, pc.file.Name)
					conflictSeries.conflicts[ix].possibleConflicts[iy].ignored = true
				}
			}
		}
	}

	return conflictSeries, nil
}

func inBagAllowList(allowlist []uvaaptsdao.BagAllowEntry, name string) bool {
	for _, a := range allowlist {
		if sameBag(name, a.Name) == true {
			return true
		}
	}
	return false
}

//
// end of file
//
