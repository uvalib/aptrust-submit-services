//
//
//

package main

import (
	"errors"
	"log"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func supressBagAllow(dao *uvaaptsdao.Dao, conflictSet []ConflictTuple) ([]ConflictTuple, error) {

	// sanity check
	if len(conflictSet) == 0 {
		return conflictSet, nil
	}

	// get our bag allow list set
	bagAllowList, err := dao.GetBagAllowList()
	if err != nil {
		if errors.As(err, &uvaaptsdao.ErrBagNotFound) == false {
			log.Printf("ERROR: getting bag allow list (%s)", err.Error())
			return nil, err
		}
	}

	// if we have allowed bags, we may be able to remove files from the conflict set
	if bagAllowList != nil && len(bagAllowList) > 0 {

		log.Printf("INFO: %d bag allow/ignore entries", len(bagAllowList))

		remainingSet := make([]ConflictTuple, 0)

		// see if we can remove any conflicts due to bag allows
		for _, cfs := range conflictSet {

			log.Printf("DEBUG: evaluating <%s:%s> for bag allow", cfs.local.BagName, cfs.local.Name)

			remaining := make([]uvaaptsdao.File, 0)
			for _, c := range cfs.conflicts {
				if inBagAllowList(bagAllowList, c.BagName) == true {
					log.Printf("INFO: conflict in bag allow/ignore, ignoring <%s:%s>", c.BagName, c.Name)
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

	return conflictSet, nil
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
