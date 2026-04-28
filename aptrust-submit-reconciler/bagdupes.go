//
//
//

package main

import (
	"log"
	"strings"
)

func ignoreBagDuplicates(conflictSeries *ConflictSeries) (*ConflictSeries, error) {

	// sanity check
	if conflictSeries.outstanding() == false {
		return conflictSeries, nil
	}

	// see if we can remove any conflicts due to bag duplicates
	for ix, csc := range conflictSeries.conflicts {

		// no processing required for already ignored files
		if csc.localFile.ignored == true {
			continue
		}

		log.Printf("DEBUG: evaluating <%s:%s> for bag duplicates", csc.localFile.file.BagName, csc.localFile.file.Name)

		for iy, pc := range csc.possibleConflicts {
			if sameBag(csc.localFile.file.BagName, pc.file.BagName) == true {
				log.Printf("INFO: duplicate hash from duplicate bag, ignoring <%s:%s>", pc.file.BagName, pc.file.Name)
				conflictSeries.conflicts[ix].possibleConflicts[iy].ignored = true
			}
		}
	}

	return conflictSeries, nil
}

// APTrust bag names have the source organization prepended so we look for the local bag name
// as the suffix
func sameBag(localBn string, aptBn string) bool {
	return strings.HasSuffix(aptBn, localBn)
}

//
// end of file
//
