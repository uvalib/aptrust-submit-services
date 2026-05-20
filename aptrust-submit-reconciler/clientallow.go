//
//
//

package main

import (
	"log"
)

var libraOpenClientName = "libra-open"

func ignoreClientAllow(conflictSeries *ConflictSeries, clientId string) (*ConflictSeries, error) {

	// sanity check
	if conflictSeries.outstanding() == false {
		return conflictSeries, nil
	}

	// get the client...
	client, err := conflictSeries.dao.GetClientByIdentifier(clientId)
	if err != nil {
		log.Printf("ERROR: getting client information (%s)", err.Error())
		return nil, err
	}

	switch client.Name {
	case libraOpenClientName:
		return ignoreLibraOpenAllow(conflictSeries)
	}
	return conflictSeries, nil
}

//
// end of file
//
