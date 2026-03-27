//
//
//

package main

import (
	"fmt"
	"strings"
)

// from the list of files included in the submission, find the manifests
func findIncludedManifests(prefix string, suppliedFiles []string) []string {
	manifests := make([]string, 0)

	for _, fname := range suppliedFiles {

		// is this a manifest
		if strings.HasSuffix(fname, manifestName) {
			// strip the prefix and the trailing slash character
			s := strings.TrimPrefix(fname, fmt.Sprintf("%s/", prefix))

			// save the manifest name
			manifests = append(manifests, s)
		}
	}
	return manifests
}

//
// end of file
//
