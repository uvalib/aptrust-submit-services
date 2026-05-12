//
//
//

package main

// the temp filesystem
var tempFilesystem = "/tmp"

// the name of the manifest file
var manifestName = "manifest-md5.txt"

type ApprovalEventExtraPayload struct {
	ComputeID string `json:"computeID"`
	Storage   string `json:"storage"`
}

type BagSubmittedEventExtraPayload struct {
	ETag string `json:"etag"`
}

//
// end of file
//
