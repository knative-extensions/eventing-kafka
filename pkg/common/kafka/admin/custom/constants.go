package custom

import "time"

//
// Custom REST Sidecar Constants
//
// NOTE - These are expected to be used by third-party implementers of
//        custom sidecars, do not remove due to "unused" status in IDE!
//
const (
	SidecarHost     = "localhost"      // The Host name used when making requests to the K8S sidecar.
	SidecarPort     = "8888"           // The HTTP port on which the sidecar must be listening for POST / DELETE requests.
	TopicsPath      = "/topics"        // The HTTP request path for Kafka Topic creation / deletion to be implemented by the sidecar.
	TopicNameHeader = "Slug"           // The HTTP Header key used to identify the TopicName in the POST request.
	SidecarTimeout  = 30 * time.Second // How long to wait for the sidecar's server to respond.
)
