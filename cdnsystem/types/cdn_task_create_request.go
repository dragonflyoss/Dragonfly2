package types


type CdnTaskCreateRequest struct {

	Headers map[string]string `json:"headers,omitempty"`

	// md5 checksum for the resource to distribute. dfget catches this parameter from dfget's CLI
	// and passes it to supernode. When supernode finishes downloading file/image from the source location,
	// it will validate the source file with this md5 value to check whether this is a valid file.
	//
	Md5 string `json:"md5,omitempty"`

	// path is used in one peer A for uploading functionality. When peer B hopes
	// to get piece C from peer A, B must provide a URL for piece C.
	// Then when creating a task in supernode, peer A must provide this URL in request.
	//
	Path string `json:"path,omitempty"`

	// PeerID is used to uniquely identifies a peer which will be used to create a dfgetTask.
	// The value must be the value in the response after registering a peer.
	//
	PeerID string `json:"peerID,omitempty"`

	// The is the resource's URL which user uses dfget to download. The location of URL can be anywhere, LAN or WAN.
	// For image distribution, this is image layer's URL in image registry.
	// The resource url is provided by command line parameter.
	//
	URL string `json:"rawURL,omitempty"`

	// IP address of supernode which the peer connects to
	SupernodeIP string `json:"supernodeIP,omitempty"`

	// This attribute represents the digest of resource, dfdaemon or dfget catches this parameter
	// from the headers of request URL. The digest will be considered as the taskID if not null.
	//
	TaskID string `json:"taskId,omitempty"`

	// taskURL is generated from rawURL. rawURL may contains some queries or parameter, dfget will filter some queries via
	// --filter parameter of dfget. The usage of it is that different rawURL may generate the same taskID.
	//
	TaskURL string `json:"taskURL,omitempty"`
}
