package types

type UrlMeta struct {
	// md5 of content downloaded from url
	Md5 string
	// downloading range for single file
	Range string
}

type SeedTaskInfo struct {
	TaskID           string
	Url              string
	SourceFileLength int64
	CdnFileLength    int64
	RequestMd5       string
	PieceSize        int32
	Headers          map[string]string
	CdnStatus        string
	PieceTotal       int32
	RealMd5          string
}

const (

	// TaskInfoCdnStatusWAITING captures enum value "WAITING"
	TaskInfoCdnStatusWAITING string = "WAITING"

	// TaskInfoCdnStatusRUNNING captures enum value "RUNNING"
	TaskInfoCdnStatusRUNNING string = "RUNNING"

	// TaskInfoCdnStatusFAILED captures enum value "FAILED"
	TaskInfoCdnStatusFAILED string = "FAILED"

	// TaskInfoCdnStatusSUCCESS captures enum value "SUCCESS"
	TaskInfoCdnStatusSUCCESS string = "SUCCESS"

	// TaskInfoCdnStatusSOURCEERROR captures enum value "SOURCE_ERROR"
	TaskInfoCdnStatusSOURCEERROR string = "SOURCE_ERROR"
)
