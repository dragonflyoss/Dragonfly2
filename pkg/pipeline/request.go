package pipeline

// Request is the result dispatched in a previous step.
type Request struct {
	Data   interface{}
	KeyVal map[string]interface{}
}
