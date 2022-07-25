package pipeline

var (
	BuildMap map[string]func() Step
)

func init() {
	BuildMap = make(map[string]func() Step, 0)
}
