package pipeline

// all step will be kept in BuildMap(project scope), key must has the
// way to tell which builder to choose

var (
	BuildMap   map[int]func() Step
	KeyResolve = getKey
)

func init() {
	BuildMap = make(map[int]func() Step)

}

type keyResolver func(key string) int

func getKey(key string) int {
	if key == "load" {
		return 0
	} else {
		return 1
	}
}
