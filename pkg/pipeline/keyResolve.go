package pipeline

// all step will be kept in BuildMap(project scope), key must has the
// way to tell which builder to choose

type Builder interface {
	BuildStep() Step
}

var BuildMap map[int]Builder

func init() {
	BuildMap = make(map[int]Builder)
}

type keyResolver func(key string) int
