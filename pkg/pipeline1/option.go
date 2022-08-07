package pipeline1

type Option struct {
	Agg func(val interface{}) interface{}
}

type OptionFunc func(*Option)

//func DefaultAgg() OptionFunc {
//	return func(option *Option) {
//		option.Agg = func(val interface{}) interface{} {
//			re
//		}
//	}
//}
