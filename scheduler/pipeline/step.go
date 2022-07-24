package pipeline

type Step interface {
	out
	// Exec is invoked by the pipeline when it is run
	Exec(*Request) *Result
	// Cancel is invoked by the pipeline when one of the concurrent steps set Result{Error:err}
	Cancel() error
}

type out interface {
	GetCtx() *stepContext
	SetCtx(ctx *stepContext)
	SetInfo(key string, val interface{})
}

type stepContext struct {
	name   string
	KeyVal map[string]interface{}
}

type StepContext struct {
	stepCtx *stepContext
}

func (sc *StepContext) GetCtx() *stepContext {
	return sc.stepCtx
}

func (sc *StepContext) SetCtx(ctx *stepContext) {
	sc.stepCtx = ctx
}

func (sc *StepContext) SetInfo(key string, val interface{}) {
	if sc.stepCtx == nil {
		sc.SetCtx(&stepContext{})
		sc.stepCtx.KeyVal = make(map[string]interface{})
	}
	sc.GetCtx().KeyVal[key] = val
}
