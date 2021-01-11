package dferror

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

var (
	Unspecified = NewCodeError(int32(base.Code_X_UNSPECIFIED), "unspecified")
	// no problem 200-299
	Success = NewCodeError(int32(base.Code_SUCCESS), "success")
	// client processing error 400-499
	ClientError = NewCodeError(int32(base.Code_CLIENT_ERROR), "client error")
	// scheduler processing error 500-599
	SchedulerError         = NewCodeError(int32(base.Code_SCHEDULER_ERROR), "scheduler error")
	SchedulerPieceDone     = NewCodeError(int32(base.Code_SCHEDULER_ERROR)+1, "scheduler piece done")
	SchedulerWaitPiece     = NewCodeError(int32(base.Code_SCHEDULER_ERROR)+2, "scheduler wait piece from cdn")
	SchedulerWaitReadyHost = NewCodeError(int32(base.Code_SCHEDULER_ERROR)+3, "scheduler wait ready host")
	SchedulerFinished      = NewCodeError(int32(base.Code_SCHEDULER_ERROR)+4, "scheduler finished")
	// cdnsystem processing error 600-699
	CDNError = NewCodeError(int32(base.Code_CDN_ERROR), "cdn error")
	// manager processing error 700-799
	ManagerError = NewCodeError(int32(base.Code_MANAGER_ERROR), "manager error")
	// shared error 1000-1099
	UnknownError   = NewCodeError(int32(base.Code_UNKNOWN_ERROR), "unknown error")
	ParamInvalid   = NewCodeError(int32(base.Code_PARAM_INVALID), "param invalid")
	RequestTimeout = NewCodeError(int32(base.Code_REQUEST_TIME_OUT), "request timeout")
)
