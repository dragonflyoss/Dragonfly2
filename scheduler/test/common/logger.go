/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common

import (
	"fmt"
	"os"
	"time"

	"github.com/pborman/uuid"
)

// TestLogger defines operations common across different types of testing
type TestLogger interface {
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Log(args ...interface{})
	Logf(format string, args ...interface{})
}

type e2eLogger struct{}

func NewE2ELogger() TestLogger {
	return e2eLogger{}
}

func (e2eLogger) Errorf(format string, args ...interface{}) {
	Errorf(format, args...)
}

func (e2eLogger) Fatal(args ...interface{}) {
	// TODO(marun) Is there a nicer way to do this?
	FailfWithOffset(1, "%v", args)
}

func (e2eLogger) Fatalf(format string, args ...interface{}) {
	FailfWithOffset(1, format, args...)
}

func (e2eLogger) Log(args ...interface{}) {
	// TODO(marun) Is there a nicer way to do this?
	Logf("%v", args)
}

func (e2eLogger) Logf(format string, args ...interface{}) {
	Logf(format, args...)
}

const (
	// Using the same interval as integration should be fine given the
	// minimal load that the apiserver is likely to be under.
	PollInterval = 50 * time.Millisecond
	// How long to try single API calls (like 'get' or 'list'). Used to prevent
	// transient failures from failing tests.
	DefaultSingleCallTimeout = 30 * time.Second
)

// RunID is unique identifier of the e2e run.
var RunID = uuid.NewUUID()

func nowStamp() string {
	return time.Now().Format(time.StampMilli)
}

func log(level string, format string, args ...interface{}) {
	// fmt.Fprintf(ginkgo.GinkgoWriter, nowStamp()+": "+level+": "+format+"\n", args...)
	fmt.Fprintf(os.Stdout, nowStamp()+": "+level+": "+format+"\n", args...)
}

func Errorf(format string, args ...interface{}) {
	log("ERROR", format, args...)
}

func Logf(format string, args ...interface{}) {
	log("INFO", format, args...)
}

func Failf(format string, args ...interface{}) {
	FailfWithOffset(1, format, args...)
}

// FailfWithOffset calls "Fail" and logs the error at "offset" levels above its caller
// (for example, for call chain f -> g -> FailfWithOffset(1, ...) error would be logged for "f").
func FailfWithOffset(offset int, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log("INFO", msg)
	Fail(nowStamp()+": "+msg, 1+offset)
}

// SetUp is likely to be fixture-specific, but TearDown needs to be
// consistent to enable TearDownOnPanic.
type TestFixture interface {
	TearDown(tl TestLogger)
}
