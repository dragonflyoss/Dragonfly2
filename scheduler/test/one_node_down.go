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

package test

import (
	"d7y.io/dragonfly/v2/scheduler/test/common"
	"d7y.io/dragonfly/v2/scheduler/test/mock_client"
	"reflect"
	"time"
)

func (suite *SchedulerTestSuite) Test601OneClientDown() {
	tl := common.NewE2ELogger()

	var (
		clientNum  = 5
		stopChList []chan struct{}
		badClient  *mock_client.MockClient
	)
	badClient = mock_client.NewMockClient("127.0.0.1:8002", "http://dragonfly.com?type=bad_client", "bc", tl)
	go badClient.Start()
	time.Sleep(time.Second)
	for i := 0; i < clientNum; i++ {
		client := mock_client.NewMockClient("127.0.0.1:8002", "http://dragonfly.com?type=bad_client", "bc", tl)
		go client.Start()
		stopCh := client.GetStopChan()
		stopChList = append(stopChList, stopCh)
	}
	time.Sleep(time.Second)
	badClient.SetDone()
	time.Sleep(time.Second * 5)
	for i := 0; i < clientNum; i++ {
		client := mock_client.NewMockClient("127.0.0.1:8002", "http://dragonfly.com?type=bad_client", "bc", tl)
		go client.Start()
		stopCh := client.GetStopChan()
		stopChList = append(stopChList, stopCh)
	}

	time.Sleep(time.Second * 5)
	timer := time.After(time.Minute * 10)
	caseList := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timer), Send: reflect.Value{}},
	}
	for _, stopCh := range stopChList {
		caseList = append(caseList, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stopCh), Send: reflect.Value{}})
	}
	closedNumber := 0
	for {
		selIndex, _, _ := reflect.Select(caseList)
		caseList = append(caseList[:selIndex], caseList[selIndex+1:]...)
		if selIndex == 0 {
			suite.Fail("download file failed")
			tl.Fatalf("download file failed")
		} else {
			closedNumber++
			if closedNumber >= clientNum {
				break
			}
		}
	}
	tl.Log("bad client test all client download file finished")
}
