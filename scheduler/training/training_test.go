/*
 *     Copyright 2022 The Dragonfly Authors
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

package training

import (
	"d7y.io/dragonfly/v2/scheduler/storage"
	"fmt"
	"testing"
)

func TestGetInstanceHeaders(t *testing.T) {
	_, i, err := GetInstanceHeaders()
	if err != nil {
		return
	}
	fmt.Println(i)
}

func TestRecordsTransData(t *testing.T) {
	record1 := &storage.Record{
		ID:                   "0",
		IP:                   "1.1.1.1",
		Hostname:             "test-host-name",
		BizTag:               "test-biz-tag",
		Cost:                 250,
		PieceCount:           10,
		TotalPieceCount:      20,
		ContentLength:        15230,
		SecurityDomain:       "test-security",
		IDC:                  "test-idc",
		NetTopology:          "test-netTopology",
		Location:             "test-location",
		FreeUploadLoad:       120,
		State:                2,
		HostType:             10,
		CreateAt:             1657463259,
		UpdateAt:             1658463259,
		ParentID:             "103",
		ParentIP:             "1.1.1.2",
		ParentHostname:       "test-p-hostname",
		ParentBizTag:         "test-biz-tag",
		ParentPieceCount:     24,
		ParentSecurityDomain: "test-domain",
		ParentIDC:            "test-idc",
		ParentNetTopology:    "test-topology",
		ParentLocation:       "test-location-123",
		ParentFreeUploadLoad: 222,
		ParentHostType:       13,
		ParentCreateAt:       1657464259,
		ParentUpdateAt:       1657474742,
	}
	record2 := &storage.Record{
		ID:                   "0",
		IP:                   "1.1.135.2",
		Hostname:             "test-host-name",
		BizTag:               "test-biz-tag",
		Cost:                 260,
		PieceCount:           15,
		TotalPieceCount:      34,
		ContentLength:        152324,
		SecurityDomain:       "test-security",
		IDC:                  "test-idc",
		NetTopology:          "test-netTopology",
		Location:             "test-location",
		FreeUploadLoad:       120,
		State:                2,
		HostType:             10,
		CreateAt:             1657463259,
		UpdateAt:             1658543939,
		ParentID:             "103",
		ParentIP:             "1.1.1.2",
		ParentHostname:       "test-p-hostname",
		ParentBizTag:         "test-biz-tag",
		ParentPieceCount:     54,
		ParentSecurityDomain: "test-domain",
		ParentIDC:            "test-idc",
		ParentNetTopology:    "test-topology",
		ParentLocation:       "test-location-123",
		ParentFreeUploadLoad: 236,
		ParentHostType:       13,
		ParentCreateAt:       1657464259,
		ParentUpdateAt:       1657474742,
	}
	records := make([]*storage.Record, 1000)
	for i := 0; i < 1000; i++ {
		records[i] = record1
	}
	fmt.Println(record2)
	/*
		fmt.Println(record2)
		data := RecordsTransData(records)
		instances, _ := DataToInstances(data)
		fmt.Println(instances)
		regression, err := train(instances)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(regression)
	*/
	t1 := new(Training)
	model, re, _ := t1.Serve(records)
	fmt.Println(re)
	fmt.Println(model)
	s1 := new(Saving)
	data, _ := s1.Serve(model)
	fmt.Println(data)
	l1 := new(Loading)
	mo, _ := l1.Serve(data)
	fmt.Println(mo)
}
