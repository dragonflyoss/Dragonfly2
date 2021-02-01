package test

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/types"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	. "github.com/onsi/ginkgo"
)

var _ = Describe("Scheduler Misc Test", func() {
	tl := common.NewE2ELogger()

	Describe("Scheduler TaskId Generate Test", func() {
		It("should be Generate TaskId same", func() {
			filter := "param2&param3"
			url1 := "http://dragonfly.com/file/test.txt?param1=3&param2=4"
			url2 := "http://dragonfly.com/file/test.txt?param1=3&param2=5"
			taskId1 := types.GenerateTaskId(url1, filter, nil)
			taskId2 := types.GenerateTaskId(url2, filter, nil)
			if taskId1 != taskId2 {
				tl.Fatalf("taskId1[%s] != taskId2[%s]", taskId1, taskId2)
				return
			}
		})

		It("should be Generate TaskId different", func() {
			filter := ""
			url1 := "http://dragonfly.com/file/test.txt?param1=3&param2=4"
			url2 := "http://dragonfly.com/file/test.txt?param1=3&param2=5"
			taskId1 := types.GenerateTaskId(url1, filter, nil)
			taskId2 := types.GenerateTaskId(url2, filter, nil)
			if taskId1 == taskId2 {
				tl.Fatalf("taskId1[%s] == taskId2[%s]", taskId1, taskId2)
				return
			}
		})

		It("should be Generate TaskId different with UrlMeta", func() {
			filter := "param2&param3"
			meta1 := &base.UrlMeta{Range:"0-100", Md5:"123"}
			url1 := "http://dragonfly.com/file/test.txt?param1=3&param2=4"
			meta2 := &base.UrlMeta{Range:"40-100", Md5:"456"}
			url2 := "http://dragonfly.com/file/test.txt?param1=3&param2=5"
			taskId1 := types.GenerateTaskId(url1, filter, meta1)
			taskId2 := types.GenerateTaskId(url2, filter, meta2)
			if taskId1 == taskId2 {
				tl.Fatalf("taskId1[%s] == taskId2[%s]", taskId1, taskId2)
				return
			}
		})

		It("should be Generate TaskId same with UrlMeta", func() {
			filter := "param2&param3"
			meta1 := &base.UrlMeta{Range:"0-100", Md5:"123"}
			url1 := "http://dragonfly.com/file/test.txt?param1=3&param2=4"
			meta2 := &base.UrlMeta{Range:"0-100", Md5:"123"}
			url2 := "http://dragonfly.com/file/test.txt?param1=3&param2=5"
			taskId1 := types.GenerateTaskId(url1, filter, meta1)
			taskId2 := types.GenerateTaskId(url2, filter, meta2)
			if taskId1 != taskId2 {
				tl.Fatalf("taskId1[%s] == taskId2[%s]", taskId1, taskId2)
				return
			}
		})
	})
})
