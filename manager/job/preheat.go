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

package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/util/net/httputils"
	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/manifest/schema2"
)

type PreheatType string

const (
	PreheatImageType PreheatType = "image"
	PreheatFileType  PreheatType = "file"
)

var accessURLPattern, _ = regexp.Compile("^(.*)://(.*)/v2/(.*)/manifests/(.*)")

type Preheat interface {
	CreatePreheat([]model.Scheduler, types.CreatePreheatRequest) (*types.Preheat, error)
	GetPreheat(string) (*types.Preheat, error)
}

type preheat struct {
	job    *internaljob.Job
	bizTag string
}

type preheatImage struct {
	protocol string
	domain   string
	name     string
	tag      string
}

func newPreheat(job *internaljob.Job, bizTag string) (Preheat, error) {
	return &preheat{
		job:    job,
		bizTag: bizTag,
	}, nil
}

func (p *preheat) GetPreheat(id string) (*types.Preheat, error) {
	groupJobState, err := p.job.GetGroupJobState(id)
	if err != nil {
		return nil, err
	}

	return &types.Preheat{
		ID:        groupJobState.GroupUUID,
		Status:    groupJobState.State,
		CreatedAt: groupJobState.CreatedAt,
	}, nil
}

func (p *preheat) CreatePreheat(schedulers []model.Scheduler, json types.CreatePreheatRequest) (*types.Preheat, error) {
	url := json.URL
	filter := json.Filter
	rawheader := json.Headers

	// Initialize queues
	queues := getSchedulerQueues(schedulers)

	// Generate download files
	var files []*internaljob.PreheatRequest
	switch PreheatType(json.Type) {
	case PreheatImageType:
		// Parse image manifest url
		image, err := parseAccessURL(url)
		if err != nil {
			return nil, err
		}

		files, err = p.getLayers(url, filter, httputils.MapToHeader(rawheader), image)
		if err != nil {
			return nil, err
		}
	case PreheatFileType:
		files = []*internaljob.PreheatRequest{
			{
				URL:     url,
				Tag:     p.bizTag,
				Filter:  filter,
				Headers: rawheader,
			},
		}
	default:
		return nil, errors.New("unknow preheat type")
	}

	for _, f := range files {
		logger.Infof("preheat %s file url: %v queues: %v", json.URL, f.URL, queues)
	}

	return p.createGroupJob(files, queues)
}

func (p *preheat) createGroupJob(files []*internaljob.PreheatRequest, queues []internaljob.Queue) (*types.Preheat, error) {
	signatures := []*machineryv1tasks.Signature{}
	for _, queue := range queues {
		for _, file := range files {
			args, err := internaljob.MarshalRequest(file)
			if err != nil {
				logger.Errorf("preheat marshal request: %v, error: %v", file, err)
				continue
			}

			signatures = append(signatures, &machineryv1tasks.Signature{
				Name:       internaljob.PreheatJob,
				RoutingKey: queue.String(),
				Args:       args,
			})
		}
	}
	group, err := machineryv1tasks.NewGroup(signatures...)

	if err != nil {
		return nil, err
	}

	if _, err := p.job.Server.SendGroup(group, 0); err != nil {
		logger.Error("create preheat group job failed", err)
		return nil, err
	}

	logger.Infof("create preheat group job successed, group uuid: %s， signatures:%+v", group.GroupUUID, signatures)
	return &types.Preheat{
		ID:        group.GroupUUID,
		Status:    machineryv1tasks.StatePending,
		CreatedAt: time.Now(),
	}, nil
}

func (p *preheat) getLayers(url string, filter string, header http.Header, image *preheatImage) ([]*internaljob.PreheatRequest, error) {
	resp, err := p.getManifests(url, header)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		if resp.StatusCode == http.StatusUnauthorized {
			token := getAuthToken(resp.Header)
			bearer := "Bearer " + token
			header.Add("Authorization", bearer)

			resp, err = p.getManifests(url, header)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("request registry %d", resp.StatusCode)
		}
	}

	layers, err := p.parseLayers(resp, url, filter, header, image)
	if err != nil {
		return nil, err
	}

	return layers, nil
}

func (p *preheat) getManifests(url string, header http.Header) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header = header
	req.Header.Add("Accept", schema2.MediaTypeManifest)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (p *preheat) parseLayers(resp *http.Response, url, filter string, header http.Header, image *preheatImage) ([]*internaljob.PreheatRequest, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	manifest, _, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, body)
	if err != nil {
		return nil, err
	}

	var layers []*internaljob.PreheatRequest
	for _, v := range manifest.References() {
		digest := v.Digest.String()
		layer := &internaljob.PreheatRequest{
			URL:     layerURL(image.protocol, image.domain, image.name, digest),
			Tag:     p.bizTag,
			Filter:  filter,
			Digest:  digest,
			Headers: httputils.HeaderToMap(header),
		}

		layers = append(layers, layer)
	}

	return layers, nil
}

func getAuthToken(header http.Header) (token string) {
	authURL := authURL(header.Values("WWW-Authenticate"))
	if len(authURL) == 0 {
		return
	}

	resp, err := http.Get(authURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	if result["token"] != nil {
		token = fmt.Sprintf("%v", result["token"])
	}
	return
}

func authURL(wwwAuth []string) string {
	// Bearer realm="<auth-service-url>",service="<service>",scope="repository:<name>:pull"
	if len(wwwAuth) == 0 {
		return ""
	}
	polished := make([]string, 0)
	for _, it := range wwwAuth {
		polished = append(polished, strings.ReplaceAll(it, "\"", ""))
	}
	fileds := strings.Split(polished[0], ",")
	host := strings.Split(fileds[0], "=")[1]
	query := strings.Join(fileds[1:], "&")
	return fmt.Sprintf("%s?%s", host, query)
}

func layerURL(protocol string, domain string, name string, digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", protocol, domain, name, digest)
}

func parseAccessURL(url string) (*preheatImage, error) {
	r := accessURLPattern.FindStringSubmatch(url)
	if len(r) != 5 {
		return nil, errors.New("parse access url failed")
	}

	return &preheatImage{
		protocol: r[1],
		domain:   r[2],
		name:     r[3],
		tag:      r[4],
	}, nil
}

func getSchedulerQueues(schedulers []model.Scheduler) []internaljob.Queue {
	var queues []internaljob.Queue
	for _, scheduler := range schedulers {
		queue, err := internaljob.GetSchedulerQueue(scheduler.SchedulerClusterID, scheduler.HostName)
		if err != nil {
			continue
		}

		queues = append(queues, queue)
	}

	return queues
}
