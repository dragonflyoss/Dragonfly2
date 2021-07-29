package tasks

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"

	internaltasks "d7y.io/dragonfly/v2/internal/tasks"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/util/net/httputils"
	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/schema2"
)

type PreheatType string

const (
	PreheatImageType PreheatType = "image"
	PreheatFileType  PreheatType = "file"
)

var imageManifestsPattern, _ = regexp.Compile("^(.*)://(.*)/v2/(.*)/manifests/(.*)")

type Preheat interface {
	CreatePreheat() (*types.Preheat, error)
}

type preheat struct {
	preheatType PreheatType
	url         string
	filter      string
	tag         string
	header      http.Header
	tasks       *internaltasks.Tasks
	queues      []internaltasks.Queue
	protocol    string
	domain      string
	name        string
}

func newPreheat(tasks *internaltasks.Tasks, hostnames []string, preheatType PreheatType, url, filter, tag string, rawHeader map[string]string) Preheat {
	p := &preheat{
		tasks:       tasks,
		preheatType: preheatType,
		url:         url,
		filter:      filter,
		tag:         tag,
		header:      httputils.MapToHeader(rawHeader),
	}

	for _, hostname := range hostnames {
		queue, err := internaltasks.GetSchedulerQueue(hostname)
		if err != nil {
			continue
		}

		p.queues = append(p.queues, queue)
	}

	result := imageManifestsPattern.FindSubmatch([]byte(p.url))
	if len(result) == 5 {
		p.protocol = string(result[1])
		p.domain = string(result[2])
		p.name = string(result[3])
	}

	return p
}

func (p *preheat) CreatePreheat() (*types.Preheat, error) {
	if p.preheatType == PreheatImageType {
		layers, err := p.getLayers(p.url, p.header)
		if err != nil {
			return nil, err
		}

		return p.createPreheat(layers)
	}

	return p.createPreheat([]*internaltasks.PreheatRequest{
		{
			URL:     p.url,
			Tag:     p.tag,
			Filter:  p.filter,
			Headers: httputils.HeaderToMap(p.header),
		},
	})
}

func (p *preheat) createPreheat(files []*internaltasks.PreheatRequest) (*types.Preheat, error) {
	signatures := []*machineryv1tasks.Signature{}
	for _, queue := range p.queues {
		for _, file := range files {
			args, err := internaltasks.MarshalRequest(file)
			if err != nil {
				continue
			}

			signatures = append(signatures, &machineryv1tasks.Signature{
				Name:       internaltasks.PreheatTask,
				RoutingKey: queue.String(),
				Args:       args,
			})
		}
	}

	group, err := machineryv1tasks.NewGroup(signatures...)
	if err != nil {
		return nil, err
	}

	if _, err := p.tasks.Server.SendGroup(group, 0); err != nil {
		return nil, err
	}

	return &types.Preheat{
		ID:        group.GroupUUID,
		Status:    machineryv1tasks.StatePending,
		CreatedAt: time.Now(),
	}, nil
}

func (p *preheat) getLayers(url string, header http.Header) ([]*internaltasks.PreheatRequest, error) {
	resp, err := p.getManifests(url, header)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		if resp.StatusCode == http.StatusUnauthorized {
			token := p.getAuthToken(resp.Header)
			bearer := "Bearer " + token
			header.Add("Authorization", bearer)

			resp, err = p.getManifests(url, header)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.New(fmt.Sprintf("request registry %d", resp.StatusCode))
		}
	}

	layers, err := p.parseLayers(resp, header)
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

func (p *preheat) parseLayers(resp *http.Response, header http.Header) ([]*internaltasks.PreheatRequest, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	manifest, _, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, body)
	if err != nil {
		return nil, err
	}

	var layers []*internaltasks.PreheatRequest
	for _, v := range manifest.References() {
		digest := v.Digest.String()
		layers = append(layers, &internaltasks.PreheatRequest{
			URL:     p.layerURL(digest),
			Tag:     p.tag,
			Filter:  p.filter,
			Digest:  digest,
			Headers: httputils.HeaderToMap(header),
		})
	}

	return layers, nil
}

func (p *preheat) getAuthToken(header http.Header) (token string) {
	authURL := p.authURL(header.Values("WWW-Authenticate"))
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

func (p *preheat) authURL(wwwAuth []string) string {
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

func (p *preheat) manifestURL(digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/manifests/%s", p.protocol, p.domain, p.name, digest)
}

func (p *preheat) layerURL(digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", p.protocol, p.domain, p.name, digest)
}
