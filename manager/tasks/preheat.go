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

var accessURLPattern, _ = regexp.Compile("^(.*)://(.*)/v2/(.*)/manifests/(.*)")

type Preheat interface {
	CreatePreheat(hostnames []string, json types.CreatePreheatRequest) (*types.Preheat, error)
}

type preheat struct {
	tasks  *internaltasks.Tasks
	bizTag string
}

type preheatImage struct {
	protocol string
	domain   string
	name     string
	tag      string
}

func newPreheat(tasks *internaltasks.Tasks, bizTag string) (Preheat, error) {
	return &preheat{
		tasks:  tasks,
		bizTag: bizTag,
	}, nil
}

func (p *preheat) CreatePreheat(hostnames []string, json types.CreatePreheatRequest) (*types.Preheat, error) {
	url := json.URL
	filter := json.Filter
	rawheader := json.Headers

	// Initialize queues
	queues := getSchedulerQueues(hostnames)

	// Generate download files
	var files []*internaltasks.PreheatRequest
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
		files = []*internaltasks.PreheatRequest{
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

	return p.createGroupTasks(files, queues)
}

func (p *preheat) createGroupTasks(files []*internaltasks.PreheatRequest, queues []internaltasks.Queue) (*types.Preheat, error) {
	signatures := []*machineryv1tasks.Signature{}
	for _, queue := range queues {
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

func (p *preheat) getLayers(url string, filter string, header http.Header, image *preheatImage) ([]*internaltasks.PreheatRequest, error) {
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

	layers, err := p.parseLayers(resp, filter, header, image)
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

func (p *preheat) parseLayers(resp *http.Response, filter string, header http.Header, image *preheatImage) ([]*internaltasks.PreheatRequest, error) {
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
			URL:     layerURL(image.protocol, image.domain, image.name, digest),
			Tag:     p.bizTag,
			Filter:  filter,
			Digest:  digest,
			Headers: httputils.HeaderToMap(header),
		})
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

func getSchedulerQueues(hostnames []string) []internaltasks.Queue {
	var queues []internaltasks.Queue
	for _, hostname := range hostnames {
		queue, err := internaltasks.GetSchedulerQueue(hostname)
		if err != nil {
			continue
		}

		queues = append(queues, queue)
	}

	return queues
}
