package tasks

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"

	internaltasks "d7y.io/dragonfly/v2/internal/tasks"
	"d7y.io/dragonfly/v2/manager/types"
	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
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
	headers     map[string]string
	tasks       *internaltasks.Tasks
	queues      []internaltasks.Queue
	protocol    string
	domain      string
	name        string
}

func newPreheat(tasks *internaltasks.Tasks, hostnames []string, preheatType PreheatType, url, filter, tag string, headers map[string]string) Preheat {
	p := &preheat{
		tasks:       tasks,
		preheatType: preheatType,
		url:         url,
		filter:      filter,
		tag:         tag,
		headers:     headers,
	}

	for _, hostname := range hostnames {
		p.queues = append(p.queues, internaltasks.GetSchedulerQueue(hostname))
	}

	return p
}

func (p *preheat) CreatePreheat() (*types.Preheat, error) {
	if p.preheatType == PreheatImageType {
		result := imageManifestsPattern.FindSubmatch([]byte(p.url))
		if len(result) == 5 {
			p.protocol = string(result[1])
			p.domain = string(result[2])
			p.name = string(result[3])
		}

		layers, err := p.getLayers(p.url, p.headers, true)
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
			Headers: p.headers,
		},
	})
}

func (p *preheat) createPreheat(files []*internaltasks.PreheatRequest) (*types.Preheat, error) {
	signatures := []*machineryv1tasks.Signature{}
	for _, queue := range p.queues {
		for _, file := range files {
			args, err := internaltasks.Marshal(file)
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

func (p *preheat) getLayers(url string, header map[string]string, retryIfUnAuth bool) (layers []*internaltasks.PreheatRequest, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	for k, v := range header {
		req.Header.Add(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		if retryIfUnAuth {
			token := p.getAuthToken(resp.Header)
			if token != "" {
				authHeader := map[string]string{"Authorization": "Bearer " + token}
				return p.getLayers(url, authHeader, false)
			}
		}
		err = fmt.Errorf("%s %s", resp.Status, string(body))
		return
	}

	layers = p.parseLayers(body, header)
	return
}

func (p *preheat) getAuthToken(header http.Header) (token string) {
	if len(header) == 0 {
		return
	}
	var values []string
	for k, v := range header {
		if strings.ToLower(k) == "www-authenticate" {
			values = v
		}
	}
	if values == nil {
		return
	}
	authURL := p.authURL(values)
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

func (p *preheat) parseLayers(body []byte, header map[string]string) (layers []*internaltasks.PreheatRequest) {
	var meta = make(map[string]interface{})
	json.Unmarshal(body, &meta)
	schemaVersion := fmt.Sprintf("%v", meta["schemaVersion"])
	var layerDigest []string
	if schemaVersion == "1" {
		layerDigest = p.parseLayerDigest(meta, "fsLayers", "blobSum")
	} else {
		mediaType := fmt.Sprintf("%s", meta["mediaType"])
		switch mediaType {
		case "application/vnd.docker.distribution.manifest.list.v2+json", "application/vnd.oci.image.index.v1+json":
			manifestDigest := p.parseLayerDigest(meta, "manifests", "digest")
			for _, digest := range manifestDigest {
				list, _ := p.getLayers(p.manifestURL(digest), header, false)
				layers = append(layers, list...)
			}
			return
		default:
			layerDigest = p.parseLayerDigest(meta, "layers", "digest")
		}
	}

	for _, digest := range layerDigest {
		layers = append(layers, &internaltasks.PreheatRequest{
			URL:     p.layerURL(digest),
			Tag:     p.tag,
			Filter:  p.filter,
			Digest:  digest,
			Headers: header,
		})
	}

	return
}

func (p *preheat) parseLayerDigest(meta map[string]interface{}, layerKey string, digestKey string) (layers []string) {
	data := meta[layerKey]
	if data == nil {
		return
	}
	array, _ := data.([]interface{})
	if array == nil {
		return
	}
	for _, it := range array {
		layer, _ := it.(map[string]interface{})
		if layer != nil {
			layers = append(layers, fmt.Sprintf("%v", layer[digestKey]))
		}
	}
	return layers
}

func (p *preheat) manifestURL(digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/manifests/%s", p.protocol, p.domain, p.name, digest)
}

func (p *preheat) layerURL(digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", p.protocol, p.domain, p.name, digest)
}
