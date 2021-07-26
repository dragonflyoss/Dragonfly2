package tasks

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	schedulertasks "d7y.io/dragonfly/v2/scheduler/tasks"
)

type Preheat interface {
	getLayers(url string, header map[string]string, retryIfUnAuth bool) (layers []*schedulertasks.PreheatRequest, err error)
}

type preheat struct {
	URL     string
	Tag     string
	Headers map[string]string
}

func (p *preheat) getLayers(url string, header map[string]string, retryIfUnAuth bool) (layers []*schedulertasks.PreheatRequest, err error) {
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
			token := getAuthToken(resp.Header)
			if token != "" {
				authHeader := map[string]string{"Authorization": "Bearer " + token}
				return getLayers(url, authHeader, false)
			}
		}
		err = fmt.Errorf("%s %s", resp.Status, string(body))
		return
	}

	layers = w.parseLayers(body, header)
	return
}

func getAuthToken(header http.Header) (token string) {
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
	authUrl := authUrl(values)
	if len(authUrl) == 0 {
		return
	}
	resp, err := http.Get(authUrl)
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

func authUrl(wwwAuth []string) string {
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

func parseLayers(body []byte, header map[string]string) (layers []*schedulertasks.PreheatRequest) {
	var meta = make(map[string]interface{})
	json.Unmarshal(body, &meta)
	schemaVersion := fmt.Sprintf("%v", meta["schemaVersion"])
	var layerDigest []string
	if schemaVersion == "1" {
		layerDigest = w.parseLayerDigest(meta, "fsLayers", "blobSum")
	} else {
		mediaType := fmt.Sprintf("%s", meta["mediaType"])
		switch mediaType {
		case "application/vnd.docker.distribution.manifest.list.v2+json", "application/vnd.oci.image.index.v1+json":
			manifestDigest := w.parseLayerDigest(meta, "manifests", "digest")
			for _, digest := range manifestDigest {
				list, _ := w.getLayers(w.manifestUrl(digest), header, false)
				layers = append(layers, list...)
			}
			return
		default:
			layerDigest = w.parseLayerDigest(meta, "layers", "digest")
		}
	}

	for _, digest := range layerDigest {
		layers = append(layers, &schedulertasks.PreheatRequest{
			Digest:  digest,
			URL:     w.layerUrl(digest),
			Headers: header,
		})
	}

	return
}

// TODO preheats
// func (t *task) preheats(hostname string, files []PreHeatFile) error {
// signatures := []*machinerytasks.Signature{}
// for _, v := range files {
// if err != nil {
// return err
// }

// signatures = append(signatures, &machinerytasks.Signature{
// Name:       internaltasks.PreheatTask,
// RoutingKey: internaltasks.GetSchedulerQueue(hostname).String(),
// Args: internaltasks.Marshal(),
// })
// }

// group, _ := machinerytasks.NewGroup(signatures...)
// _, err := t.Server.SendGroup(group, 0)
// return err
// }
