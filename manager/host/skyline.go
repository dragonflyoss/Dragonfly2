package host

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/manager/config"
	"go.uber.org/multierr"
	"gopkg.in/errgo.v2/fmt/errors"
)

type QueryRequest struct {
	From      string `json:"from"`
	Select    string `json:"select"`
	Condition string `json:"condition"`
	Page      int    `json:"page"`
	Num       int    `json:"num"`
	NeedTotal bool   `json:"needTotal"`
}

func NewQueryRequest() *QueryRequest {
	return &QueryRequest{
		Page:      1,
		NeedTotal: false,
	}
}

func (req *QueryRequest) SetNum(num int) {
	req.Page = 1
	req.Num = num
}

func (req *QueryRequest) SetSelect(sel string) {
	req.Select = sel
}

func (req *QueryRequest) SetFrom(from string) {
	req.From = from
}

func (req *QueryRequest) SetCondition(cond string) {
	req.Condition = cond
}

type Item struct {
	Ip                 string   `json:"ip"`
	Sn                 string   `json:"sn"`
	HostName           string   `json:"host_name"`
	VpcId              string   `json:"vpc_id"`
	SecurityDomain     string   `json:"security_domain"`
	IdcAbbreviation    string   `json:"idc.abbreviation"`
	IdcCountry         string   `json:"idc.country"`
	IdcArea            string   `json:"idc.area"`
	IdcProvince        string   `json:"idc.province"`
	IdcCity            string   `json:"idc.city"`
	RoomAbbreviation   string   `json:"room.abbreviation"`
	CabinetCabinetNum  string   `json:"cabinet.cabinet_num"`
	CabinetLogicRegion string   `json:"cabinet.logic_region"`
	DswClusterName     string   `json:"dsw_cluster.name"`
	NetAswName         string   `json:"net_asw.name"`
	LogicPodName       string   `json:"logic_pod.name"`
	NetPodName         string   `json:"net_pod.name"`
	Tags               []string `json:"tags"`
}

type Value struct {
	TotalCount int     `json:"totalCount"`
	HasMore    bool    `json:"hasMore"`
	ItemList   []*Item `json:"itemList"`
}

type QueryResponse struct {
	Success      bool   `json:"success"`
	Value        *Value `json:"value"`
	ErrorCode    int    `json:"errorCode"`
	ErrorCodeMsg string `json:"errorCodeMsg"`
	ErrorMessage string `json:"errorMessage"`
	AllowRetry   bool   `json:"allowRetry"`
}

type Auth struct {
	AppName   string `json:"appName"`
	Account   string `json:"account"`
	Timestamp int64  `json:"timestamp"`
	Signature string `json:"signature"`
}

type skylineClient struct {
	domain    string
	appName   string
	account   string
	accessKey string
}

type ItemSearchRequest struct {
	Auth *Auth         `json:"auth"`
	Item *QueryRequest `json:"item"`
}

func (client *skylineClient) getSignature(timestamp string) string {
	str := client.account + client.accessKey + timestamp
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}

func (client *skylineClient) itemSearch(req *QueryRequest) (*QueryResponse, error) {
	now := time.Now().Unix()
	body, err := json.Marshal(&ItemSearchRequest{
		Auth: &Auth{
			AppName:   client.appName,
			Account:   client.account,
			Timestamp: now,
			Signature: client.getSignature(strconv.FormatInt(now, 10)),
		},
		Item: req,
	})

	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/item/query", client.domain)
	request, err := http.NewRequest("POST", url, bytes.NewReader(body))
	defer request.Body.Close()
	if err != nil {
		return nil, err
	}

	request.Header.Set("Content-Type", "application/json;charset=UTF-8")
	request.Header.Set("account", client.account)

	cli := &http.Client{}
	response, err := cli.Do(request)
	if err != nil {
		return nil, err
	}

	ret, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	resp := &QueryResponse{}
	err = json.Unmarshal(ret, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

type skyline struct {
	client *skylineClient
}

func NewSkyline(config *config.SkylineService) (HostManager, error) {
	return &skyline{
		client: &skylineClient{
			domain:    config.Domain,
			appName:   config.AppName,
			account:   config.Account,
			accessKey: config.AccessKey,
		}}, nil
}

var SelectLists = []string{
	"ip", "sn", "tags", "host_name", "security_domain", "vpc_id",
	"idc.abbreviation", "idc.country", "idc.area", "idc.province", "idc.city",
	"room.abbreviation", "cabinet.cabinet_num", "cabinet.logic_region",
	"dsw_cluster.name", "net_asw.name", "logic_pod.name", "net_pod.name",
}

func (skyline *skyline) GetHostInfo(ctx context.Context, opts ...OpOption) (*HostInfo, error) {
	op := Op{}
	op.ApplyOpts(opts)

	var conditions []string
	if len(op.sn) > 0 {
		conditions = append(conditions, fmt.Sprintf("sn = '%s'", op.sn))
	}

	if len(op.ip) > 0 {
		conditions = append(conditions, fmt.Sprintf("ip = '%s'", op.ip))
	}

	if len(op.hostName) > 0 {
		conditions = append(conditions, fmt.Sprintf("host_name = '%s'", op.hostName))
	}

	req := NewQueryRequest()
	req.SetNum(1)
	req.SetSelect(strings.Join(SelectLists, ","))
	req.SetFrom("server")

	var mutErr error
	for _, cond := range conditions {
		var r = *req
		r.SetCondition(cond)
		resp, err := skyline.client.itemSearch(&r)
		if err != nil {
			mutErr = multierr.Append(mutErr, err)
			continue
		}

		if !resp.Success {
			mutErr = multierr.Append(mutErr, errors.Newf("ErrorCode: %d, ErrorCodeMsg: %s, ErrorMessage: %s", resp.ErrorCode, resp.ErrorCodeMsg, resp.ErrorMessage))
			continue
		}

		if resp.Value.TotalCount == 0 {
			mutErr = multierr.Append(mutErr, errors.Newf("TotalCount zero, condition: %s", cond))
			continue
		}

		item := resp.Value.ItemList[0]
		location := fmt.Sprintf("%s|%s|%s|%s", item.IdcArea, item.IdcCountry, item.IdcProvince, item.IdcCity)
		netTopology := fmt.Sprintf("%s|%s", item.CabinetLogicRegion, item.CabinetCabinetNum)
		return &HostInfo{
			HostName:       item.HostName,
			Ip:             item.Ip,
			SecurityDomain: item.SecurityDomain,
			Location:       location,
			Idc:            item.IdcAbbreviation,
			NetTopology:    netTopology,
		}, nil
	}

	return nil, mutErr
}
