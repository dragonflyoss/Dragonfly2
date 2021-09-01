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

package oauth

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/github"
	"gorm.io/gorm"
)

type githubOauth2 struct {
	baseOauth2
}

func NewGithubOauth2(name string, clientID string, clientSecret string, db *gorm.DB) (Oauther, error) {

	oa := &githubOauth2{}
	oa.Name = name
	oa.Config = &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Scopes:       strings.Split(GithubScopes, ","),
		Endpoint:     github.Endpoint,
	}
	oa.UserInfoURL = GithubUserInfoURL

	redirectURL, err := oa.GetRediectURL(db)
	if err != nil {
		return nil, err
	}
	oa.Config.RedirectURL = redirectURL
	return oa, nil
}

func (oa *githubOauth2) GetOauthUserInfo(token string) (*oauth2User, error) {
	req, err := http.NewRequest("GET", oa.UserInfoURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "token"+" "+token)
	response, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	u := oauth2User{}
	err = json.Unmarshal(contents, &u)
	if err != nil {
		return nil, err
	}
	return &u, nil
}
