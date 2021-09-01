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
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"gorm.io/gorm"
)

type googleOauth2 struct {
	baseOauth2
}

func NewGoogleOauth2(name string, clientID string, clientSecret string, db *gorm.DB) (Oauther, error) {

	oa := &googleOauth2{}
	oa.Name = name
	oa.Config = &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Scopes:       strings.Split(GoogleScopes, ","),
		Endpoint:     google.Endpoint,
	}
	oa.UserInfoURL = GithubUserInfoURL

	redirectURL, err := oa.GetRediectURL(db)
	if err != nil {
		return nil, err
	}
	oa.Config.RedirectURL = redirectURL
	return oa, nil
}
