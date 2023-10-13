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

//go:generate mockgen -destination mocks/oauth_mock.go -source oauth.go -package mocks

package oauth

import (
	"errors"
	"time"

	"golang.org/x/oauth2"
)

const (
	timeout = 2 * time.Minute
)

const (
	Google = "google"
	Github = "github"
)

type User struct {
	Name   string
	Email  string
	Avatar string
}

type Oauth interface {
	AuthCodeURL() (string, error)
	Exchange(string) (*oauth2.Token, error)
	GetUser(*oauth2.Token) (*User, error)
}

func New(name, clientID, clientSecret, redirectURL string) (Oauth, error) {
	var o Oauth
	switch name {
	case Google:
		o = newGoogle(clientID, clientSecret, redirectURL)
	case Github:
		o = newGithub(clientID, clientSecret, redirectURL)
	default:
		return nil, errors.New("invalid oauth name")
	}

	return o, nil
}
