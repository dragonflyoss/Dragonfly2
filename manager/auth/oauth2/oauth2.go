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

package oauth2

import (
	"errors"
	"time"

	xoauth2 "golang.org/x/oauth2"
)

const (
	timeout = 5 * time.Second
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

type Oauth2 interface {
	AuthCodeURL() string
	Exchange(string) (*xoauth2.Token, error)
	GetUser(*xoauth2.Token) (*User, error)
}

type oauth2 struct {
	Oauth2 Oauth2
}

func New(name, clientID, clientSecret, redirectURL string) (Oauth2, error) {
	var o Oauth2
	switch name {
	case Google:
		o = newGoogle(name, clientID, clientSecret, redirectURL)
	case Github:
		o = newGithub(name, clientID, clientSecret, redirectURL)
	default:
		return nil, errors.New("invalid oauth name")
	}

	return o, nil
}

func (g *oauth2) AuthCodeURL() string {
	return g.Oauth2.AuthCodeURL()
}

func (g *oauth2) Exchange(code string) (*xoauth2.Token, error) {
	return g.Oauth2.Exchange(code)
}

func (g *oauth2) GetUser(token *xoauth2.Token) (*User, error) {
	return g.Oauth2.GetUser(token)
}
