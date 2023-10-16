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
	"context"
	"crypto/rand"
	"encoding/base64"

	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
	oauth2github "golang.org/x/oauth2/github"
)

var githubScopes = []string{
	"user",
	"public_repo",
}

type oauthGithub struct {
	*oauth2.Config
}

func newGithub(clientID, clientSecret, redirectURL string) *oauthGithub {
	return &oauthGithub{
		Config: &oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       githubScopes,
			Endpoint:     oauth2github.Endpoint,
			RedirectURL:  redirectURL,
		},
	}
}

func (g *oauthGithub) AuthCodeURL() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return g.Config.AuthCodeURL(base64.URLEncoding.EncodeToString(b)), nil
}

func (g *oauthGithub) Exchange(code string) (*oauth2.Token, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return g.Config.Exchange(ctx, code)
}

func (g *oauthGithub) GetUser(token *oauth2.Token) (*User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client := github.NewClient(g.Client(ctx, token))
	user, _, err := client.Users.Get(ctx, "")
	if err != nil {
		return nil, err
	}

	return &User{
		Name:   *user.Name,
		Email:  *user.Email,
		Avatar: *user.AvatarURL,
	}, nil
}
