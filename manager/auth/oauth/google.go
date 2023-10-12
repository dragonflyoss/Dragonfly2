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

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	oauth2v2 "google.golang.org/api/oauth2/v2"
	"google.golang.org/api/option"
)

var googleScopes = []string{
	"https://www.googleapis.com/auth/userinfo.email",
	"https://www.googleapis.com/auth/userinfo.profile",
}

type oauthGoogle struct {
	*oauth2.Config
}

func newGoogle(clientID, clientSecret, redirectURL string) *oauthGoogle {
	return &oauthGoogle{
		Config: &oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       googleScopes,
			Endpoint:     google.Endpoint,
			RedirectURL:  redirectURL,
		},
	}
}

func (g *oauthGoogle) AuthCodeURL() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return g.Config.AuthCodeURL(base64.URLEncoding.EncodeToString(b)), nil
}

func (g *oauthGoogle) Exchange(code string) (*oauth2.Token, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return g.Config.Exchange(ctx, code)
}

func (g *oauthGoogle) GetUser(token *oauth2.Token) (*User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := oauth2v2.NewService(ctx, option.WithTokenSource(g.Config.TokenSource(ctx, token)))
	if err != nil {
		return nil, err
	}

	user, err := client.Userinfo.Get().Do()
	if err != nil {
		return nil, err
	}

	return &User{
		Name:   user.Name,
		Email:  user.Email,
		Avatar: user.Picture,
	}, nil
}
