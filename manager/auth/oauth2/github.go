package oauth2

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

type oauth2Github struct {
	*oauth2.Config
}

func newGithub(name, clientID, clientSecret, redirectURL string) *oauth2Github {
	return &oauth2Github{
		Config: &oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       githubScopes,
			Endpoint:     oauth2github.Endpoint,
			RedirectURL:  redirectURL,
		},
	}
}

func (g *oauth2Github) AuthCodeURL() string {
	b := make([]byte, 16)
	rand.Read(b)
	return g.Config.AuthCodeURL(base64.URLEncoding.EncodeToString(b))
}

func (g *oauth2Github) Exchange(ctx context.Context, code string) (*oauth2.Token, error) {
	return g.Config.Exchange(ctx, code)
}

func (g *oauth2Github) GetUser(ctx context.Context, token *oauth2.Token) (*User, error) {
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
