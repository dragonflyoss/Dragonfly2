package oauth2

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

type oauth2Google struct {
	*oauth2.Config
}

func newGoogle(name, clientID, clientSecret, redirectURL string) *oauth2Google {
	return &oauth2Google{
		Config: &oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       googleScopes,
			Endpoint:     google.Endpoint,
			RedirectURL:  redirectURL,
		},
	}
}

func (g *oauth2Google) AuthCodeURL() string {
	b := make([]byte, 16)
	rand.Read(b)
	return g.Config.AuthCodeURL(base64.URLEncoding.EncodeToString(b))
}

func (g *oauth2Google) Exchange(ctx context.Context, code string) (*oauth2.Token, error) {
	return g.Config.Exchange(ctx, code)
}

func (g *oauth2Google) GetUser(ctx context.Context, token *oauth2.Token) (*User, error) {
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
