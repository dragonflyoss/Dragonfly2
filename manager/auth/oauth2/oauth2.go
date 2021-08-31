package oauth2

import (
	"context"
	"errors"

	xoauth2 "golang.org/x/oauth2"
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
	Exchange(context.Context, string) (*xoauth2.Token, error)
	GetUser(context.Context, *xoauth2.Token) (*User, error)
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

func (g *oauth2) Exchange(ctx context.Context, code string) (*xoauth2.Token, error) {
	return g.Oauth2.Exchange(ctx, code)
}

func (g *oauth2) GetUser(ctx context.Context, token *xoauth2.Token) (*User, error) {
	return g.Oauth2.GetUser(ctx, token)
}
