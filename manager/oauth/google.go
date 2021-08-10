package oauth
import (
	"golang.org/x/oauth2/google"
)

type googleOauth2 struct {
	oauth2
}

func NewGoogle(name string, clientID string, clientSecret string, scopes string, authURL string, tokenURL string, db *gorm.DB) (*oauth, error) {

	oa := &googleOauth2{
		Name: name,
		Config: &oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       strings.Split(scopes, ","),
			Endpoint: google. 
		},
	}
	redirectURL, err := oa.GetRediectURL(db)
	if err != nil {
		return nil, err
	}
	oa.Config.RedirectURL = redirectURL
	return oa, nil
}