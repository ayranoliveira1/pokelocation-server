package push

import (
	"context"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

func NewSender(ctx context.Context, credPath string) (Sender, error) {
	if credPath == "" {
		return logSender{}, nil
	}
	app, err := firebase.NewApp(ctx, nil, option.WithCredentialsFile(credPath))
	if err != nil {
		return nil, err
	}
	c, err := app.Messaging(ctx)
	if err != nil {
		return nil, err
	}
	return fcmSender{c}, nil
}

type fcmSender struct {
	c *messaging.Client
}

func (f fcmSender) Send(ctx context.Context, _, token string) error {
	_, err := f.c.Send(ctx, &messaging.Message{
		Token:   token,
		Data:    map[string]string{"t": "m"},
		Android: &messaging.AndroidConfig{Priority: "high", CollapseKey: "m"},
	})
	// InvalidArgument pode ser erro nosso de formato: tratá-lo como token inválido apagaria todos os tokens.
	if messaging.IsUnregistered(err) || messaging.IsSenderIDMismatch(err) {
		return ErrInvalidToken
	}
	return err
}
