package push

import (
	"context"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

// NewSender: credPath vazio = envio falso (só loga); senão o FCM de verdade com a conta de serviço do JSON.
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

// Send: data message de alta prioridade, sem conteúdo; a collapse key junta avisos ainda não entregues num só.
func (f fcmSender) Send(ctx context.Context, _, token string) error {
	_, err := f.c.Send(ctx, &messaging.Message{
		Token:   token,
		Data:    map[string]string{"t": "m"},
		Android: &messaging.AndroidConfig{Priority: "high", CollapseKey: "m"},
	})
	// Só estes dizem que o token não serve; InvalidArgument pode ser erro nosso de formato e apagaria todos os tokens.
	if messaging.IsUnregistered(err) || messaging.IsSenderIDMismatch(err) {
		return ErrInvalidToken
	}
	return err
}
