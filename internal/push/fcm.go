package push

import (
	"context"
	"errors"

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
	return fcmErr(err)
}

// SendData: até 500 mensagens, enviadas em paralelo pela lib.
func (f fcmSender) SendData(ctx context.Context, _, token string, msgs []Data) error {
	ms := make([]*messaging.Message, len(msgs))
	for i, d := range msgs {
		ttl := d.TTL
		ms[i] = &messaging.Message{Token: token, Data: d.Fields,
			Android: &messaging.AndroidConfig{Priority: "high", TTL: &ttl}}
	}
	br, err := f.c.SendEach(ctx, ms)
	if err != nil {
		return fcmErr(err)
	}
	var failed FailedMsgs
	for i, r := range br.Responses {
		if r.Error != nil {
			failed.Idx = append(failed.Idx, i)
			failed.Err = errors.Join(failed.Err, fcmErr(r.Error))
		}
	}
	if failed.Err == nil {
		return nil
	}
	return &failed
}

// InvalidArgument pode ser erro nosso de formato: tratá-lo como token inválido apagaria todos os tokens.
func fcmErr(err error) error {
	if messaging.IsUnregistered(err) || messaging.IsSenderIDMismatch(err) {
		return ErrInvalidToken
	}
	return err
}
