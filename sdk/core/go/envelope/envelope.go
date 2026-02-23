package envelope

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/relaymesh/relaybus/sdk/core/go/errdefs"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

type envelopeV1 struct {
	V           string            `json:"v"`
	ID          string            `json:"id"`
	Topic       string            `json:"topic"`
	Timestamp   string            `json:"ts"`
	ContentType string            `json:"content_type"`
	PayloadB64  string            `json:"payload_b64"`
	Meta        map[string]string `json:"meta"`
}

func EncodeEnvelope(msg message.Message) ([]byte, error) {
	if msg.SchemaVersion == "" {
		return nil, fmt.Errorf("%w: schema version required", errdefs.ErrInvalidEnvelope)
	}
	if msg.ID == "" {
		return nil, fmt.Errorf("%w: id required", errdefs.ErrInvalidEnvelope)
	}
	if msg.Topic == "" {
		return nil, fmt.Errorf("%w: topic required", errdefs.ErrInvalidEnvelope)
	}
	if msg.Timestamp.IsZero() {
		return nil, fmt.Errorf("%w: timestamp required", errdefs.ErrInvalidEnvelope)
	}
	if msg.ContentType == "" {
		return nil, fmt.Errorf("%w: content_type required", errdefs.ErrInvalidEnvelope)
	}
	if msg.Metadata == nil {
		return nil, fmt.Errorf("%w: meta required", errdefs.ErrInvalidEnvelope)
	}

	env := envelopeV1{
		V:           msg.SchemaVersion,
		ID:          msg.ID,
		Topic:       msg.Topic,
		Timestamp:   msg.Timestamp.UTC().Format(time.RFC3339Nano),
		ContentType: msg.ContentType,
		PayloadB64:  base64.StdEncoding.EncodeToString(msg.Payload),
		Meta:        msg.Metadata,
	}

	data, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal envelope: %v", errdefs.ErrInvalidEnvelope, err)
	}

	return data, nil
}

func DecodeEnvelope(data []byte) (message.Message, error) {
	var env envelopeV1
	if err := validateEnvelopeJSON(data, &env); err != nil {
		return message.Message{}, err
	}

	payload, err := base64.StdEncoding.DecodeString(env.PayloadB64)
	if err != nil {
		return message.Message{}, fmt.Errorf("%w: invalid payload_b64: %v", errdefs.ErrInvalidEnvelope, err)
	}

	ts, err := time.Parse(time.RFC3339Nano, env.Timestamp)
	if err != nil {
		return message.Message{}, fmt.Errorf("%w: invalid ts: %v", errdefs.ErrInvalidEnvelope, err)
	}

	msg := message.Message{
		ID:            env.ID,
		Topic:         env.Topic,
		Timestamp:     ts,
		ContentType:   env.ContentType,
		Payload:       payload,
		Metadata:      env.Meta,
		SchemaVersion: env.V,
	}

	return msg, nil
}

func ValidateEnvelopeJSON(data []byte) error {
	return validateEnvelopeJSON(data, nil)
}

func validateEnvelopeJSON(data []byte, dest *envelopeV1) error {
	var env envelopeV1
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("%w: invalid json: %v", errdefs.ErrInvalidEnvelope, err)
	}

	if env.V != "v1" {
		return fmt.Errorf("%w: v must be v1", errdefs.ErrInvalidEnvelope)
	}
	if env.ID == "" {
		return fmt.Errorf("%w: id required", errdefs.ErrInvalidEnvelope)
	}
	if env.Topic == "" {
		return fmt.Errorf("%w: topic required", errdefs.ErrInvalidEnvelope)
	}
	if env.Timestamp == "" {
		return fmt.Errorf("%w: ts required", errdefs.ErrInvalidEnvelope)
	}
	if _, err := time.Parse(time.RFC3339Nano, env.Timestamp); err != nil {
		return fmt.Errorf("%w: invalid ts: %v", errdefs.ErrInvalidEnvelope, err)
	}
	if env.ContentType == "" {
		return fmt.Errorf("%w: content_type required", errdefs.ErrInvalidEnvelope)
	}
	if env.Meta == nil {
		return fmt.Errorf("%w: meta required", errdefs.ErrInvalidEnvelope)
	}
	if _, err := base64.StdEncoding.DecodeString(env.PayloadB64); err != nil {
		return fmt.Errorf("%w: invalid payload_b64: %v", errdefs.ErrInvalidEnvelope, err)
	}

	if dest != nil {
		*dest = env
	}
	return nil
}
