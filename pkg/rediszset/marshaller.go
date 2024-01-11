package rediszset

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
	"strings"
)

const UUIDHeaderKey = "_watermill_message_uuid"

type Marshaller interface {
	Marshal(topic string, msg *message.Message) ([]byte, error)
}

type Unmarshaller interface {
	Unmarshal(values []byte) (msg *message.Message, err error)
}

type MarshallerUnmarshaller interface {
	Marshaller
	Unmarshaller
}

type DefaultMarshallerUnmarshaller struct{}

func (DefaultMarshallerUnmarshaller) Marshal(_ string, msg *message.Message) ([]byte, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}

	var (
		metadata []byte
		err      error
	)
	if msg.Metadata != nil {
		if metadata, err = msgpack.Marshal(msg.Metadata); err != nil {
			return nil, err
		}
	}

	data := map[string]interface{}{
		UUIDHeaderKey: msg.UUID,
		"metadata":    metadata,
		"payload":     msg.Payload,
	}
	return msgpack.Marshal(data)
}

func (DefaultMarshallerUnmarshaller) Unmarshal(values []byte) (msg *message.Message, err error) {
	data := make(map[string]interface{})
	if err = msgpack.Unmarshal(values, &data); err != nil {
		return
	}

	msg = message.NewMessage(data[UUIDHeaderKey].(string), data["payload"].([]uint8))

	md := data["metadata"]
	if md != nil {
		s := md.([]uint8)
		if len(s) > 0 {
			metadata := make(message.Metadata)
			if err = msgpack.Unmarshal(s, &metadata); err != nil {
				return nil, errors.Wrapf(err, "unmarshal metadata fail")
			}
			msg.Metadata = metadata
		}

	}

	return msg, nil
}

type WithoutTraceMarshallerUnmarshaller struct{}

func (WithoutTraceMarshallerUnmarshaller) Marshal(_ string, msg *message.Message) ([]byte, error) {
	return []byte(fmt.Sprintf("%s_%s", msg.Metadata[scoreKey], string(msg.Payload))), nil
}

func (WithoutTraceMarshallerUnmarshaller) Unmarshal(values []byte) (msg *message.Message, err error) {
	val := strings.SplitN(string(values), "_", 2)

	msg = message.NewMessage(uuid.NewString(), []byte(val[1]))
	msg.Metadata = message.Metadata{scoreKey: val[0]}

	return msg, nil
}
