package rediszset

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"strconv"
)

const scoreKey = "watermill_score"

func GetScore(message *message.Message) (float64, error) {
	f, err := strconv.ParseFloat(message.Metadata.Get(scoreKey), 64)
	if err != nil {
		return 0, errors.Wrapf(err, "should use rediszset.NewMessage since zset need score")
	}
	return f, nil
}

func NewMessage(uuid string, score float64, payload message.Payload) *message.Message {
	msg := message.NewMessage(uuid, payload)
	msg.Metadata.Set(scoreKey, strconv.FormatFloat(score, 'f', -1, 64))
	return msg
}
