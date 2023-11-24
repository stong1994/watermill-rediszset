package redisstream

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"strconv"
)

const scoreKey = "watermill_score"

func GetScore(message *message.Message) (float64, error) {
	return strconv.ParseFloat(message.Metadata.Get(scoreKey), 64)
}

func NewMessage(uuid string, score float64, payload message.Payload) *message.Message {
	msg := message.NewMessage(uuid, payload)
	msg.Metadata.Set(scoreKey, strconv.FormatFloat(score, 'E', -1, 64))
	return msg
}
