package rediszset_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stong1994/watermill-rediszset/pkg/rediszset"
)

func TestDefaultMarshallerUnmarshaller_MarshalUnmarshal(t *testing.T) {
	m := rediszset.DefaultMarshallerUnmarshaller{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := m.Unmarshal(marshaled)
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))
}

func BenchmarkDefaultMarshallerUnmarshaller_Marshal(b *testing.B) {
	m := rediszset.DefaultMarshallerUnmarshaller{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	var err error
	for i := 0; i < b.N; i++ {
		_, err = m.Marshal("foo", msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDefaultMarshallerUnmarshaller_Unmarshal(b *testing.B) {
	m := rediszset.DefaultMarshallerUnmarshaller{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("foo", msg)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, err = m.Unmarshal(marshaled)
		if err != nil {
			b.Fatal(err)
		}
	}
}
