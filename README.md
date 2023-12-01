[english](README.md) | [中文](README_CN.md)
# Watermill Redis Sorted Set Pub/Sub
<img align="right" width="200" src="https://threedots.tech/watermill-io/watermill-logo.png">

This is Pub/Sub for the [Watermill](https://watermill.io/) project.

Watermill is a Go library for working efficiently with message streams. It is intended
for building event driven applications, enabling event sourcing, RPC over messages,
sagas and basically whatever else comes to your mind. You can use conventional pub/sub
implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog if that fits your use case. 
## Usage
Since redis zset is not a traditional message queue, two modes are adopted in the implementation to meet daily needs.
### go get
```shell
go get -u github.com/stong1994/watermill-rediszset
```
### Publish
```go
    subscriber, _ := rediszset.NewSubscriber(
        rediszset.SubscriberConfig{
            Client:          client,
        },
        watermill.NewStdLogger(false, false),
    )
    messages, _ := subscriber.Subscribe(context.Background(), topic)
    for msg := range messages {
        fmt.Println(string(msg.Payload))
    }
```
Since  `redis zset`  needs to pass additional score,  `rediszset.NewMessage`  needs to be used to create a dedicated  `Message` .
### Consume
#### Normal mode
Usage:
```go
    subscriber, _ := rediszset.NewSubscriber(
        rediszset.SubscriberConfig{
            Client:          client,
        },
        watermill.NewStdLogger(false, false),
    )
    messages, _ := subscriber.Subscribe(context.Background(), topic)
    for msg := range messages {
        fmt.Println(string(msg.Payload))
    }
```
In Normal mode, the consumer uses  `zpopmin`  to consume the data with the lowest score, which means that once the consumption process fails, the data will be lost.
Therefore, we also support another more strict mode.
#### Strict mode
```go
    subscriber, err := rediszset.NewStrictSubscriber(
        rediszset.StrictSubscriberConfig{
            Client:       client,
        },
        locker,
        watermill.NewStdLogger(true, false),
    )
```
In Strict mode, the consumer uses  `zrangewithscores`  to get the lowest-scored data. Once the business code  `ACK` s the  `Message` , the consumer will execute  `zrem`  to delete the data.
If the business code  `NAck` s the  `Message` , the consumer will not execute  `zrem` .

In Strict mode, since the data is still in redis when consuming data, it may cause duplicate consumption, and locker is required to lock the data.

## License

[MIT License](./LICENSE)
