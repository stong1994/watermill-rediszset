[english](README.md) | [中文](README_CN.md)
# Watermill Redis Sorted Set Pub/Sub
<img align="right" width="200" src="https://threedots.tech/watermill-io/watermill-logo.png">

这个项目使用redis zset实现了[Watermill](https://watermill.io/) 的Pub/Sub.
> Watermill 是一个用于快速实现消息流的库。可以使用Watermill快速构建基于事件驱动的应用，支持事件源、RPC消息、saga等其他功能

## Usage
由于redis zset不是传统意义上的消息队列，因此实现中采用了两种模式来满足日常需求。
### go get
```shell
go get -u github.com/stong1994/watermill-rediszset
```
### Publish
```go
    publisher, _ := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     client,
			Marshaller: rediszset.DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	msg := rediszset.NewMessage(watermill.NewShortUUID(), 100, []byte("hello"))
	publisher.Publish(topic, msg)
```
由于`redis zset`需要传递额外的score，因此需要使用`rediszset.NewMessage`来创建专用的`Message`。
### Consume
#### Normal模式
使用方式：
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
在Normal模式中，消费者使用`zpopmin`来消费数据分数最低的数据，这意味着一旦消费过程出现异常，这个数据就会丢失。
因此我们还支持另外一种更严格的模式。
#### Strict模式
```go
    subscriber, err := rediszset.NewStrictSubscriber(
        rediszset.StrictSubscriberConfig{
            Client:       client,
        },
		locker,
		watermill.NewStdLogger(true, false),
	)
```
在Strict模式中，消费者使用`zrangewithscores`来获取分数最低的一个数据，一旦业务代码`ACK`了`Message`，消费者会执行`zrem`来删除数据。
如果业务代码`NAck`了`Message`，则消费者不会执行`zrem`.

在Strict模式中，由于消费数据时数据仍在redis中，因此可能会造成重复消费，需要使用locker来锁定数据。


## License

[MIT License](./LICENSE)
