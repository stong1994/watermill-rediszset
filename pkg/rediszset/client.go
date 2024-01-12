package rediszset

import (
	"context"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

type Client interface {
	ZRem(ctx context.Context, key string, members ...interface{}) (int64, error)
	ZAdd(ctx context.Context, key string, members ...redis.Z) (int64, error)

	ZPopMin(ctx context.Context, key string, count ...int64) ([]redis.Z, error)
	BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) (*redis.ZWithKey, error)

	Close() error
}

func NewClient(client redis.UniversalClient, version [3]int) Client {
	var c Client
	if version[0] < 5 {
		c = Client2_0{BaseClient{client: client}}
	} else {
		c = Client5_0{BaseClient{client: client}}
	}
	return c
}

type BaseClient struct {
	client redis.UniversalClient
}

// ZRem available since 1.2.0
func (b BaseClient) ZRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return b.client.ZRem(ctx, key, members...).Result()
}

// ZAdd available since 1.2.0
func (b BaseClient) ZAdd(ctx context.Context, key string, members ...redis.Z) (int64, error) {
	return b.client.ZAdd(ctx, key, members...).Result()
}

func (b BaseClient) Close() error {
	return b.client.Close()
}

type Client5_0 struct {
	BaseClient
}

func (c Client5_0) ZPopMin(ctx context.Context, key string, count ...int64) ([]redis.Z, error) {
	return c.client.ZPopMin(ctx, key, count...).Result()
}

func (c Client5_0) BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) (*redis.ZWithKey, error) {
	return c.client.BZPopMin(ctx, timeout, keys...).Result()
}

type Client2_0 struct {
	BaseClient
}

func (c Client2_0) ZPopMin(ctx context.Context, key string, count ...int64) ([]redis.Z, error) {
	cnt := "1"
	if len(count) > 0 {
		cnt = strconv.Itoa(int(count[0]))
	}
	return zpop(ctx, c.client, key, cnt)
}

func zpop(ctx context.Context, client redis.UniversalClient, key string, cnt string) ([]redis.Z, error) {
	rst, err := zpopScript.Run(ctx, client, []string{key}, "-inf", "+inf", cnt).Result()
	if err != nil {
		return nil, err
	}
	if rst == nil {
		return nil, nil
	}
	data := rst.([]interface{})
	var zs []redis.Z
	for i := 0; i < len(data); i += 2 {
		score, err := strconv.ParseFloat(data[1].(string), 64)
		if err != nil {
			return nil, err
		}
		zs = append(zs, redis.Z{
			Score:  score,
			Member: data[0],
		})
	}
	return zs, nil
}

func (c Client2_0) BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) (*redis.ZWithKey, error) {
	ctx, _ = context.WithTimeout(ctx, timeout)
	timer := time.NewTimer(timeout)

	for {
		for _, key := range keys {
			select {
			case <-timer.C:
				return nil, nil
			default:
				data, err := zpop(ctx, c.client, key, "1")
				if err != nil {
					return nil, err
				}
				if len(data) == 0 {
					continue
				}
				return &redis.ZWithKey{
					Z:   data[0],
					Key: key,
				}, nil
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

// ZRANGEBYSCORE available since 1.0.5
// ZCOUNT available since 2.0.0
// ZREM available since 1.2.0
var zpopScript = redis.NewScript(`
local data = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[2], "WITHSCORES", "limit", "0", ARGV[3])
if (data and #data > 0) then
	local members = {}
	for i, value in ipairs(data) do
	  if i % 2 == 1 then
	    table.insert(members, value)
	  end
	end
	redis.call("ZREM", KEYS[1], unpack(members))	
end
return data
`)
