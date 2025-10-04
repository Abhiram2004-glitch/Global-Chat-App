package main

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func SetupRedisClient() (*redis.Client, *redis.PubSub) {
	ctx := context.Background()

	opt := &redis.Options{
		Addr:      "valkey-2881b9f0-dak222004-e707.e.aivencloud.com:12722",
		Username:  "default",
		Password:  "AVNS_eNbJkdshaz_YZXELTYi",
		DB:        0,
		TLSConfig: &tls.Config{}, // Required for `rediss://`
	}

	client := redis.NewClient(opt)

	// Test connection
	pong, err := client.Ping(ctx).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("✅ Connected to Redis:", pong)

	// Pub/Sub setup

	sub := client.Subscribe(ctx, "my-channel") // Subscriber listens to same channel

	return client, sub
}
