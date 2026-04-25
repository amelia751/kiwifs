package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type RedisSource struct {
	client  *redis.Client
	pattern string
	addr    string
}

func NewRedis(addr, password string, db int, pattern string) (*RedisSource, error) {
	if pattern == "" {
		pattern = "*"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &RedisSource{
		client:  client,
		pattern: pattern,
		addr:    addr,
	}, nil
}

func (s *RedisSource) Name() string { return "redis" }

func (s *RedisSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		var cursor uint64
		idx := 0

		for {
			keys, nextCursor, err := s.client.Scan(ctx, cursor, s.pattern, 100).Result()
			if err != nil {
				errs <- fmt.Errorf("redis scan: %w", err)
				return
			}

			for _, key := range keys {
				if ctx.Err() != nil {
					return
				}

				fields, err := s.readKey(ctx, key)
				if err != nil {
					errs <- fmt.Errorf("redis read %s: %w", key, err)
					continue
				}

				pk := sanitizeRedisKey(key)

				rec := Record{
					SourceID:   fmt.Sprintf("redis:%s:%d", key, idx),
					SourceDSN:  s.addr,
					Table:      "redis",
					Fields:     fields,
					PrimaryKey: pk,
				}
				select {
				case records <- rec:
				case <-ctx.Done():
					return
				}
				idx++
			}

			cursor = nextCursor
			if cursor == 0 {
				return
			}
		}
	}()
	return records, errs
}

func (s *RedisSource) readKey(ctx context.Context, key string) (map[string]any, error) {
	keyType, err := s.client.Type(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	switch keyType {
	case "hash":
		result, err := s.client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		fields := make(map[string]any, len(result))
		for k, v := range result {
			fields[k] = v
		}
		return fields, nil

	case "string":
		val, err := s.client.Get(ctx, key).Result()
		if err != nil {
			return nil, err
		}

		// Try JSON parsing.
		var obj map[string]any
		if json.Unmarshal([]byte(val), &obj) == nil {
			return obj, nil
		}

		return map[string]any{"value": val}, nil

	case "list":
		vals, err := s.client.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		return map[string]any{"items": vals}, nil

	case "set":
		vals, err := s.client.SMembers(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		return map[string]any{"members": vals}, nil

	case "zset":
		vals, err := s.client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		items := make([]map[string]any, len(vals))
		for i, z := range vals {
			items[i] = map[string]any{"member": z.Member, "score": z.Score}
		}
		return map[string]any{"items": items}, nil

	default:
		return map[string]any{"_type": keyType}, nil
	}
}

func (s *RedisSource) Close() error {
	return s.client.Close()
}

func sanitizeRedisKey(key string) string {
	key = strings.ReplaceAll(key, ":", "_")
	key = strings.ReplaceAll(key, "/", "_")
	key = strings.ReplaceAll(key, " ", "_")
	return key
}
