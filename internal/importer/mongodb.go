package importer

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoSource implements Source for MongoDB.
type MongoSource struct {
	client     *mongo.Client
	database   string
	collection string
}

// NewMongoDB creates a MongoDB source.
func NewMongoDB(uri, database, collection string) (*MongoSource, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connect mongo: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("ping mongo: %w", err)
	}
	return &MongoSource{client: client, database: database, collection: collection}, nil
}

func (s *MongoSource) Name() string { return s.collection }

func (s *MongoSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		coll := s.client.Database(s.database).Collection(s.collection)
		cursor, err := coll.Find(ctx, bson.D{})
		if err != nil {
			errs <- fmt.Errorf("find: %w", err)
			return
		}
		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			var doc bson.M
			if err := cursor.Decode(&doc); err != nil {
				errs <- fmt.Errorf("decode: %w", err)
				return
			}

			fields := mapBSONDoc(doc)
			pk := ""
			if id, ok := doc["_id"]; ok {
				pk = fmt.Sprintf("%v", id)
				delete(fields, "_id")
			}

			rec := Record{
				SourceID:   fmt.Sprintf("mongo:%s:%s", s.collection, pk),
				SourceDSN:  s.database,
				Table:      s.collection,
				Fields:     fields,
				PrimaryKey: pk,
			}
			select {
			case records <- rec:
			case <-ctx.Done():
				return
			}
		}
		if err := cursor.Err(); err != nil {
			errs <- err
		}
	}()
	return records, errs
}

func (s *MongoSource) Close() error {
	return s.client.Disconnect(context.Background())
}

func mapBSONDoc(doc bson.M) map[string]any {
	out := make(map[string]any, len(doc))
	for k, v := range doc {
		out[k] = mapBSONValue(v)
	}
	return out
}

func mapBSONValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case bson.M:
		return mapBSONDoc(val)
	case bson.A:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = mapBSONValue(item)
		}
		return out
	case bson.ObjectID:
		return val.Hex()
	case time.Time:
		return val.Format(time.RFC3339)
	default:
		return val
	}
}
