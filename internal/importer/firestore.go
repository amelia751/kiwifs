package importer

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

// FirestoreSource implements Source for Google Cloud Firestore.
type FirestoreSource struct {
	client     *firestore.Client
	projectID  string
	collection string
}

// NewFirestore creates a Firestore source. Uses Application Default Credentials
// (ADC) — set GOOGLE_APPLICATION_CREDENTIALS env var.
func NewFirestore(projectID, collection string) (*FirestoreSource, error) {
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("firestore client: %w", err)
	}
	return &FirestoreSource{client: client, projectID: projectID, collection: collection}, nil
}

func (s *FirestoreSource) Name() string { return s.collection }

func (s *FirestoreSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		iter := s.client.Collection(s.collection).Documents(ctx)
		defer iter.Stop()

		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				errs <- fmt.Errorf("firestore iter: %w", err)
				return
			}

			fields := mapFirestoreDoc(doc.Data())
			rec := Record{
				SourceID:   fmt.Sprintf("firestore:%s:%s", s.collection, doc.Ref.ID),
				SourceDSN:  s.projectID,
				Table:      s.collection,
				Fields:     fields,
				PrimaryKey: doc.Ref.ID,
			}
			select {
			case records <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()
	return records, errs
}

func (s *FirestoreSource) Close() error {
	return s.client.Close()
}

func mapFirestoreDoc(data map[string]any) map[string]any {
	out := make(map[string]any, len(data))
	for k, v := range data {
		out[k] = mapFirestoreValue(v)
	}
	return out
}

func mapFirestoreValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339)
	case map[string]any:
		return mapFirestoreDoc(val)
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = mapFirestoreValue(item)
		}
		return out
	default:
		return val
	}
}
