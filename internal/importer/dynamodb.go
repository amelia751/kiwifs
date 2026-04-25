package importer

import (
	"context"
	"fmt"
	"strconv"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamoSource struct {
	region    string
	tableName string
	client    *dynamodb.Client
}

func NewDynamoDB(region, tableName string) (*DynamoSource, error) {
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	return &DynamoSource{
		region:    region,
		tableName: tableName,
		client:    client,
	}, nil
}

func (s *DynamoSource) Name() string { return s.tableName }

func (s *DynamoSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		name := s.Name()
		idx := 0
		var lastKey map[string]types.AttributeValue

		for {
			input := &dynamodb.ScanInput{
				TableName:         &s.tableName,
				ExclusiveStartKey: lastKey,
			}

			resp, err := s.client.Scan(ctx, input)
			if err != nil {
				errs <- fmt.Errorf("dynamodb scan: %w", err)
				return
			}

			for _, item := range resp.Items {
				if ctx.Err() != nil {
					return
				}

				fields := unmarshalDynamoItem(item)

				pk := fmt.Sprintf("%d", idx)
				if id, ok := fields["id"]; ok {
					pk = fmt.Sprintf("%v", id)
				} else if id, ok := fields["pk"]; ok {
					pk = fmt.Sprintf("%v", id)
				} else if id, ok := fields["PK"]; ok {
					pk = fmt.Sprintf("%v", id)
				}

				rec := Record{
					SourceID:   fmt.Sprintf("dynamodb:%s:%d", name, idx),
					SourceDSN:  fmt.Sprintf("dynamodb://%s/%s", s.region, s.tableName),
					Table:      name,
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

			if resp.LastEvaluatedKey == nil {
				return
			}
			lastKey = resp.LastEvaluatedKey
		}
	}()
	return records, errs
}

func (s *DynamoSource) Close() error { return nil }

func unmarshalDynamoItem(item map[string]types.AttributeValue) map[string]any {
	fields := make(map[string]any, len(item))
	for k, v := range item {
		fields[k] = unmarshalDynamoValue(v)
	}
	return fields
}

func unmarshalDynamoValue(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		if n, err := strconv.ParseFloat(v.Value, 64); err == nil {
			if n == float64(int64(n)) {
				return int64(n)
			}
			return n
		}
		return v.Value
	case *types.AttributeValueMemberBOOL:
		return v.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberL:
		result := make([]any, len(v.Value))
		for i, item := range v.Value {
			result[i] = unmarshalDynamoValue(item)
		}
		return result
	case *types.AttributeValueMemberM:
		return unmarshalDynamoItem(v.Value)
	case *types.AttributeValueMemberSS:
		result := make([]any, len(v.Value))
		for i, s := range v.Value {
			result[i] = s
		}
		return result
	case *types.AttributeValueMemberNS:
		result := make([]any, len(v.Value))
		for i, s := range v.Value {
			if n, err := strconv.ParseFloat(s, 64); err == nil {
				result[i] = n
			} else {
				result[i] = s
			}
		}
		return result
	case *types.AttributeValueMemberBS:
		return fmt.Sprintf("[%d binary sets]", len(v.Value))
	case *types.AttributeValueMemberB:
		return fmt.Sprintf("[binary %d bytes]", len(v.Value))
	default:
		return fmt.Sprintf("%v", av)
	}
}
