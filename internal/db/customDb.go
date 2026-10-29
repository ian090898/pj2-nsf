package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const mongoTimeout = 5 * time.Second

// Each document represents one service and its serialized provider list.
type mongoRecord struct {
	Key   string `bson:"_id"`
	Value string `bson:"value"`
}

type customDb struct {
	client     *mongo.Client
	collection *mongo.Collection
}

// Ensure customDb implements all DbIf methods.
var _ DbIf = (*customDb)(nil)

func newCustomDb() (*customDb, error) {
	uri := os.Getenv("NSF_MONGO_URI")
	if uri == "" {
		uri = "mongodb://127.0.0.1:27018"
	}

	dbName := os.Getenv("NSF_MONGO_DATABASE")
	if dbName == "" {
		dbName = "pj2_nsf"
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connect to MongoDB: %w", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), mongoTimeout,
	)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping MongoDB: %w", err)
	}

	return &customDb{
		client:     client,
		collection: client.Database(dbName).Collection("services"),
	}, nil
}

func (m *customDb) Release() error {
	ctx, cancel := context.WithTimeout(
		context.Background(), mongoTimeout,
	)
	defer cancel()

	return m.client.Disconnect(ctx)
}

func (m *customDb) Save(key, value string) error {
	ctx, cancel := context.WithTimeout(
		context.Background(), mongoTimeout,
	)
	defer cancel()

	_, err := m.collection.ReplaceOne(
		ctx,
		bson.M{"_id": key},
		mongoRecord{Key: key, Value: value},
		options.Replace().SetUpsert(true),
	)
	return err
}

func (m *customDb) SaveAll(data map[string]string) error {
	if len(data) == 0 {
		return nil
	}

	models := make([]mongo.WriteModel, 0, len(data))
	for key, value := range data {
		models = append(models,
			mongo.NewReplaceOneModel().
				SetFilter(bson.M{"_id": key}).
				SetReplacement(mongoRecord{
					Key:   key,
					Value: value,
				}).
				SetUpsert(true),
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), mongoTimeout,
	)
	defer cancel()

	_, err := m.collection.BulkWrite(ctx, models)
	return err
}

func (m *customDb) Exist(key string) (bool, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), mongoTimeout,
	)
	defer cancel()

	err := m.collection.FindOne(
		ctx, bson.M{"_id": key},
	).Err()

	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}

	return err == nil, err
}

func (m *customDb) Load(key string) (string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), mongoTimeout,
	)
	defer cancel()

	var record mongoRecord
	err := m.collection.FindOne(
		ctx, bson.M{"_id": key},
	).Decode(&record)

	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return record.Value, nil
}

func (m *customDb) LoadAll() (map[string]string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), mongoTimeout,
	)
	defer cancel()

	cursor, err := m.collection.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	var records []mongoRecord
	if err := cursor.All(ctx, &records); err != nil {
		return nil, err
	}

	result := make(map[string]string, len(records))
	for _, record := range records {
		result[record.Key] = record.Value
	}

	return result, nil
}

func (m *customDb) Remove(key string) error {
	ctx, cancel := context.WithTimeout(
		context.Background(), mongoTimeout,
	)
	defer cancel()

	_, err := m.collection.DeleteOne(
		ctx, bson.M{"_id": key},
	)
	return err
}

func (m *customDb) RemoveAll() error {
	ctx, cancel := context.WithTimeout(
		context.Background(), mongoTimeout,
	)
	defer cancel()

	_, err := m.collection.DeleteMany(ctx, bson.D{})
	return err
}
