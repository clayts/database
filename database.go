package main //import "github.com/clayts/database"

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"

	"github.com/clayts/insist"
	"github.com/go-redis/redis/v7"
)

var db *redis.Client

var errNotFound = redis.Nil

var maxDatabaseRetryAttempts = 3

func getEnvironmentVariable(variableName string, defaultValue string) string {
	value := os.Getenv(variableName)
	if value == "" {
		return defaultValue
	}
	return value
}
func initDB() {
	redisURL := getEnvironmentVariable("REDIS_URL", "")
	log.Println("initialising database")
	opt, err := redis.ParseURL(redisURL)
	insist.IsNil(err)
	db = redis.NewClient(opt)
	pong, err := db.Ping().Result()
	log.Println("pinging:", pong, err)
	gob.Register(map[string]interface{}{})
}

func termDB() {
	if db != nil {
		insist.IsNil(db.Close())
	}
}

//Transaction is an object which allows interaction with the database.
type Transaction struct {
	tx      *redis.Tx
	cache   map[string]string
	written map[string]struct{}
}

//Transact creates a temporary Transaction object and executes the given function.
//Expect the function to be run several times, in case another process changes the data while it's being executed (see redis optimistic locking).
//Because of this, be very careful about modifying data outside of the database in this function.
//If the function returns an error, the transaction is aborted and no changes are made.
func Transact(f func(t Transaction) error) error {
	var err error
	for i := 0; i < maxDatabaseRetryAttempts; i++ {
		err = db.Watch(func(tx *redis.Tx) error {
			t := Transaction{}
			t.tx = tx
			t.cache = make(map[string]string)
			t.written = make(map[string]struct{})
			if err := f(t); err != nil {
				return err
			}
			_, err := tx.TxPipelined(func(pipe redis.Pipeliner) error {
				for k := range t.written {
					err := pipe.Set(k, t.cache[k], 0).Err()
					if err != nil {
						return err
					}
				}
				return nil
			})
			return err
		})
		if err == nil {
			return nil
		}
	}
	log.Println("max retries reached in transaction")
	return err
}

//Exists checks for the existence of a key in the database.
func (t Transaction) Exists(key string) bool {
	if _, ok := t.cache[key]; !ok {
		if err := t.tx.Watch(key).Err(); err != nil {
			return false
		}
		return true
	}
	return true
}

//Read reads the given key into the given interface, which should be a pointer.
func (t Transaction) Read(key string) func(e interface{}) error {
	if _, ok := t.cache[key]; !ok {
		if err := t.tx.Watch(key).Err(); err != nil {
			return func(e interface{}) error { return err }
		}
		value, err := t.tx.Get(key).Result()
		if err != nil {
			return func(e interface{}) error { return err }
		}
		t.cache[key] = value
	}

	return gob.NewDecoder(bytes.NewBufferString(t.cache[key])).Decode
}

//Write writes the given data into the database at the given key.
func (t Transaction) Write(key string) func(e interface{}) error {
	buffer := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buffer)
	return func(e interface{}) error {
		err := encoder.Encode(e)
		if err != nil {
			return err
		}
		t.cache[key] = buffer.String()
		t.written[key] = struct{}{}
		return nil
	}
}