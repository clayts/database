package database //import "github.com/clayts/database"

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"

	"github.com/clayts/insist"
	"github.com/go-redis/redis/v7"
)

var db *redis.Client

//ErrNotFound is returned when a key is not found
var ErrNotFound = redis.Nil

var maxDatabaseRetryAttempts = 3

func init() {
	redisURL := os.Getenv("REDIS_URL")
	log.Println("initialising database")
	opt, err := redis.ParseURL(redisURL)
	insist.IsNil(err)
	db = redis.NewClient(opt)
	insist.Is(insist.OnString(db.Ping().Result()), "PONG")
}

//Flush deletes all information in the database
func Flush() {
	log.Println("flushing database:", insist.OnString(db.FlushDB().Result()))
}

//Terminate must be called before the program terminates.
func Terminate() {
	if db != nil {
		insist.IsNil(db.Close())
		db = nil
	}
}

//Transaction is an object which allows interaction with the database.
type Transaction struct {
	tx      *redis.Tx
	cache   map[string]string
	written map[string]struct{}
}

//Execute creates a temporary Transaction object and executes the given function.
//Expect the function to be run several times, in case another process changes the data while it's being executed (see redis optimistic locking).
//Because of this, be very careful about modifying data outside of the database in this function.
//If the function returns an error, the transaction is aborted and no changes are made.
func Execute(f func(t Transaction) error) error {
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
func (t Transaction) Read(key string, value interface{}) error {
	if _, ok := t.cache[key]; !ok {
		if err := t.tx.Watch(key).Err(); err != nil {
			return err
		}
		value, err := t.tx.Get(key).Result()
		if err != nil {
			return err
		}
		t.cache[key] = value
	}
	return gob.NewDecoder(bytes.NewBufferString(t.cache[key])).Decode(value)
}

//Write writes the given data into the database at the given key.
func (t Transaction) Write(key string, value interface{}) error {
	buffer := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(value)
	if err != nil {
		return err
	}
	t.cache[key] = buffer.String()
	t.written[key] = struct{}{}
	return nil
}
