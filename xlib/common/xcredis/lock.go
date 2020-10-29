/*
  For example, lock and unlock a user using its ID as a resource identifier:

  token, ok, err := mgr.Lock(key, timeout)
	if err != nil {
		log.Fatal("Error while attempting lock")
	}
	if !ok {
		// User is in use - return to avoid duplicate work, race conditions, etc.
		return
	}
	defer mgr.Unlock(key, token)

	// Do something with the user.
*/

package xcredis

import (
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/pborman/uuid"
)

func (w *RedisManager) Lock(key string, timeout time.Duration) (ok bool, err error) {
	conn := w.getConn()
	defer w.putConn(conn)

	token := uuid.New()
	status, err := redis.String(conn.Do("SET", key, token, "EX", int64(timeout/time.Second), "NX"))
	if err == redis.ErrNil {
		// The lock was not successful, it already exists.
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return status == "OK", nil
}

func (w *RedisManager) Unlock(key string) (err error) {
	_, err = w.Del(key)
	return
}
