/*
 * Copyright (C) Zhiguang Zheng
 * Copyright (C) ixiaochuan.cn
 */

package xcredis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"

	"git.ixiaochuan.cn/xclib/common/logger"
	"git.ixiaochuan.cn/xclib/common/lookupd"
	"git.ixiaochuan.cn/xclib/common/perfcounter"
	"git.ixiaochuan.cn/xclib/common/xccrypt"
	"git.ixiaochuan.cn/xclib/common/xckey"
	"git.ixiaochuan.cn/xclib/common/xcmetrics"
	"git.ixiaochuan.cn/xclib/common/xcredis/cfgstruct"
)

const (
	DEFAULT_POOLSIZE  = 5
	DEFAULT_MAX_RETRY = 2
)

type RedisManager struct {
	host         string
	auth         string
	redisPool    chan redis.Conn
	timeout      time.Duration
	readCounter  int64
	writeCounter int64
	retry        int

	normalizeSecName string
}

func Init(cfg cfgstruct.RedisSt) (mgr *RedisManager, err error) {
	return InitWithSecName(cfg, "")
}

func InitWithSecName(cfg cfgstruct.RedisSt, secName string) (mgr *RedisManager, err error) {
	hostport := cfg.Hostport
	auth := cfg.Auth
	poolsize := cfg.Poolsize
	timeout := cfg.Timeout
	if hostport == "" {
		err = fmt.Errorf("redis cfg hostport is nil")
		return
	}
	if timeout == 0 {
		err = fmt.Errorf("redis cfg timeout is nil")
		return
	}
	if auth != "" {
		old := auth
		auth, err = xccrypt.Decrypt(auth, xckey.RedisKey)
		logger.Info("[redis] auth of %s = [%s]%s", secName, old, auth)
		if err != nil {
			return
		}
	}
	mgr, err = NewRedisManagerWithSecName(hostport, auth, poolsize, time.Duration(timeout)*time.Second, secName)
	if err != nil {
		logger.Error("fail to connect to redis [%s]%s: %v", secName, hostport, err)
		return
	}
	logger.Info("[redis] connected to redis [%s]%s", secName, hostport)
	return
}

func NewRedisManager(host, auth string, poolsize int, timeout time.Duration) (p *RedisManager, err error) {
	return NewRedisManagerWithSecName(host, auth, poolsize, timeout, "")
}

func NewRedisManagerWithSecName(host, auth string, poolsize int, timeout time.Duration, secName string) (p *RedisManager, err error) {
	p = &RedisManager{}
	if poolsize == 0 {
		poolsize = DEFAULT_POOLSIZE
	}
	p.host = host
	p.auth = auth
	p.redisPool = make(chan redis.Conn, poolsize)
	p.timeout = timeout
	p.retry = DEFAULT_MAX_RETRY
	for i := 0; i < poolsize; i++ {
		conn, err := p.dialRedis()
		if err != nil {
			return nil, err
		}
		p.redisPool <- conn
	}

	if len(secName) > 0 {
		p.normalizeSecName = strings.Replace(secName, "-", "_", -1)
	}

	return
}

func parseErrForMetric(err error) string {
	if err == nil {
		return ""
	}
	estr := fmt.Sprintf("%v", err)
	if len(estr) > 16 {
		return estr[:16]
	}
	return estr
}

func (w *RedisManager) InfoRM() string {
	return fmt.Sprintf("pool-len:%d, auth:%s, host:%s, timeout:%v", len(w.redisPool), w.auth, w.host, w.timeout)
}

func (w *RedisManager) SetRetry(retry int) {
	w.retry = retry
}

func (w *RedisManager) dialRedis() (conn redis.Conn, err error) {
	conn, err = redis.DialTimeout("tcp", w.host, w.timeout, w.timeout, w.timeout)
	if err != nil {
		return nil, err
	}
	if w.auth != "" {
		_, err = conn.Do("AUTH", w.auth)
	}
	return
}

func (w *RedisManager) do(conn redis.Conn, cmd string, args ...interface{}) (reply interface{}, err error) {
	t1 := time.Now()
	defer func() {
		sub := time.Now().Sub(t1)
		switch {
		case sub > time.Millisecond*1000:
			perfcounter.Add("redis-usedtime-1000", 1)
		case sub > time.Millisecond*500:
			perfcounter.Add("redis-usedtime-500", 1)
		case sub > time.Millisecond*200:
			perfcounter.Add("redis-usedtime-200", 1)
		case sub > time.Millisecond*100:
			perfcounter.Add("redis-usedtime-100", 1)
		case sub > time.Millisecond*50:
			perfcounter.Add("redis-usedtime-50", 1)
		case sub <= time.Millisecond*50:
			perfcounter.Add("redis-usedtime-0", 1)
		}
		perfcounter.Add("redis-usedtime-total", 1)

		if sub > time.Millisecond*500 {
			warn := fmt.Sprintf("redis too slow, used %v, %s %+v", sub, cmd, args)
			if len(warn) > 500 {
				warn = warn[:500] + "..."
			}
			logger.Warn(warn)
		}

		// ATTENTION(zhiguang): redis elapsed time gt 5 seconds
		if sub > time.Millisecond*4500 {
			logger.Error("redis elapsed: %v, %s, hostport: %v", sub, cmd, w.host)
		}

		if sub > time.Millisecond*200 {
			lookupd.DailyAdd("#P1.redis.slow", 1)
		}

		estr := parseErrForMetric(err)
		xcmetrics.HandleRedisRequest("unknown", w.normalizeSecName, cmd, sub.Seconds()*1000, estr)
	}()

	// ATTENTION(zhiguang): redis error with logger.Error
	reply, err = conn.Do(cmd, args...)
	if err != nil && err != redis.ErrNil {
		logger.Error("redis %v, err: %v, hostport: %v, args:%v", cmd, err, w.host, args)
	}
	return
}

func (w *RedisManager) Do(action func(conn redis.Conn) (interface{}, error)) (reply interface{}, err error) {
	return w.redialDo(action)
}

func (w *RedisManager) redialDo(action func(conn redis.Conn) (reply interface{}, err error)) (reply interface{}, err error) {
	defer func() {
		if err != nil && err != redis.ErrNil {
			lookupd.DailyAdd("#P0.redis.err", 1)
		}
	}()

	if w.retry == 0 {
		w.retry = DEFAULT_MAX_RETRY
	}
	conn := w.getConn()
	count := 0

start:
	count++
	if count > w.retry {
		w.putConn(conn)
		return
	}
	reply, err = action(conn)
	if err != nil {
		goto fail_redial
	}
	w.putConn(conn)
	return

fail_redial:
	conn.Close()
	var nerr error
	var nconn redis.Conn
	if nconn, nerr = w.dialRedis(); nerr != nil {
		err = nerr
		goto start
	}
	conn = nconn
	goto start
}

func (w *RedisManager) getConn() redis.Conn {
	return <-w.redisPool
}

func (w *RedisManager) putConn(conn redis.Conn) {
	w.redisPool <- conn
}

// ----------- string -----------------
func (w *RedisManager) Set(key string, value interface{}) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SET", key, value)
	}
	return w.Do(action)
}

func (w *RedisManager) SetEx(key string, value interface{}, expire int64) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SETEX", key, expire, value)
	}
	return w.Do(action)
}

func (w *RedisManager) SetNx(key string, value interface{}) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SETNX", key, value)
	}
	return w.Do(action)
}

func (w *RedisManager) SetExNx(key string, value interface{}, expire int64) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SET", key, value, "EX", expire, "NX")
	}
	return w.Do(action)
}

func (w *RedisManager) SetExXx(key string, value interface{}, expire int64) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SET", key, value, "EX", expire, "XX")
	}
	return w.Do(action)
}

func (w *RedisManager) SetXx(key string, value interface{}, expire int64) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SET", key, value, "EX", expire, "XX")
	}
	return w.Do(action)
}

func (w *RedisManager) SetRange(key string, pos uint64, value interface{}) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "setrange", key, pos, value)
	}
	return w.Do(action)
}

func (w *RedisManager) PSetEx(key string, value interface{}, expire int64) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "PSETEX", key, expire, value)
	}
	return w.Do(action)
}

func (w *RedisManager) MSet(param map[string]interface{}) (reply interface{}, err error) {
	args := []interface{}{}
	for k, v := range param {
		args = append(args, k, v)
	}
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "MSET", args...)
	}

	reply, err = w.Do(action)
	return
}

func (w *RedisManager) Get(key string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "GET", key)
	}
	return w.Do(action)
}

func (w *RedisManager) MGetSlice(keys []string) (reply interface{}, err error) {
	args := []interface{}{}
	for _, v := range keys {
		args = append(args, v)
	}

	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "MGET", args...)
	}
	reply, err = w.Do(action)

	return
}

func (w *RedisManager) MGet(keys []string) (result map[string]string, err error) {
	args := []interface{}{}
	for _, v := range keys {
		args = append(args, v)
	}

	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "MGET", args...)
	}
	reply, err := w.Do(action)
	if err != nil {
		return
	}

	result, err = w.replyToMap(reply, keys)
	return
}

func (w *RedisManager) MGetBytes(keys []string) (result map[string][]byte, err error) {
	args := []interface{}{}
	for _, v := range keys {
		args = append(args, v)
	}

	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "MGET", args...)
	}
	reply, err := w.Do(action)
	if err != nil {
		return
	}

	result, err = w.replyToMapBytes(reply, keys)
	return
}

func (w *RedisManager) Append(key string, value string) (length int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "APPEND", key, value)
	}
	length, err = redis.Int64(w.Do(action))
	return
}

func (w *RedisManager) Exists(key string) (is bool, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "EXISTS", key)
	}
	return redis.Bool(w.Do(action))
}

func (w *RedisManager) Del(key string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "DEL", key)
	}
	return w.Do(action)
}

func (w *RedisManager) Incr(key string) (num int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "INCR", key)
	}
	num, err = redis.Int64(w.Do(action))
	return
}

func (w *RedisManager) Incrby(key string, add int64) (num int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "INCRBY", key, add)
	}

	num, err = redis.Int64(w.Do(action))
	return
}

func (w *RedisManager) Decr(key string) (num int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "DECR", key)
	}
	num, err = redis.Int64(w.Do(action))
	return
}

func (w *RedisManager) GetString(key string) (value string, err error) {
	reply, err := w.Get(key)
	if err != nil {
		return
	}
	if reply == nil {
		return
	}

	return redis.String(reply, err)
}

func (w *RedisManager) GetInt64(key string) (value int64, err error) {
	val, err := w.Get(key)
	if err != nil {
		return
	}
	if val == nil {
		return
	}

	return redis.Int64(val, err)
}

func (w *RedisManager) GetUint64(key string) (value uint64, err error) {
	val, err := w.Get(key)
	if err != nil {
		return
	}
	if val == nil {
		return
	}

	return redis.Uint64(val, err)
}

func (w *RedisManager) GetFloat(key string) (value float64, err error) {
	return redis.Float64(w.Get(key))
}

func (w *RedisManager) StrLen(key string) (len int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "strlen", key)
	}
	return redis.Int64(w.Do(action))
}

// ----------- hash -----------------
func (w *RedisManager) Hincrby(key string, subkey string, inc int64) (num int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HINCRBY", key, subkey, inc)
	}
	num, err = redis.Int64(w.Do(action))
	return
}

func (w *RedisManager) Hsetnx(key string, subkey string, value interface{}) (reply int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HSETNX", key, subkey, value)
	}
	reply, err = redis.Int64(w.Do(action))
	return
}

func (w *RedisManager) Hset(key string, subkey string, value interface{}) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HSET", key, subkey, value)
	}
	return w.Do(action)
}

func (w *RedisManager) Hmset(key string, param map[string]interface{}) (reply interface{}, err error) {
	args := []interface{}{key}
	for k, v := range param {
		args = append(args, k, v)
	}
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HMSET", args...)
	}

	reply, err = w.Do(action)
	return
}

func (w *RedisManager) Hmget(key string, param []string) (result map[string]string, err error) {
	args := []interface{}{key}
	for _, v := range param {
		args = append(args, v)
	}
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HMGET", args...)
	}

	reply, err := w.Do(action)
	if err != nil {
		return
	}

	result, err = w.replyToMap(reply, param)
	return
}

func (w *RedisManager) Hget(key string, subkey string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HGET", key, subkey)
	}
	return w.Do(action)
}

func (w *RedisManager) HgetInt64(key string, subkey string) (value int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HGET", key, subkey)
	}

	val, err := w.Do(action)
	return redis.Int64(val, err)
}

func (w *RedisManager) Hexists(key string, subkey string) (result bool, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HEXISTS", key, subkey)
	}
	reply, err := w.Do(action)
	if err != nil {
		return
	}
	if reply == nil {
		return
	}
	res := fmt.Sprintf("%v", reply)
	if res == "1" {
		result = true
	}
	return
}

func (w *RedisManager) Hkeys(key string) (result []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HKEYS", key)
	}
	reply, err := w.Do(action)
	result, err = redis.Strings(reply, err)
	if err != nil {
		return
	}
	return
}

func (w *RedisManager) HScan(key string, offset int64) (nextOffset int64, datas map[string]string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HSCAN", key, offset)
	}
	reply, err := w.Do(action)
	if err != nil {
		return
	}
	result, err := redis.MultiBulk(reply, err)
	if err != nil {
		return
	}
	if len(result) != 2 {
		err = fmt.Errorf("unexpect result length %d", len(result))
		return
	}
	nextOffset, err = redis.Int64(result[0], nil)
	if err != nil {
		return
	}
	strs, err := redis.Strings(result[1], nil)
	if err != nil {
		return
	}
	if len(strs)%2 != 0 {
		if len(result) != 2 {
			err = fmt.Errorf("unexpect sub result length %d", len(strs))
			return
		}
	}
	datas = make(map[string]string)
	for i := 0; i < len(strs); i = i + 2 {
		datas[strs[i]] = strs[i+1]
	}
	return
}

func (w *RedisManager) Pttl(key string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "PTTL", key)
	}
	return w.Do(action)
}

func (w *RedisManager) Ttl(key string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "TTL", key)
	}
	return w.Do(action)
}

func (w *RedisManager) PTTL(key string) (pttl int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "PTTL", key)
	}
	return redis.Int64(w.Do(action))
}

func (w *RedisManager) TTL(key string) (ttl int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "TTL", key)
	}
	return redis.Int64(w.Do(action))
}

func (w *RedisManager) HGetAll(key string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HGETALL", key)
	}
	return w.Do(action)
}

func (w *RedisManager) HgetAll(key string) (result map[string]string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HGETALL", key)
	}
	reply, err := w.Do(action)
	replyList, err := redis.Strings(reply, err)
	if err != nil {
		return
	}
	var k string
	result = make(map[string]string)
	for i, cur := range replyList {
		if i%2 == 0 {
			k = cur
			continue
		}
		result[k] = cur
	}
	return
}

func (w *RedisManager) HDel(key string, subkey string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HDEL", key, subkey)
	}
	return w.Do(action)
}

func (w *RedisManager) HExists(key string, subkey string) (is bool, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HEXISTS", key, subkey)
	}
	return redis.Bool(w.Do(action))
}

func (w *RedisManager) GetHString(key string, subkey string) (value string, err error) {
	return redis.String(w.Hget(key, subkey))
}

func (w *RedisManager) GetHInt64(key string, subkey string) (value int64, err error) {
	val, err := w.Hget(key, subkey)
	if err == redis.ErrNil {
		err = nil
		return
	}
	if err != nil {
		return
	}
	if val == nil {
		return
	}

	return redis.Int64(val, err)
}

// ----------- set -----------------
func (w *RedisManager) Sismember(key string, member interface{}) (res bool, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SISMEMBER", key, member)
	}
	return redis.Bool(w.Do(action))
}

func (w *RedisManager) Sadd(key string, member ...interface{}) (reply interface{}, err error) {
	args := []interface{}{key}
	for _, m := range member {
		args = append(args, m)
	}
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SADD", args...)
	}
	return w.Do(action)
}

func (w *RedisManager) Srem(key string, member ...interface{}) (reply interface{}, err error) {
	args := []interface{}{key}
	for _, m := range member {
		args = append(args, m)
	}
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SREM", args...)
	}
	return w.Do(action)
}

func (w *RedisManager) Scard(key string) (num int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SCARD", key)
	}
	reply, err := w.Do(action)
	if err != nil || reply == nil {
		return
	}
	num, err = redis.Int64(reply, err)
	return
}

func (w *RedisManager) Smembers(key string) (member []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SMEMBERS", key)
	}
	reply, err := w.Do(action)
	if err != nil {
		return
	}
	if reply == nil {
		return
	}
	member, err = redis.Strings(reply, err)
	return
}

func (w *RedisManager) Srandmember(key string, count int64) (member []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "SRANDMEMBER", key, count)
	}
	reply, err := w.Do(action)
	if err != nil {
		return
	}
	if reply == nil {
		return
	}
	member, err = redis.Strings(reply, err)
	return
}

// ----------- system ---------------
func (w *RedisManager) Expire(key string, ts int64) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "EXPIRE", key, ts)
	}
	return w.Do(action)
}

func (w *RedisManager) Info() (stats []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "INFO")
	}
	content, err := redis.Bytes(w.Do(action))
	stats = strings.Split(string(content), "\n")
	return
}

// ----------- list ---------------
func (w *RedisManager) Lpush(key string, content interface{}) (err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "LPUSH", key, content)
	}
	_, err = w.Do(action)
	return
}

func (w *RedisManager) LpushMulti(key string, contents ...interface{}) (reply interface{}, err error) {
	args := []interface{}{key}
	args = append(args, contents...)
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "LPUSH", args...)
	}
	return w.Do(action)
}

func (w *RedisManager) Rpush(key string, content interface{}) (err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "RPUSH", key, content)
	}
	_, err = w.Do(action)
	return
}

func (w *RedisManager) RpushMulti(key string, contents ...interface{}) (reply interface{}, err error) {
	args := []interface{}{key}
	args = append(args, contents...)
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "RPUSH", args...)
	}
	return w.Do(action)
}

func (w *RedisManager) Rpop(key string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "RPOP", key)
	}
	reply, err = w.Do(action)
	return
}

func (w *RedisManager) Rpoplpush(key1 string, key2 string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "RPOPLPUSH", key1, key2)
	}
	reply, err = w.Do(action)
	return
}

func (w *RedisManager) Brpop(key string, timeout int) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "BRPOP", key, timeout)
	}
	reply, err = w.Do(action)
	return
}

func (w *RedisManager) LLen(key string) (length int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "LLEN", key)
	}
	reply, err := w.Do(action)
	if reply == nil {
		err = fmt.Errorf("reply eq nil")
		return
	}
	_, ok := reply.(int64)
	if !ok {
		err = fmt.Errorf("invalid valid")
		return
	}
	length = int(reply.(int64))
	return
}

func (w *RedisManager) Lpop(key string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "LPOP", key)
	}
	reply, err = w.Do(action)
	return
}

func (w *RedisManager) Blpop(key string, timeout int) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "BLPOP", key, timeout)
	}
	reply, err = w.Do(action)
	return
}

func (w *RedisManager) Lrange(key string, start, end int) (reply []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "LRANGE", key, start, end)
	}
	reply, err = redis.Strings(w.Do(action))
	return
}

func (w *RedisManager) LrangeValues(key string, start, end int) (reply []interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "LRANGE", key, start, end)
	}
	reply, err = redis.Values(w.Do(action))
	return
}

func (w *RedisManager) Ltrim(key string, start, end int) (err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "LTRIM", key, start, end)
	}
	_, err = w.Do(action)
	return
}

// ----------- zset -----------------
func (w *RedisManager) Zincrby(key string, name string, add int64) (num int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZINCRBY", key, add, name)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}
	num, err = redis.Int64(reply, err)
	return
}

func (w *RedisManager) Zrank(key string, name string) (rank int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZRANK", key, name)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}
	if reply == nil {
		rank = -1
		return
	}
	rank, err = redis.Int64(reply, err)
	return
}

func (w *RedisManager) Zrevrank(key string, name string) (rank int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREVRANK", key, name)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}
	if reply == nil {
		rank = -1
		return
	}
	rank, err = redis.Int64(reply, err)
	return
}

func (w *RedisManager) Zrem(key string, member string) (del int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREM", key, member)
	}

	reply, err := w.Do(action)
	if err != nil {
		return
	}

	del, err = redis.Int(reply, err)
	return
}

func (w *RedisManager) Zadd(key string, member string, score float64) (add int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZADD", key, score, member)
	}

	reply, err := w.Do(action)
	if err != nil {
		return
	}

	add, err = redis.Int(reply, err)
	return
}

func (w *RedisManager) ZaddNx(key string, member string, score float64) (add int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZADD", key, "NX", score, member)
	}

	reply, err := w.Do(action)
	if err != nil {
		return
	}

	add, err = redis.Int(reply, err)
	return
}

func (w *RedisManager) ZaddMulti(key string, args ...interface{}) (add int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		tmpArgs := []interface{}{key}
		tmpArgs = append(tmpArgs, args...)
		return w.do(conn, "ZADD", tmpArgs...)
	}

	reply, err := w.Do(action)
	if err != nil {
		return
	}

	add, err = redis.Int(reply, err)
	return
}

func (w *RedisManager) Zscore(key string, member string) (score string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZSCORE", key, member)
	}

	reply, err := w.Do(action)
	if err != nil {
		return
	}

	score, err = redis.String(reply, err)
	return
}

func (w *RedisManager) ZScore(key string, member string) (score interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZSCORE", key, member)
	}
	score, err = w.Do(action)
	if err != nil {
		return
	}
	return
}

func (w *RedisManager) Zrange(key string, start, end int) (rankList []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZRANGE", key, start, end)
	}
	rankList, err = redis.Strings(w.Do(action))
	return
}

func (w *RedisManager) Zrevrange(key string, start, end int) (rankList []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREVRANGE", key, start, end)
	}
	rankList, err = redis.Strings(w.Do(action))
	return
}

func (w *RedisManager) ZrangeWithScore(key string, start, end int) (sortedKeys []string, result map[string]float64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZRANGE", key, start, end, "WITHSCORES")
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}

	replyList, err := redis.Strings(reply, err)
	if err != nil {
		return
	}

	if len(replyList)%2 != 0 {
		err = fmt.Errorf("invalid reply")
		return
	}

	sortedKeys = make([]string, 0)
	result = make(map[string]float64)
	for i := 0; i < len(replyList); i += 2 {
		key := replyList[i]
		value, err := strconv.ParseFloat(replyList[i+1], 64)
		if err != nil {
			return nil, nil, err
		}
		result[key] = value
		sortedKeys = append(sortedKeys, key)
	}
	return
}

func (w *RedisManager) ZrevrangeWithScore(key string, start, end int) (result map[string]float64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREVRANGE", key, start, end, "WITHSCORES")
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}

	replyList, err := redis.Strings(reply, err)
	if err != nil {
		return
	}

	if len(replyList)%2 != 0 {
		err = fmt.Errorf("invalid reply")
		return
	}

	result = make(map[string]float64)
	for i := 0; i < len(replyList); i += 2 {
		key := replyList[i]
		value, err := strconv.ParseFloat(replyList[i+1], 64)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	return
}

func (w *RedisManager) ZrevrangeWithScoreRetRList(key string, start, end int) (sortedKeys []string, result map[string]float64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREVRANGE", key, start, end, "WITHSCORES")
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}

	replyList, err := redis.Strings(reply, err)
	if err != nil {
		return
	}

	if len(replyList)%2 != 0 {
		err = fmt.Errorf("invalid reply")
		return
	}

	sortedKeys = make([]string, 0)
	result = make(map[string]float64)
	for i := 0; i < len(replyList); i += 2 {
		key := replyList[i]
		value, err := strconv.ParseFloat(replyList[i+1], 64)
		if err != nil {
			return nil, nil, err
		}
		result[key] = value
		sortedKeys = append(sortedKeys, key)
	}
	return
}

func (w *RedisManager) ZrangeByScore(key string, min, max, offset, limit int) (rankList []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZRANGEBYSCORE", key, min, max, "LIMIT", offset, limit)
	}
	rankList, err = redis.Strings(w.Do(action))
	return
}

func (w *RedisManager) ZrevrangeByScore(key string, max, min, offset, limit int) (rankList []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREVRANGEBYSCORE", key, max, min, "LIMIT", offset, limit)
	}
	rankList, err = redis.Strings(w.Do(action))
	return
}

func (w *RedisManager) ZrangeByScoreWithPrefix(key string, prefixMin string, min int, prefixMax string, max, offset, limit int) (rankList []string, err error) {
	minStr := fmt.Sprintf("%s%d", prefixMin, min)
	maxStr := fmt.Sprintf("%s%d", prefixMax, max)
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZRANGEBYSCORE", key, minStr, maxStr, "LIMIT", offset, limit)
	}
	rankList, err = redis.Strings(w.Do(action))
	return
}

func (w *RedisManager) ZrevrangeByScoreWithPrefix(key string, prefixMin string, min int, prefixMax string, max, offset, limit int) (rankList []string, err error) {
	minStr := fmt.Sprintf("%s%d", prefixMin, min)
	maxStr := fmt.Sprintf("%s%d", prefixMax, max)
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREVRANGEBYSCORE", key, maxStr, minStr, "LIMIT", offset, limit)
	}
	rankList, err = redis.Strings(w.Do(action))
	return
}

func (w *RedisManager) ZrangeByScoreWithScore(key string, min, max, offset, count int) (sortedKeys []string, result map[string]float64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZRANGEBYSCORE", key, min, max, "WITHSCORES", "LIMIT", offset, count)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}

	replyList, err := redis.Strings(reply, err)
	if err != nil {
		return
	}

	if len(replyList)%2 != 0 {
		err = fmt.Errorf("invalid reply")
		return
	}

	sortedKeys = make([]string, 0)
	result = make(map[string]float64)
	for i := 0; i < len(replyList); i += 2 {
		key := replyList[i]
		value, err := strconv.ParseFloat(replyList[i+1], 64)
		if err != nil {
			return nil, nil, err
		}
		result[key] = value
		sortedKeys = append(sortedKeys, key)
	}
	return
}

func (w *RedisManager) ZrangeByScoreWithScoreStr(key string, min, max string, offset, count int) (sortedKeys []string, result map[string]float64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZRANGEBYSCORE", key, min, max, "WITHSCORES", "LIMIT", offset, count)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}

	replyList, err := redis.Strings(reply, err)
	if err != nil {
		return
	}

	if len(replyList)%2 != 0 {
		err = fmt.Errorf("invalid reply")
		return
	}

	sortedKeys = make([]string, 0)
	result = make(map[string]float64)
	for i := 0; i < len(replyList); i += 2 {
		key := replyList[i]
		value, err := strconv.ParseFloat(replyList[i+1], 64)
		if err != nil {
			return nil, nil, err
		}
		result[key] = value
		sortedKeys = append(sortedKeys, key)
	}
	return
}

func (w *RedisManager) ZrevrangeByScoreWithScore(key string, max, min, offset, count int) (sortedKeys []string, result map[string]float64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREVRANGEBYSCORE", key, max, min, "WITHSCORES", "LIMIT", offset, count)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}

	replyList, err := redis.Strings(reply, err)
	if err != nil {
		return
	}

	if len(replyList)%2 != 0 {
		err = fmt.Errorf("invalid reply")
		return
	}

	sortedKeys = make([]string, 0)
	result = make(map[string]float64)
	for i := 0; i < len(replyList); i += 2 {
		key := replyList[i]
		value, err := strconv.ParseFloat(replyList[i+1], 64)
		if err != nil {
			return nil, nil, err
		}
		result[key] = value
		sortedKeys = append(sortedKeys, key)
	}
	return
}

func (w *RedisManager) ZrevrangeByScoreWithScoreStr(key string, max, min string, offset, count int) (sortedKeys []string, result map[string]float64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREVRANGEBYSCORE", key, max, min, "WITHSCORES", "LIMIT", offset, count)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}

	replyList, err := redis.Strings(reply, err)
	if err != nil {
		return
	}

	if len(replyList)%2 != 0 {
		err = fmt.Errorf("invalid reply")
		return
	}

	sortedKeys = make([]string, 0, len(replyList)/2)
	result = make(map[string]float64)
	for i := 0; i < len(replyList); i += 2 {
		key := replyList[i]
		value, err := strconv.ParseFloat(replyList[i+1], 64)
		if err != nil {
			return nil, nil, err
		}
		result[key] = value
		sortedKeys = append(sortedKeys, key)
	}
	return
}

func (w *RedisManager) Zcount(key string, minScore interface{}, maxScore interface{}) (n int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZCOUNT", key, minScore, maxScore)
	}
	n, err = redis.Int(w.Do(action))
	return
}

func (w *RedisManager) Zcard(key string) (n int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZCARD", key)
	}
	n, err = redis.Int(w.Do(action))
	return
}

func (w *RedisManager) Zremrangebyrank(key string, start int, stop int) (n int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREMRANGEBYRANK", key, start, stop)
	}
	n, err = redis.Int(w.Do(action))
	return
}

func (w *RedisManager) Zremrangebyscore(key string, start int, stop int) (n int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREMRANGEBYSCORE", key, start, stop)
	}
	n, err = redis.Int(w.Do(action))
	return
}

func (w *RedisManager) ZremrangebyscoreStr(key string, start, stop string) (n int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZREMRANGEBYSCORE", key, start, stop)
	}
	n, err = redis.Int(w.Do(action))
	return
}

func (w *RedisManager) ZUnionStore(key string, keys []string, weights []interface{}, aggregate string) (n int, err error) {
	length := len(keys)
	args := make([]interface{}, 0, length*2+1)
	args = append(args, key, length)
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, "WEIGHTS")
	args = append(args, weights...)
	args = append(args, "AGGREGATE", aggregate)

	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "ZUNIONSTORE", args...)
	}
	n, err = redis.Int(w.Do(action))
	return
}

// ----------- geo -----------------
func (w *RedisManager) GeoAdd(key string, lon, lat float64, member string) (add int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "GEOADD", key, lon, lat, member)
	}

	reply, err := w.Do(action)
	if err != nil {
		return
	}

	add, err = redis.Int(reply, err)
	return
}

func (w *RedisManager) GeoRadiusWithDist(key string, lon, lat float64, radius float64, unit string, count int) (members []string, result map[string]float64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "GEORADIUS", key, lon, lat, radius, unit, "WITHDIST", "COUNT", count)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}

	reply2, err := redis.Values(reply, err)
	if err != nil {
		return
	}

	replyList := []string{}
	for _, v := range reply2 {
		reply3, err1 := redis.Strings(v, err)
		if err1 != nil {
			err = err1
			return
		}
		replyList = append(replyList, reply3...)
	}
	if len(replyList)%2 != 0 {
		err = fmt.Errorf("invalid reply")
		return
	}

	members = make([]string, 0)
	result = make(map[string]float64)
	for i := 0; i < len(replyList); i += 2 {
		key := replyList[i]
		value, err := strconv.ParseFloat(replyList[i+1], 64)
		if err != nil {
			return nil, nil, err
		}
		result[key] = value
		members = append(members, key)
	}
	return
}

func (w *RedisManager) GeoRadiusWithDistByMember(key string, member string, radius float64, unit string, count int) (members []string, result map[string]float64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "GEORADIUSBYMEMBER", key, member, radius, unit, "WITHDIST", "COUNT", count)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}

	reply2, err := redis.Values(reply, err)
	if err != nil {
		return
	}

	replyList := []string{}
	for _, v := range reply2 {
		reply3, err1 := redis.Strings(v, err)
		if err1 != nil {
			err = err1
			return
		}
		replyList = append(replyList, reply3...)
	}
	if len(replyList)%2 != 0 {
		err = fmt.Errorf("invalid reply")
		return
	}

	members = make([]string, 0)
	result = make(map[string]float64)
	for i := 0; i < len(replyList); i += 2 {
		key := replyList[i]
		value, err := strconv.ParseFloat(replyList[i+1], 64)
		if err != nil {
			return nil, nil, err
		}
		result[key] = value
		members = append(members, key)
	}
	return
}

func (w *RedisManager) GeoDist(key string, memberA, memberB string, unit string) (dist string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "GEODIST", key, memberA, memberB, unit)
	}

	reply, err := w.Do(action)
	if err != nil {
		return
	}

	dist, err = redis.String(reply, err)
	return
}

// ----------- other ----------------
func (w *RedisManager) replyToMap(reply interface{}, param []string) (result map[string]string, err error) {
	replyList, err := redis.Strings(reply, err)
	if err != nil {
		return
	}
	result = make(map[string]string)
	if len(replyList) != len(param) {
		err = fmt.Errorf("RedisManager: hmget reply list not match ")
		return
	}
	for i, cur := range replyList {
		result[param[i]] = cur
	}

	return
}

func (w *RedisManager) replyToMapBytes(reply interface{}, param []string) (result map[string][]byte, err error) {
	replyList, err := redis.Bytess(reply, err)
	if err != nil {
		return
	}
	result = make(map[string][]byte)
	if len(replyList) != len(param) {
		err = fmt.Errorf("RedisManager: hmget reply list not match ")
		return
	}
	for i, cur := range replyList {
		result[param[i]] = cur
	}

	return
}

// ---------hlen------------------
func (w *RedisManager) Hlen(key string) (length int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "HLEN", key)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}
	if reply == nil {
		length = -1
		return
	}
	length, err = redis.Int64(reply, err)
	return
}

func (w *RedisManager) Rename(old, name string) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "RENAME", old, name)
	}
	reply, err = w.Do(action)
	return
}

// ----------- keys ----------------
func (w *RedisManager) Keys(pattern string) (keys []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "keys", pattern)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}
	if reply == nil {
		return
	}
	keys, err = redis.Strings(reply, err)
	return
}

// ----------- type ----------------
func (w *RedisManager) Type(key string) (result string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "type", key)
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}
	if reply == nil {
		return
	}
	result, err = redis.String(reply, err)
	return
}

// ----------- bitmap ----------------
func (w *RedisManager) SetBit(key string, offset, value uint32) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "setbit", key, offset, value)
	}
	return w.Do(action)
}

func (w *RedisManager) GetBit(key string, offset uint32) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "getbit", key, offset)
	}
	return w.Do(action)
}

// ----------- server ----------------
func (w *RedisManager) Time() (replyList []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "TIME")
	}
	reply, err := w.Do(action)

	if err != nil {
		return
	}

	replyList, err = redis.Strings(reply, err)
	if err != nil {
		return
	}

	if len(replyList) != 2 {
		err = fmt.Errorf("invalid reply")
		return
	}
	return
}

// ----------- scan -------------------
func (w *RedisManager) Scan(iter int64) (next int64, data []string, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "scan", iter)
	}
	reply, err := w.Do(action)
	if err != nil || reply == nil {
		return
	}
	slice, ok := reply.([]interface{})
	if !ok {
		err = fmt.Errorf("unexpect reply type")
		return
	}
	if len(slice) != 2 {
		err = fmt.Errorf("unexpect reply length")
		return
	}
	next, err = redis.Int64(slice[0], err)
	if err != nil {
		return
	}
	data, err = redis.Strings(slice[1], err)
	if err != nil {
		return
	}
	return
}

func (w *RedisManager) ScanMatchCount(iter int64, match string, count int64) (next int64, data []string, err error) {
	var action func(conn redis.Conn) (interface{}, error)
	if match != "" && count > 0 {
		action = func(conn redis.Conn) (interface{}, error) {
			return w.do(conn, "scan", iter, "MATCH", match, "COUNT", count)
		}
	} else if match != "" {
		action = func(conn redis.Conn) (interface{}, error) {
			return w.do(conn, "scan", iter, "MATCH", match)
		}
	} else if count > 0 {
		action = func(conn redis.Conn) (interface{}, error) {
			return w.do(conn, "scan", iter, "COUNT", count)
		}
	} else {
		action = func(conn redis.Conn) (interface{}, error) {
			return w.do(conn, "scan", iter)
		}
	}
	reply, err := w.Do(action)
	if err != nil || reply == nil {
		return
	}
	slice, ok := reply.([]interface{})
	if !ok {
		err = fmt.Errorf("unexpect reply type")
		return
	}
	if len(slice) != 2 {
		err = fmt.Errorf("unexpect reply length")
		return
	}
	next, err = redis.Int64(slice[0], err)
	if err != nil {
		return
	}
	data, err = redis.Strings(slice[1], err)
	if err != nil {
		return
	}
	return
}

// ----------- lua script -------------------
// !!!! NOTE: redis-cluster不支持
func (w *RedisManager) Eval(script string, keysNum int, args ...interface{}) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		a := append([]interface{}{script, keysNum}, args...)
		return w.do(conn, "EVAL", a...)
	}
	return w.Do(action)
}

// ----------- hyperloglog -----------------
func (w *RedisManager) PFAdd(key string, items ...interface{}) (reply interface{}, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "PFADD", key, items)
	}
	return w.Do(action)
}

func (w *RedisManager) PFCount(key string) (count int64, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "PFCOUNT", key)
	}
	reply, err := w.Do(action)
	if reply == nil {
		err = fmt.Errorf("reply eq nil")
		return
	}
	_, ok := reply.(int64)
	if !ok {
		err = fmt.Errorf("invalid valid")
		return
	}
	count = int64(reply.(int64))
	return
}

//--------------pipeline-----------------------
//notice:
//    cmds can not support "out" parameter inside. (Actually I am not sure if it is supportable.)
//    Hope someone find or confirm it.
func (w *RedisManager) PipelineRun(cmds [][]string) (resp []interface{}, err error) {
	begin := time.Now()
	conn := w.getConn()
	defer func() {
		if cerr := conn.Err(); cerr != nil {
			conn.Close()
			logger.Error("redis connection(%v) error:%v", w.host, cerr)
			nconn, cerr := w.dialRedis()
			if cerr == nil {
				conn = nconn
			}
		}
		w.putConn(conn)
		estr := parseErrForMetric(err)
		xcmetrics.HandleRedisRequest("unknown", w.normalizeSecName, "pipeline", time.Since(begin).Seconds()*1000, estr)
	}()

	cmd_cnt := 0
	for _, cmd := range cmds {
		if len(cmd) == 1 {
			conn.Send(cmd[0])
			cmd_cnt += 1
		} else if len(cmd) > 1 {
			paras := make([]interface{}, len(cmd)-1)
			for i, v := range cmd[1:] {
				paras[i] = v
			}
			conn.Send(cmd[0], paras...)
			cmd_cnt += 1
		} else {
			err := fmt.Errorf("%s", "cmd is empty")
			return nil, err
		}
	}
	conn.Flush()
	res := []interface{}{}
	for i := 0; i < cmd_cnt; i++ {
		r, err := conn.Receive()
		if err == nil {
			res = append(res, r)
		} else {
			return nil, err
		}
		xcmetrics.HandleRedisRequest("unknown", w.normalizeSecName, cmds[i][0], time.Since(begin).Seconds()*1000, "")
	}
	return res, nil
}

func (w *RedisManager) PipelineRun_v2(cmds [][]string) (resp []interface{}, err error) {
	begin := time.Now()
	defer func() {
		estr := parseErrForMetric(err)
		xcmetrics.HandleRedisRequest("unknown", w.normalizeSecName, "pipeline", time.Since(begin).Seconds()*1000, estr)
	}()
	action := func(conn redis.Conn) (interface{}, error) {
		cmd_cnt := 0
		for _, cmd := range cmds {
			if len(cmd) == 1 {
				conn.Send(cmd[0])
				cmd_cnt += 1
			} else if len(cmd) > 1 {
				paras := make([]interface{}, len(cmd)-1)
				for i, v := range cmd[1:] {
					paras[i] = v
				}
				conn.Send(cmd[0], paras...)
				cmd_cnt += 1
			} else {
				err := fmt.Errorf("%s", "cmd is empty")
				return nil, err
			}
		}
		conn.Flush()
		res := []interface{}{}
		for i := 0; i < cmd_cnt; i++ {
			r, err := conn.Receive()
			if err == nil {
				res = append(res, r)
			} else {
				return nil, err
			}
			xcmetrics.HandleRedisRequest("unknown", w.normalizeSecName, cmds[i][0], time.Since(begin).Seconds()*1000, "")
		}
		return res, nil
	}

	r, err := w.Do(action)
	if err != nil {
		return nil, err
	}

	res := r.([]interface{})
	return res, nil
}

func (w *RedisManager) PipelineTransaction(cmds [][]string) ([]interface{}, error) {
	conn := w.getConn()
	defer func() {
		if cerr := conn.Err(); cerr != nil {
			conn.Close()
			logger.Error("redis connection(%v) error:%v", w.host, cerr)
			nconn, cerr := w.dialRedis()
			if cerr == nil {
				conn = nconn
			}
		}
		w.putConn(conn)
	}()

	conn.Send("MULTI")
	for _, cmd := range cmds {
		if len(cmd) == 1 {
			conn.Send(cmd[0])
		} else if len(cmd) > 1 {
			paras := make([]interface{}, len(cmd)-1)
			for i, v := range cmd[1:] {
				paras[i] = v
			}
			conn.Send(cmd[0], paras...)
		} else {
			err := fmt.Errorf("%s", "cmd is empty")
			return nil, err
		}
	}
	r, err := redis.Values(conn.Do("exec"))
	return r, err
}

//--------------pubsub-----------------------
func (w *RedisManager) Publish(channel string, message interface{}) (count int, err error) {
	action := func(conn redis.Conn) (interface{}, error) {
		return w.do(conn, "publish", channel, message)
	}

	val, err := w.Do(action)
	if err != nil {
		return
	}
	if val == nil {
		return
	}

	return redis.Int(val, err)
}

func (w *RedisManager) Subscribe(channel string, closeChan chan bool) (messages chan []byte, err error) {
	conn, err := redis.Dial("tcp", w.host)
	if err != nil {
		return
	}
	if w.auth != "" {
		if _, err = conn.Do("AUTH", w.auth); err != nil {
			return
		}
	}

	psc := redis.PubSubConn{Conn: conn}
	err = psc.Subscribe(channel)
	if err != nil {
		return
	}

	raw := make(chan []byte)
	go func() {
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				raw <- v.Data
			case error:
				logger.Error("subscribe err: %v, channel: %s", v, channel)
				close(raw)
				return
			}
		}
	}()

	messages = make(chan []byte)
	go func() {
		defer psc.Close()
		defer close(messages)

		for {
			select {
			case message, ok := <-raw:
				if !ok {
					return
				}
				messages <- message
			case <-closeChan:
				return
			}
		}
	}()

	return
}

//-----transactions
func (w *RedisManager) GetConn() redis.Conn {
	return <-w.redisPool
}

func (w *RedisManager) PutConn(conn redis.Conn) {
	w.redisPool <- conn
}

func (w *RedisManager) CWatch(conn redis.Conn, keys []string) (reply interface{}, err error) {
	args := []interface{}{}
	for _, v := range keys {
		args = append(args, v)
	}

	reply, err = conn.Do("WATCH", args...)
	return
}

func (w *RedisManager) CMulti(conn redis.Conn) (reply interface{}, err error) {
	reply, err = conn.Do("MULTI")
	return
}

func (w *RedisManager) CExec(conn redis.Conn) (reply interface{}, err error) {
	reply, err = conn.Do("EXEC")
	return
}

func (w *RedisManager) CDiscard(conn redis.Conn) (reply interface{}, err error) {
	reply, err = conn.Do("DISCARD")
	return
}

func (w *RedisManager) CSetEx(conn redis.Conn, key string, value interface{}, expire int64) (reply interface{}, err error) {
	reply, err = conn.Do("SETEX", key, expire, value)
	return
}
