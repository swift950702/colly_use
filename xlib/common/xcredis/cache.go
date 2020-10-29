package xcredis

import (
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"

	"git.ixiaochuan.cn/xclib/common/config"
	"git.ixiaochuan.cn/xclib/common/logger"
	"git.ixiaochuan.cn/xclib/common/xccrypt"
	"git.ixiaochuan.cn/xclib/common/xckey"

	"cfgstruct"
)

const (
	//DEFAULT_POOLSIZE = 16
	DEFAULT_TIMEOUT        = 30 * time.Second
	DEFAULT_EXPIRE_TIMEOUT = 60 * 60 * 24 * 7
)

type Cache struct {
	mgr *RedisManager

	prefix   string
	timeout  time.Duration
	poolsize int
	secName  string
}

func NewCache(cfg *config.Config, secName string) (c *Cache, err error) {
	sec, err := cfg.GetSection(secName)
	if err != nil {
		return nil, err
	}
	auth, err := sec.GetValue("auth")
	if err != nil {
		return nil, err
	}
	poolsize, err := sec.GetIntValue("poolsize")
	if err != nil {
		return nil, err
	}
	hostport, err := sec.GetValue("hostport")
	if err != nil {
		return nil, err
	}
	return newCache(hostport, auth, secName, poolsize, DEFAULT_TIMEOUT)
}

func NewCacheV2(cfg cfgstruct.RedisSt, secName string) (c *Cache, err error) {
	c, err = newCache(cfg.Hostport, cfg.Auth, secName, cfg.Poolsize, DEFAULT_TIMEOUT)
	if err != nil {
		logger.Error("new cache, sec name %s, error: %v", secName, err)
		return
	}
	return
}

func (c *Cache) GetRedisManager() *RedisManager {
	return c.mgr
}

func newCache(host, auth, secName string, poolsize int, timeout time.Duration) (c *Cache, err error) {
	if auth != "" {
		auth, err = xccrypt.Decrypt(auth, xckey.AppKey)
		if err != nil {
			return
		}
	}
	if poolsize == 0 {
		poolsize = DEFAULT_POOLSIZE
	}
	mgr, err := NewRedisManagerWithSecName(host, auth, int(poolsize), timeout, secName)
	c = &Cache{
		mgr: mgr,

		timeout:  timeout,
		poolsize: poolsize,
		secName:  secName,
	}
	return
}

func NewCacheWithRedis(mgr *RedisManager) (c *Cache, err error) {
	c = &Cache{
		mgr: mgr,
	}
	return
}

func (c *Cache) SectionName() string {
	return c.secName
}

func (c *Cache) SetPrefix(prefix string) {
	c.prefix = prefix
}

func (c *Cache) ReplyAsInt(result interface{}) (n int, err error) {
	n, err = redis.Int(result, nil)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) ReplyAsInt64(result interface{}) (n int64, err error) {
	n, err = redis.Int64(result, nil)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) GetHData(key string, subKey string) (val []byte, err error) {
	redisKey := c.prefix + key
	result, err := c.mgr.Hget(redisKey, subKey)
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	val, err = redis.Bytes(result, err)
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return nil, err
	}

	// logger.Debug("GetHData redisKey: %v, subKey: %v, val: %v, err: %v", redisKey, subKey, len(val), err)
	return val, nil
}

func (c *Cache) GetHKeys(key string) (result []string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Hkeys(redisKey)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("GetHKeys redisKey: %v, result: %v, err: %v", redisKey, result, err)
	return
}

func (c *Cache) SetHData(key string, subKey string, val []byte) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.Hset(redisKey, subKey, val)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("SetHData redisKey: %v, subKey: %v, val: %v, err: %v", redisKey, subKey, len(val), err)
	return err
}

func (c *Cache) MSetHData(key string, subKeyValue map[string]interface{}) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.Hmset(redisKey, subKeyValue)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("MSetHData redisKey: %v, subKeyValue: %v, err: %v", redisKey, subKeyValue, err)
	return err
}

func (c *Cache) DelHData(key string, subKey string) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.HDel(redisKey, subKey)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("DelHData redisKey: %v, subKey: %v, err: %v", redisKey, subKey, err)
	return err
}

func (c *Cache) Expire(key string, timeout int64) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.Expire(redisKey, timeout)
	if err == redis.ErrNil {
		err = nil
	}
	return err
}

func (c *Cache) ExpireRet(key string, timeout int64) (set bool, err error) {
	redisKey := c.prefix + key
	rep, err := c.mgr.Expire(redisKey, timeout)
	if err == redis.ErrNil {
		err = nil
		return
	}

	n, _ := redis.Int(rep, nil)
	set = n == 1
	return
}

func (c *Cache) SetWithExpire(key string, val interface{}, expire int64) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.SetEx(redisKey, val, expire)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) SetExNx(key string, val interface{}, expire int64) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.SetExNx(redisKey, val, expire)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) SetXx(key string, val interface{}, expire int64) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.SetXx(redisKey, val, expire)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) SetExXx(key string, val interface{}, expire int64) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.SetExXx(redisKey, val, expire)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) Del(key string) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.Del(redisKey)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("redisKey: %v, err: %v", redisKey, err)
	return err
}

func (c *Cache) Exists(key string) (ok bool, err error) {
	redisKey := c.prefix + key
	ok, err = c.mgr.Exists(redisKey)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("Exists redisKey: %v, ok: %v, err: %v", redisKey, ok, err)
	return
}

func (c *Cache) TTL(key string) (ttl int64, err error) {
	redisKey := c.prefix + key
	ttl, err = c.mgr.TTL(redisKey)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) LPush(key string, val interface{}) (err error) {
	redisKey := c.prefix + key
	err = c.mgr.Lpush(redisKey, val)
	if err == redis.ErrNil {
		err = nil
	}
	return err
}

func (c *Cache) LPushMulti(key string, val ...interface{}) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.LpushMulti(redisKey, val...)
	if err == redis.ErrNil {
		err = nil
	}
	return err
}

func (c *Cache) LPushWithQueueName(queueName string, val interface{}) (err error) {
	err = c.mgr.Lpush(queueName, val)
	if err == redis.ErrNil {
		err = nil
	}
	return err
}

func (c *Cache) LPop(key string) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Lpop(redisKey)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) RPop(key string) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Rpop(redisKey)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) BLPop(key string, timeout int) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Blpop(redisKey, timeout)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) BRPop(key string, timeout int) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Brpop(redisKey, timeout)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) RPoplpush(key1 string, key2 string) (result interface{}, err error) {
	redisKey1 := c.prefix + key1
	redisKey2 := c.prefix + key2
	result, err = c.mgr.Rpoplpush(redisKey1, redisKey2)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) LLen(key string) (length int, err error) {
	redisKey := c.prefix + key
	length, err = c.mgr.LLen(redisKey)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) HExists(key, subkey string) (ok bool, err error) {
	redisKey := c.prefix + key
	ok, err = c.mgr.HExists(redisKey, subkey)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) HSet(key, subkey string, value interface{}) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Hset(redisKey, subkey, value)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) HMSet(key string, param map[string]interface{}) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Hmset(redisKey, param)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) HGet(key string, subkey string) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Hget(redisKey, subkey)
	if err == redis.ErrNil {
		err = nil
	}
	// logger.Debug("HGET redisKey: %v, result: %v, err: %v", redisKey, result, err)
	return
}

func (c *Cache) HGetInt64(key string, subkey string) (result int64, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.HgetInt64(redisKey, subkey)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) HGetInt(key string, subkey string) (result int, err error) {
	redisKey := c.prefix + key
	result, err = redis.Int(c.mgr.Hget(redisKey, subkey))
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) HMGet(key string, param []string) (result map[string]string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Hmget(redisKey, param)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) HDel(key, subkey string) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.HDel(redisKey, subkey)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) HGetAll(key string) (result map[string]string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.HgetAll(redisKey)
	if err == redis.ErrNil {
		err = nil
	}
	if result == nil {
		result = make(map[string]string)
	}
	// logger.Debug("HGETALL redisKey: %v, result: %v, err: %v", redisKey, result, err)
	return
}

func (c *Cache) Hincrby(key string, subkey string, inc int64) (result int64, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Hincrby(redisKey, subkey, inc)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) Hlen(key string) (result int64, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Hlen(redisKey)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) Incr(key string) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.Incr(redisKey)
	if err == redis.ErrNil {
		err = nil
	}
	return err
}

func (c *Cache) IncrRet(key string) (n int64, err error) {
	redisKey := c.prefix + key
	reply, err := c.mgr.Incr(redisKey)
	return redis.Int64(reply, err)
}

func (c *Cache) Incrby(key string, increment int64) (n int64, err error) {
	redisKey := c.prefix + key
	reply, err := c.mgr.Incrby(redisKey, increment)
	return redis.Int64(reply, err)
}

func (c *Cache) GetInt64(key string) (ret int64, err error) {
	redisKey := c.prefix + key
	val, err := c.mgr.Get(redisKey)
	ret, err = redis.Int64(val, err)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) GetString(key string) (ret string, err error) {
	redisKey := c.prefix + key
	return c.mgr.GetString(redisKey)
}

func (c *Cache) Set(key, val string) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.Set(redisKey, val)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) MSet(para map[string]interface{}) (err error) {
	newPara := make(map[string]interface{})
	for k, v := range para {
		key := c.prefix + k
		newPara[key] = v
	}
	_, err = c.mgr.MSet(newPara)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) MGet(keys []string) (rtn map[string]string, err error) {
	newKeys := make([]string, len(keys), len(keys))
	for i, k := range keys {
		key := c.prefix + k
		newKeys[i] = key
	}
	result, err := c.mgr.MGet(newKeys)
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return
	}
	if result == nil {
		result = map[string]string{}
	}
	if c.prefix == "" {
		return result, nil
	}

	rtn = make(map[string]string)
	for k, v := range result {
		key := strings.TrimPrefix(k, c.prefix)
		rtn[key] = v
	}
	return
}

func (c *Cache) MGetBytes(keys []string) (rtn map[string][]byte, err error) {
	newKeys := make([]string, len(keys), len(keys))
	for i, k := range keys {
		key := c.prefix + k
		newKeys[i] = key
	}
	result, err := c.mgr.MGetBytes(newKeys)
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		logger.Error("failed to db MGetBytes: %s", err.Error())
		return
	}
	if result == nil {
		result = map[string][]byte{}
	}
	if c.prefix == "" {
		return result, nil
	}

	rtn = make(map[string][]byte)
	for k, v := range result {
		key := strings.TrimPrefix(k, c.prefix)
		rtn[key] = v
	}
	return
}

func (c *Cache) SetBit(key string, offset, value uint32) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.SetBit(redisKey, offset, value)
	return
}

func (c *Cache) GetBit(key string, offset uint32) (result bool, err error) {
	redisKey := c.prefix + key
	reply, err := c.mgr.GetBit(redisKey, offset)
	if err == redis.ErrNil {
		err = nil
	}
	result, err = redis.Bool(reply, err)
	if err == redis.ErrNil {
		err = nil
	}
	return
}

func (c *Cache) GetData(key string) (val []byte, err error) {
	redisKey := c.prefix + key
	result, err := c.mgr.Get(redisKey)
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	val, err = redis.Bytes(result, err)
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return nil, err
	}

	// logger.Debug("GetData redisKey: %v, val: %v, err: %v", redisKey, len(val), err)
	return val, nil
}

func (c *Cache) RPush(key string, val interface{}) (err error) {
	redisKey := c.prefix + key
	err = c.mgr.Rpush(redisKey, val)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("RPush redisKey: %v, val: %v, err: %v", redisKey, val, err)
	return
}

func (c *Cache) RPushMulti(key string, val ...interface{}) (err error) {
	redisKey := c.prefix + key
	_, err = c.mgr.RpushMulti(redisKey, val...)
	if err == redis.ErrNil {
		err = nil
	}
	return err
}

func (c *Cache) LRange(key string, start, end int) (result []interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.LrangeValues(redisKey, start, end)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("LRange redisKey: %v [%d , %d], val: %v, err: %v", redisKey, start, end, len(result), err)
	return
}

func (c *Cache) Sismember(key string, member interface{}) (result bool, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Sismember(redisKey, member)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("Sismember redisKey: %v, member: %v, result: %v, err: %v", redisKey, member, result, err)
	return
}

func (c *Cache) Sadd(key string, member ...interface{}) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Sadd(redisKey, member...)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("Sadd redisKey: %v, member: %v, result: %v, err: %v", redisKey, member, result, err)
	return
}

func (c *Cache) Srem(key string, member ...interface{}) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Srem(redisKey, member...)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("Srem redisKey: %v, member: %v, result: %v, err: %v", redisKey, member, result, err)
	return
}

func (c *Cache) Scard(key string) (result int64, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Scard(redisKey)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("Scard redisKey: %v, result: %v, err: %v", redisKey, result, err)
	return
}

func (c *Cache) Smembers(key string) (result []string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Smembers(redisKey)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("Smembers redisKey: %v, result: %v, err: %v", redisKey, result, err)
	return
}

func (c *Cache) Srandmember(key string, count int64) (result []string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Srandmember(redisKey, count)
	if err == redis.ErrNil {
		err = nil
	}

	// logger.Debug("Srandmember redisKey: %v, count: %v, result: %v, err: %v", redisKey, count, result, err)
	return
}

func (c *Cache) GetWithErrNil(key string) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Get(redisKey)
	// logger.Debug("GetWithErrNil redisKey: %v, val: %v, err: %v", redisKey, result, err)
	return
}

func (c *Cache) ZaddMulti(key string, args ...interface{}) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.ZaddMulti(redisKey, args...)
	// logger.Debug("ZaddMulti redisKey: %v, args: %v, result: %v, err: %v", redisKey, args, result, err)
	return
}

func (c *Cache) Zrem(key string, member string) (result int, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Zrem(redisKey, member)
	// logger.Debug("Zrem redisKey: %v, member: %v, result: %v, err: %v", redisKey, member, result, err)
	return
}

func (c *Cache) Zrange(key string, start, end int) (result []string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Zrange(redisKey, start, end)
	// logger.Debug("Zrange redisKey: %v, start: %v, end: %v, result: %v, err: %v", redisKey, start, end, result, err)
	return
}

func (c *Cache) ZrangeWithScore(key string, start, end int) (list []string, scores map[string]float64, err error) {
	redisKey := c.prefix + key
	list, scores, err = c.mgr.ZrangeWithScore(redisKey, start, end)
	// logger.Debug("ZrangeWithScore redisKey: %v, start: %v, end: %v, list: %v, scores: %v, err: %v", redisKey, start, end, list, scores, err)
	return
}

func (c *Cache) Zrevrange(key string, start, end int) (result []string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Zrevrange(redisKey, start, end)
	// logger.Debug("Zrevrange redisKey: %v, start: %v, end: %v, result: %v, err: %v", redisKey, start, end, result, err)
	return
}

func (c *Cache) ZrevrangeWithScore(key string, start, end int) (result map[string]float64, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.ZrevrangeWithScore(redisKey, start, end)
	// logger.Debug("ZrevrangeWithScore redisKey: %v, start: %v, end: %v, result: %v, err: %v", redisKey, start, end, result, err)
	return
}

func (c *Cache) ZrevrangeByScoreWithScore(key string, max, min, offset, count int) (list []string, scores map[string]float64, err error) {
	redisKey := c.prefix + key
	list, scores, err = c.mgr.ZrevrangeByScoreWithScore(redisKey, max, min, offset, count)
	// logger.Debug("ZrevrangeByScoreWithScore redisKey: %v, max: %v, min: %v, offset: %v, count: %v, list: %v, scores: %v, err: %v", redisKey, max, min, offset, count, list, scores, err)
	return
}

func (c *Cache) ZrevrangeByScoreWithScoreStr(key string, max, min string, offset, count int) (list []string, scores map[string]float64, err error) {
	redisKey := c.prefix + key
	list, scores, err = c.mgr.ZrevrangeByScoreWithScoreStr(redisKey, max, min, offset, count)
	// logger.Debug("ZrevrangeByScoreWithScore redisKey: %v, max: %v, min: %v, offset: %v, count: %v, list: %v, scores: %v, err: %v", redisKey, max, min, offset, count, list, scores, err)
	return
}

func (c *Cache) ZrangeByScore(key string, min, max, offset, limit int) (result []string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.ZrangeByScore(redisKey, min, max, offset, limit)
	// logger.Debug("ZrangeByScore redisKey: %v, max: %v, min: %v, offset: %v, limit: %v, result: %v, err: %v", redisKey, max, min, offset, limit, result, err)
	return
}

func (c *Cache) ZrevrangeByScore(key string, max, min, offset, limit int) (result []string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.ZrevrangeByScore(redisKey, max, min, offset, limit)
	return
}

func (c *Cache) ZrangeByScoreWithScore(key string, min, max, offset, count int) (list []string, scores map[string]float64, err error) {
	redisKey := c.prefix + key
	list, scores, err = c.mgr.ZrangeByScoreWithScore(redisKey, min, max, offset, count)
	// logger.Debug("ZrangeByScoreWithScore redisKey: %v, max: %v, min: %v, offset: %v, count: %v, list: %v, scores: %v, err: %v", redisKey, max, min, offset, count, list, scores, err)
	return
}

func (c *Cache) ZrangeByScoreWithScoreStr(key string, min, max string, offset, count int) (list []string, scores map[string]float64, err error) {
	redisKey := c.prefix + key
	list, scores, err = c.mgr.ZrangeByScoreWithScoreStr(redisKey, min, max, offset, count)
	// logger.Debug("ZrangeByScoreWithScore redisKey: %v, max: %v, min: %v, offset: %v, count: %v, list: %v, scores: %v, err: %v", redisKey, max, min, offset, count, list, scores, err)
	return
}

func (c *Cache) ZrangeByScoreWithPrefix(key string, prefixMin string, min int, prefixMax string, max, offset, limit int) (result []string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.ZrangeByScoreWithPrefix(redisKey, prefixMin, min, prefixMax, max, offset, limit)
	// logger.Debug("ZrangeByScoreWithPrefix redisKey: %v, prefixMin: %v, min: %v, prefixMax: %v, max: %v, offset: %v, limit: %v, result: %v, err: %v", redisKey, prefixMin, min, prefixMax, max, offset, limit, result, err)
	return
}

func (c *Cache) Zcount(key string, minScore interface{}, maxScore interface{}) (result int, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Zcount(redisKey, minScore, maxScore)
	// logger.Debug("Zcount redisKey: %v, minScore: %v, maxScore: %v, result: %v, err: %v", redisKey, minScore, maxScore, result, err)
	return
}

func (c *Cache) Zscore(key string, member string) (result string, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Zscore(redisKey, member)
	// logger.Debug("Zscore redisKey: %v, member: %v, result: %v, err: %v", redisKey, member, result, err)
	return
}

func (c *Cache) Zcard(key string) (result int, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Zcard(redisKey)
	// logger.Debug("Zcard redisKey: %v, result: %v, err: %v", redisKey, result, err)
	return
}

func (c *Cache) Zrevrank(key string, member string) (result int64, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Zrevrank(redisKey, member)
	// logger.Debug("Zrevrank redisKey: %v, result: %v, err: %v", redisKey, result, err)
	return
}

func (c *Cache) Zremrangebyrank(key string, start int, stop int) (result int, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Zremrangebyrank(redisKey, start, stop)
	// logger.Debug("Zremrangebyrank redisKey: %v, start: %v, stop: %v, result: %v, err: %v", redisKey, start, stop, result, err)
	return
}

func (c *Cache) Zremrangebyscore(key string, start int, stop int) (result int, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.Zremrangebyscore(redisKey, start, stop)
	// logger.Debug("Zremrangebyscore redisKey: %v, start: %v, stop: %v, result: %v, err: %v", redisKey, start, stop, result, err)
	return
}

func (c *Cache) ZUnionStore(key string, keys []string, weights []interface{}, aggregate string) (result int, err error) {
	redisKey := c.prefix + key
	for i, k := range keys {
		keys[i] = c.prefix + k
	}
	result, err = c.mgr.ZUnionStore(redisKey, keys, weights, aggregate)
	// logger.Debug("ZUnionStore redisKey: %v, keys: %v, weights: %v, aggregate: %v, result: %v, err: %v", redisKey, keys, weights, aggregate, result, err)
	return
}

func (c *Cache) Eval(script string, keysNum int, args ...interface{}) (result interface{}, err error) {
	result, err = c.mgr.Eval(script, keysNum, args...)
	if err == redis.ErrNil {
		err = nil
	}

	return
}

func (c *Cache) Subscribe(channel string, closeChan chan bool) (messages chan []byte, err error) {
	channel = c.prefix + channel
	return c.mgr.Subscribe(channel, closeChan)
}

func (c *Cache) Publish(channel string, message interface{}) (count int, err error) {
	channel = c.prefix + channel
	return c.mgr.Publish(channel, message)
}

// ---- geo ----
func (c *Cache) GeoAdd(key string, lon, lat float64, member string) (result interface{}, err error) {
	redisKey := c.prefix + key
	result, err = c.mgr.GeoAdd(redisKey, lon, lat, member)
	return
}

func (c *Cache) GeoRadiusWithDist(key string, lon, lat float64, radius float64, unit string, count int) (members []string, result map[string]float64, err error) {
	redisKey := c.prefix + key
	members, result, err = c.mgr.GeoRadiusWithDist(redisKey, lon, lat, radius, unit, count)
	return
}
