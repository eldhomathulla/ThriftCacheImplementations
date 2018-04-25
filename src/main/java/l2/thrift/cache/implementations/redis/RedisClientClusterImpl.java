package l2.thrift.cache.implementations.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.BinaryClient.LIST_POSITION;

public class RedisClientClusterImpl implements RedisClient {
	private JedisCluster jedisCluster;

	public RedisClientClusterImpl(JedisCluster jedisCluster) {
		this.jedisCluster = jedisCluster;
	}

	@Override
	public Long hdel(String key, String... field) {
		return this.jedisCluster.hdel(key, field);
	}

	@Override
	public Long del(String key) {
		return this.jedisCluster.del(key);
	}

	@Override
	public String flushDB() throws RedisFunctionNotImplementedException {
		throw new RedisFunctionNotImplementedException("flushDB Function has not been implemented");
	}

	@Override
	public Set<String> keys(String pattern) throws RedisFunctionNotImplementedException {
		throw new RedisFunctionNotImplementedException("keys Function has not been implemented");	}

	@Override
	public Map<String, String> hgetAll(String key) {
		return this.jedisCluster.hgetAll(key);
	}

	@Override
	public Long hset(String key, String field, String value) {
		return this.jedisCluster.hset(key, field, value);
	}

	@Override
	public String hget(String key, String field) {
		return this.jedisCluster.hget(key, field);
	}

	@Override
	public Long llen(String key) {
		return this.jedisCluster.llen(key);
	}

	@Override
	public List<String> lrange(String key, long start, long end) {
		return this.jedisCluster.lrange(key, start, end);
	}

	@Override
	public Long rpush(String key, String... strings) {
		return this.jedisCluster.rpush(key, strings);
	}

	@Override
	public Long lrem(String key, long count, String value) {
		return this.jedisCluster.lrem(key, count, value);
	}

	@Override
	public String lindex(String key, long index) {
		return this.jedisCluster.lindex(key, index);
	}

	@Override
	public String lset(String key, long index, String value) {
		return this.jedisCluster.lset(key, index, value);
	}

	@Override
	public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
		return this.jedisCluster.linsert(key, where, pivot, value);
	}

}
