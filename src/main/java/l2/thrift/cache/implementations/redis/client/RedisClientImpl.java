package l2.thrift.cache.implementations.redis.client;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import l2.thrift.cache.implementations.redis.RedisFunctionNotImplementedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.BinaryClient.LIST_POSITION;

public abstract class RedisClientImpl implements RedisClient {

	protected abstract Jedis getResource();

	private <T> T doAction(Function<Jedis, T> action) {
		try (Jedis jedis = getResource();) {
			return action.apply(jedis);
		}
	}

	@Override
	public Long hdel(String key, String... field) {
		return doAction((Jedis jedis) -> jedis.hdel(key, field));
	}

	@Override
	public Long del(String key) {
		return doAction((Jedis jedis) -> jedis.del(key));
	}

	@Override
	public String flushDB() throws RedisFunctionNotImplementedException {
		return doAction((Jedis jedis) -> jedis.flushDB());
	}

	@Override
	public Set<String> keys(String pattern) throws RedisFunctionNotImplementedException {
		return doAction((Jedis jedis) -> jedis.keys(pattern));
	}

	@Override
	public Map<String, String> hgetAll(String key) {
		return doAction((Jedis jedis) -> jedis.hgetAll(key));
	}

	@Override
	public Long hset(String key, String field, String value) {
		return doAction((Jedis jedis) -> jedis.hset(key, field, value));
	}

	@Override
	public String hget(String key, String field) {
		return doAction((Jedis jedis) -> jedis.hget(key, field));
	}

	@Override
	public Long llen(String key) {
		return doAction((Jedis jedis) -> jedis.llen(key));
	}

	@Override
	public List<String> lrange(String key, long start, long end) {
		return doAction((Jedis jedis) -> jedis.lrange(key, start, end));
	}

	@Override
	public Long rpush(String key, String... strings) {
		return doAction((Jedis jedis) -> jedis.rpush(key, strings));
	}

	@Override
	public Long lrem(String key, long count, String value) {
		return doAction((Jedis jedis) -> jedis.lrem(key, count, value));
	}

	@Override
	public String lindex(String key, long index) {
		return doAction((Jedis jedis) -> jedis.lindex(key, index));
	}

	@Override
	public String lset(String key, long index, String value) {
		return doAction((Jedis jedis) -> jedis.lset(key, index, value));
	}

	@Override
	public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
		return doAction((Jedis jedis) -> jedis.linsert(key, where, pivot, value));

	}

}
