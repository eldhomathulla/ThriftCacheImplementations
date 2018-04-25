package l2.thrift.cache.implementations.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;

public interface RedisClient {
	

	Long hdel(String key, String... field);

	Long del(String key);

	String flushDB() throws RedisFunctionNotImplementedException;

	Set<String> keys(String pattern) throws RedisFunctionNotImplementedException;

	Map<String, String> hgetAll(String key);

	Long hset(String key, String field, String value);

	String hget(String key, String field);

	Long llen(String key);

	List<String> lrange(String key, long start, long end);

	Long rpush(String key, String... strings);

	Long lrem(String key, long count, String value);

	String lindex(String key, long index);

	String lset(String key, long index, String value);

	Long linsert(String key, LIST_POSITION where, String pivot, String value);
	

}
