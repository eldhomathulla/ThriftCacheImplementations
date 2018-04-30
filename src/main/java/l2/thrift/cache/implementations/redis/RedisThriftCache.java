package l2.thrift.cache.implementations.redis;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.cache.*;

import l2.thrift.cache.implementations.ThriftDefaultCache;
import l2.thrift.cache.implementations.redis.client.RedisClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

public class RedisThriftCache extends ThriftDefaultCache {
	private RedisClient redisClient;

	public RedisThriftCache(CacheConfiguration cacheConfiguration, RedisClient redisClient) {
		super(cacheConfiguration);
		this.redisClient = redisClient;
	}

	public RedisThriftCache(CacheConfiguration cacheConfiguration, RedisClient redisClient, Object... ifaces) {
		super(cacheConfiguration, ifaces);
		this.redisClient = redisClient;
	}

	public RedisThriftCache(CacheConfiguration cacheConfiguration, RedisClient redisClient,
			ThriftLockFactory thriftLockFactory,
			Function<String, List<DependentFunctionActionHolder>> dependentFunctionActionHolderListSupplier,
			Object... ifaces) {
		super(cacheConfiguration, thriftLockFactory, dependentFunctionActionHolderListSupplier, ifaces);
		this.redisClient = redisClient;
	}

	public RedisThriftCache(CacheConfiguration cacheConfiguration, RedisClient redisClient,
			ThriftLockFactory thriftLockFactory, Object... ifaces) {
		this(cacheConfiguration, redisClient, thriftLockFactory,
				(String name) -> new RedisList<DependentFunctionActionHolder>(name, redisClient,
						DependentFunctionActionHolder.class),
				ifaces);
	}

	@Override
	public void delete(TCacheKey tCacheKey) throws TException {
		redisClient.hdel(tCacheKey.functionName(), serialize(tCacheKey));
	}

	@Override
	public void delete(TCacheKey tCacheKey, boolean partial) throws TException {
		if (partial) {
			redisClient.del(tCacheKey.functionName());
		} else {
			delete(tCacheKey);
		}
	}

	@Override
	public void deleteAll() throws TException {
		try {
			redisClient.flushDB();
		} catch (RedisFunctionNotImplementedException e) {
			throw new TCacheFunctionNotImplementedException("deleteAll has not been implemented", e);
		}
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Map<TCacheKey, TBase> readAll() throws TException {
		try {
			redisClient.keys("*").stream().reduce(new HashMap<>(), (Map<TCacheKey, TBase> map, String functionName) -> {
				map.putAll(fetchFunctionCacheEntries(functionName));
				return map;
			}, (Map<TCacheKey, TBase> map1, Map<TCacheKey, TBase> map2) -> {
				map1.putAll(map2);
				return map1;
			});
		} catch (RedisFunctionNotImplementedException e) {
			throw new TCacheFunctionNotImplementedException("readAll has not been implemented", e);
		}
		return null;
	}

	@SuppressWarnings("rawtypes")
	private Map<TCacheKey, TBase> fetchFunctionCacheEntries(String functionName) {
		return redisClient.hgetAll(functionName).entrySet().stream().reduce(new HashMap<TCacheKey, TBase>(),
				(HashMap<TCacheKey, TBase> map, Entry<String, String> entry) -> {
					map.put(deSerializeTCacheKey(entry.getKey()), deSerializeTBase(entry.getValue()));
					return map;
				}, (HashMap<TCacheKey, TBase> map1, HashMap<TCacheKey, TBase> map2) -> {
					map1.putAll(map2);
					return map1;
				});
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Map<TCacheKey, TBase> readFromPartialKey(TCacheKey tCacheKey) throws TException {
		return fetchFunctionCacheEntries(tCacheKey.functionName());
	}

	@Override
	protected void writeToCache(TCacheKey key, @SuppressWarnings("rawtypes") TBase value) throws TException {
		redisClient.hset(key.functionName(), serialize(key), serialize(value));
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected TBase readFromCache(TCacheKey key) throws TException {
		return deSerializeTBase(redisClient.hget(key.functionName(), serialize(key)));
	}
}
