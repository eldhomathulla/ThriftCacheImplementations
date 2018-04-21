package l2.thrift.cache.implementations;


import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.cache.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class RedisThriftCache extends ThriftDefaultCache {
	private JedisPool jedisPool;

	public RedisThriftCache(CacheConfiguration cacheConfiguration) {
		super(cacheConfiguration);
	}
	
	public RedisThriftCache(CacheConfiguration cacheConfiguration,JedisPool jedisPool) {
		this(cacheConfiguration);
		this.jedisPool=jedisPool;
	}

	@Override
	public void delete(TCacheKey tCacheKey) throws TException {
		jedisPool.getResource().hdel(tCacheKey.functionName(), serialize(tCacheKey));
	}

	@Override
	public void delete(TCacheKey tCacheKey, boolean partial) throws TException {
		if (partial) {
			jedisPool.getResource().del(tCacheKey.functionName());
		} else {
			delete(tCacheKey);
		}
	}

	@Override
	public void deleteAll() throws TException {
		jedisPool.getResource().flushDB();
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Map<TCacheKey, TBase> readAll() throws TException {
		Jedis jedis=jedisPool.getResource();
		jedis.keys("*").stream().reduce(new HashMap<>(), (Map<TCacheKey, TBase> map,String functionName)->{
			map.putAll(fetchFunctionCacheEntries(functionName));
			return map;
		}
		, (Map<TCacheKey, TBase> map1,Map<TCacheKey, TBase> map2)->{
			map1.putAll(map2);
			return map1;
		});
		return null;
	}
	
	@SuppressWarnings("rawtypes")
	private Map<TCacheKey,TBase> fetchFunctionCacheEntries(String functionName){
		Jedis jedis=jedisPool.getResource();
		return jedis.hgetAll(functionName).entrySet().stream().reduce(new HashMap<TCacheKey,TBase>(), (HashMap<TCacheKey,TBase> map,Entry<String,String> entry)->{
			map.put(deSerializeTCacheKey(entry.getKey()), deSerializeTBase(entry.getValue()));
			return map;
		}, (HashMap<TCacheKey,TBase> map1,HashMap<TCacheKey,TBase> map2)->{
			map1.putAll(map2);
			return map1;
		});
	}

	
	@Override
	@SuppressWarnings("rawtypes")
	public Map<TCacheKey,TBase> readFromPartialKey(TCacheKey tCacheKey) throws TException {
		return fetchFunctionCacheEntries(tCacheKey.functionName());
	}
	
	@Override
	protected void writeToCache(TCacheKey key, @SuppressWarnings("rawtypes") TBase value) throws TException {
		jedisPool.getResource().hset(key.functionName(), serialize(key), serialize(value));
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	protected TBase readFromCache(TCacheKey key) throws TException {
		return deSerializeTBase(jedisPool.getResource().hget(key.functionName(), serialize(key)));
	}
}
