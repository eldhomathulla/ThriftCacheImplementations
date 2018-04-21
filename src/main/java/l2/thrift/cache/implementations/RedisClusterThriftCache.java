package l2.thrift.cache.implementations;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.cache.CacheConfiguration;
import org.apache.thrift.cache.TCacheFunctionNotImplementedException;
import org.apache.thrift.cache.TCacheKey;

import redis.clients.jedis.JedisCluster;

public class RedisClusterThriftCache extends ThriftDefaultCache {
	private JedisCluster jedisCluster;

	public RedisClusterThriftCache(CacheConfiguration cacheConfiguration, JedisCluster jedisCluster) {
		super(cacheConfiguration);
		this.jedisCluster=jedisCluster;
	}
	@Override
	public void delete(TCacheKey tCacheKey) throws TException {
		jedisCluster.hdel(tCacheKey.functionName(), serialize(tCacheKey));
	}

	@Override
	public void delete(TCacheKey tCacheKey, boolean partial) throws TException {
		if (partial) {
			jedisCluster.del(tCacheKey.functionName());
		} else {
			delete(tCacheKey);
		}
	}

	@Override
	public void deleteAll() throws TException {
		throw new TCacheFunctionNotImplementedException("deleteAll Function has not been implemented");
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Map<TCacheKey, TBase> readAll() throws TException {
		throw new TCacheFunctionNotImplementedException("readAll Function has not been implemented");
	}
	
	@SuppressWarnings("rawtypes")
	private Map<TCacheKey,TBase> fetchFunctionCacheEntries(String functionName){
		return jedisCluster.hgetAll(functionName).entrySet().stream().reduce(new HashMap<TCacheKey,TBase>(), (HashMap<TCacheKey,TBase> map,Entry<String,String> entry)->{
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
		jedisCluster.hset(key.functionName(), serialize(key), serialize(value));
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	protected TBase readFromCache(TCacheKey key) throws TException {
		return deSerializeTBase(jedisCluster.hget(key.functionName(), serialize(key)));
	}


}
