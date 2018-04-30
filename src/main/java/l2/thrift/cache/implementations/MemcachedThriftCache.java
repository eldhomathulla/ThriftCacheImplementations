package l2.thrift.cache.implementations;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.cache.CacheConfiguration;
import org.apache.thrift.cache.TCacheFunctionNotImplementedException;
import org.apache.thrift.cache.TCacheKey;

import net.spy.memcached.MemcachedClient;

public class MemcachedThriftCache extends ThriftDefaultCache {
	private MemcachedClient memcachedClient;
	private int exp = 5000;

	public MemcachedThriftCache(CacheConfiguration cacheConfiguration, MemcachedClient memcachedClient, int exp,
			Object... ifaces) {
		super(cacheConfiguration, ifaces);
		this.memcachedClient = memcachedClient;
		this.exp = exp;
	}

	@Override
	public void delete(TCacheKey key) throws TException {
		memcachedClient.delete(serialize(key));
	}

	@Override
	public void delete(TCacheKey key, boolean partial) throws TException {
		delete(key);
	}

	@Override
	public void deleteAll() throws TException {
		memcachedClient.flush();
	}

	@Override
	public Map<TCacheKey, TBase> readAll() throws TException {
		throw new TCacheFunctionNotImplementedException("readAll Function has not been implemented");
	}

	@Override
	public Map<TCacheKey, TBase> readFromPartialKey(TCacheKey partialKey) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	protected void writeToCache(TCacheKey key, TBase value) throws TException {
		LOGGER.info("writing to cache : " + key);
		memcachedClient.set(serialize(key), exp, serialize(value));
	}

	protected TBase readFromCache(TCacheKey key) throws TException {
		LOGGER.info("Reading from cache: " + key);
		long before = System.currentTimeMillis();
		try {
			return deSerializeTBase((String) memcachedClient.get(serialize(key)));
		} finally {
			System.out.println("key: " + (System.currentTimeMillis() - before));
		}
	}

	@Override
	public void postProcess(TCacheKey tCacheKey, String iFaceClassName, String processFunctionClassName,
			String argsClassName) throws TException {
	}

	@Override
	public TBase read(TCacheKey key, Supplier<TBase> getResult) throws TException {
		TBase result = readFromCache(key);
		if (result == null) {
			result = getResult.get();
			write(key, result);
		}
		return result;
	}

}
