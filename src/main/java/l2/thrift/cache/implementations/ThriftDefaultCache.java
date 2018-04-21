package l2.thrift.cache.implementations;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.cache.CacheConfiguration;
import org.apache.thrift.cache.DefaultCache;
import org.apache.thrift.cache.TCacheKey;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;

public class ThriftDefaultCache extends DefaultCache {
	protected static final Logger LOGGER = Logger.getLogger(RedisThriftCache.class.getCanonicalName());

	private ObjectMapper objectMapper;

	public ThriftDefaultCache(CacheConfiguration cacheConfiguration) {
		super(cacheConfiguration);
		objectMapper=new ObjectMapper();
		objectMapper.enableDefaultTyping(DefaultTyping.NON_FINAL);
		objectMapper.setVisibility(PropertyAccessor.ALL,Visibility.NONE).setVisibility(PropertyAccessor.FIELD, Visibility.PUBLIC_ONLY).setVisibility(PropertyAccessor.GETTER, Visibility.PUBLIC_ONLY).setVisibility(PropertyAccessor.SETTER,Visibility.PUBLIC_ONLY);
	}
	
	protected String serialize(Object obj) throws TException {
		try {
			return objectMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new TException(e);
		}
	}

	@SuppressWarnings("rawtypes")
	protected TBase deSerializeTBase(String tbase) {
		try {
			return objectMapper.readValue(tbase, TBase.class);
		} catch (IOException e) {
			throw new RuntimeException(tbase);
		}
	}

	protected TCacheKey deSerializeTCacheKey(String tCacheKey)  {
		try {
			return objectMapper.readValue(tCacheKey, TCacheKey.class);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


}
