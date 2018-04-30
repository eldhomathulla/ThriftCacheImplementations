package l2.thrift.cache.implementations;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.thrift.cache.CacheConfiguration;
import org.apache.thrift.cache.TCache;
import org.apache.thrift.cache.ThriftLockFactory;

import l2.thrift.cache.implementations.redis.RedisThriftCache;
import l2.thrift.cache.implementations.redis.client.RedisClientClusterImpl;
import l2.thrift.cache.implementations.redis.client.RedisPoolClientImpl;
import l2.thrift.cache.implementations.redis.client.RedisSentinelImpl;
import l2.thrift.cache.locks.ZookeeperReadWriteLockFactory;
import net.spy.memcached.MemcachedClient;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

public class CacheFactory {

	public static final String LOCK_TYPE_ZOOKEEPER = "zookeeper";
	
	public static final String CACHE_TYPE_DISABLED = "disabled";
	public static final String CACHE_TYPE_MEMCACHED = "memcached";
	public static final String CACHE_TYPE_REDIS_SENTINEL = "redis_sentinel";
	public static final String CACHE_TYPE_REDIS_CLUSTER = "redis_cluster";
	public static final String CACHE_TYPE_REDIS = "redis";

	private static final String LOCK_TYPE = "lock.type";
	private static final String CURATOR_LOCKS_PATH = "/curator/locks";

	public static final String CACHE_TYPE_KEY = "cache.type";
	public static final String MEMCACHED_EXPR_KEY = "memcached.expr";
	public static final String MEMCACHED_INSTANCES_KEY = "memcached.instances";
	public static final String REDIS_CLUSTER_INSTANCES_KEY = "redis.cluster.instances";
	public static final String REDIS_PORT_KEY = "redis.port";
	public static final String REDIS_HOST_KEY = "redis.host";
	public static final String REDIS_SENTINEL_MASTER_KEY = "redis.sentinel.master";
	public static final String REDIS_SENTINELS_KEY = "redis.sentinels";
	public static final String CURATOR_CONNECT_STRING_KEY = "curator.connect.string";

	private CacheConfiguration cacheConfiguration;
	private Object[] ifaces;

	public CacheFactory(CacheConfiguration cacheConfiguration, Object... ifaces) {
		this.cacheConfiguration = cacheConfiguration;
		this.ifaces = ifaces;
	}

	public CacheFactory(CacheConfiguration cacheConfiguration) {
		this.cacheConfiguration = cacheConfiguration;
	}

	private ThriftLockFactory createThriftLockFactory() {
		String lockType = (String) cacheConfiguration.getConfiguration(LOCK_TYPE);
		lockType = lockType == null ? "" : lockType;
		switch (lockType) {
		case LOCK_TYPE_ZOOKEEPER:
			ZookeeperReadWriteLockFactory zookeeperReadWriteLockFactory = createZookeeperReadWriteLockFactory();
			return new ThriftLockFactory((String name) -> zookeeperReadWriteLockFactory.createLock(name));
		default:
			return new ThriftLockFactory();
		}
	}

	private CuratorFramework createCuratorClient(CacheConfiguration cacheConfiguration) {
		return CuratorFrameworkFactory.newClient(
				(String) cacheConfiguration.getConfiguration(CURATOR_CONNECT_STRING_KEY), new RetryNTimes(10, 4000));
	}

	public TCache createCache() {
		String cacheType = (String) cacheConfiguration.getConfiguration(CACHE_TYPE_KEY);
		switch (cacheType) {
		case CACHE_TYPE_REDIS:
			JedisPool jedisPool = new JedisPool((String) cacheConfiguration.getConfiguration(REDIS_HOST_KEY),
					Integer.parseInt((String) cacheConfiguration.getConfiguration(REDIS_PORT_KEY)));
			return new RedisThriftCache(cacheConfiguration, new RedisPoolClientImpl(jedisPool),
					createThriftLockFactory(), ifaces);
		case CACHE_TYPE_REDIS_CLUSTER:
			Set<HostAndPort> hostAndPorts = Arrays
					.stream(cacheConfiguration.getConfiguration(REDIS_CLUSTER_INSTANCES_KEY).toString().split(","))
					.map((String hostPortStr) -> {
						String[] hostPort = hostPortStr.split(":");
						return new HostAndPort(hostPort[0], Integer.parseInt(hostPort[1]));
					}).collect(Collectors.toSet());
			JedisCluster jedisCluster = new JedisCluster(hostAndPorts);
			return new RedisThriftCache(cacheConfiguration, new RedisClientClusterImpl(jedisCluster),
					createThriftLockFactory(), ifaces);
		case CACHE_TYPE_REDIS_SENTINEL:
			Set<String> sentinels = Arrays
					.stream(cacheConfiguration.getConfiguration(REDIS_SENTINELS_KEY).toString().split(","))
					.collect(Collectors.toSet());
			JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(
					(String) cacheConfiguration.getConfiguration(REDIS_SENTINEL_MASTER_KEY), sentinels);
			return new RedisThriftCache(cacheConfiguration, new RedisSentinelImpl(jedisSentinelPool),
					createThriftLockFactory(), ifaces);
		case CACHE_TYPE_MEMCACHED:
			List<InetSocketAddress> inetSocketAddresses = Arrays
					.stream(cacheConfiguration.getConfiguration(MEMCACHED_INSTANCES_KEY).toString().split(";"))
					.map((String hostPortStr) -> {
						String[] hostPort = hostPortStr.split(",");
						return new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));
					}).collect(Collectors.toList());
			try {
				new MemcachedThriftCache(cacheConfiguration, new MemcachedClient(inetSocketAddresses),
						Integer.parseInt((String) cacheConfiguration.getConfiguration(MEMCACHED_EXPR_KEY)), ifaces);
			} catch (NumberFormatException | IOException e) {
				new RuntimeException(e);
			}
		case CACHE_TYPE_DISABLED:
			return null;
		default:
			throw new RuntimeException("Unknown cache type: " + cacheType);
		}
	}

	private ZookeeperReadWriteLockFactory createZookeeperReadWriteLockFactory() {
		ZookeeperReadWriteLockFactory zookeeperReadWriteLockFactory;
		zookeeperReadWriteLockFactory = new ZookeeperReadWriteLockFactory(createCuratorClient(cacheConfiguration),
				CURATOR_LOCKS_PATH);
		return zookeeperReadWriteLockFactory;
	}

}
