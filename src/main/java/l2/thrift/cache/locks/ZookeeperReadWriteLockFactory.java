package l2.thrift.cache.locks;

import org.apache.curator.framework.CuratorFramework;

public class ZookeeperReadWriteLockFactory {
	private String basePath;
	private CuratorFramework curatorFramework;

	public ZookeeperReadWriteLockFactory(CuratorFramework curatorFramework, String basePath) {
		this.basePath = basePath;
		this.curatorFramework = curatorFramework;
	}

	public ZookeeperReadWriteLock createLock(String name) {
		return new ZookeeperReadWriteLock(curatorFramework, basePath + "/" + name);
	}

}
