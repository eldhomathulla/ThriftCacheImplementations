package l2.thrift.cache.locks;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;

public class ZookeeperReadWriteLock implements ReadWriteLock {
	private InterProcessReadWriteLock interProcessReadWriteLock;

	public ZookeeperReadWriteLock(CuratorFramework curatorFramework, String path) {
		this.interProcessReadWriteLock = new InterProcessReadWriteLock(curatorFramework, path);
	}

	@Override
	public Lock readLock() {
		return new ZookeeperLock(interProcessReadWriteLock.readLock());
	}

	@Override
	public Lock writeLock() {
		return new ZookeeperLock(interProcessReadWriteLock.writeLock());
	}

}
