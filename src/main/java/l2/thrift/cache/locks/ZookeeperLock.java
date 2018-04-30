package l2.thrift.cache.locks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;

public class ZookeeperLock implements Lock {
	private InterProcessMutex interProcessMutex;

	public ZookeeperLock(InterProcessMutex interProcessMutex) {
		this.interProcessMutex = interProcessMutex;
	}

	@Override
	public void lock() {
		try {
			interProcessMutex.acquire();
		} catch (Exception e) {
			throw new ZookeeperLockException(e);
		}

	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public Condition newCondition() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean tryLock() {
		try {
			return tryLock(0, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			throw new ZookeeperLockException(e);
		}
	}

	@Override
	public boolean tryLock(long arg0, TimeUnit arg1) throws InterruptedException {
		try {
			return interProcessMutex.acquire(arg0, arg1);
		} catch (Exception e) {
			throw new ZookeeperLockException(e);
		}
	}

	@Override
	public void unlock() {
		try {
			interProcessMutex.release();
		} catch (Exception e) {
			throw new ZookeeperLockException(e);
		}

	}

}
