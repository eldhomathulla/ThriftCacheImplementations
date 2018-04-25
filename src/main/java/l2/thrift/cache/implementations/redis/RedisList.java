package l2.thrift.cache.implementations.redis;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.BinaryClient;

public class RedisList<E> implements List<E> {
	private RedisClient redisClient;
	private String name;
	private Function<String, E> deSerializer;
	private Function<E, String> serializer;

	public RedisList(String name, RedisClient redisClient, Function<String, E> deSerializer,
			Function<E, String> serializer) {
		this.name = name;
		this.redisClient = redisClient;
		this.deSerializer = deSerializer;
		this.serializer = serializer;
	}

	public RedisList(String name, RedisClient redisClient, Class<E> eClass) {
		this(name, redisClient, null, null);
		ObjectMapper objectMapper = new ObjectMapper();
		this.serializer = (E obj) -> {
			try {
				return objectMapper.writeValueAsString(obj);
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		};
		this.deSerializer = (String str) -> {
			try {
				return objectMapper.readValue(str, eClass);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		};
	}

	@Override
	public int size() {
		return redisClient.llen(name).intValue();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean contains(Object o) {
		return getAllElements().contains(o);
	}

	private List<E> getAllElements() {
		return redisClient.lrange(name, 0, -1).parallelStream().map((String value) -> deSerializer.apply(value))
				.collect(Collectors.toList());
	}

	@Override
	public Iterator<E> iterator() {
		return new RedisListIterator<>(name, redisClient, deSerializer);
	}

	@Override
	public Object[] toArray() {
		return getAllElements().toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return getAllElements().toArray(a);
	}

	@Override
	public boolean add(E e) {
		return redisClient.rpush(name, serializer.apply(e)) == 1;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object o) {
		return redisClient.lrem(name, 1, serializer.apply((E) o)) == 1;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		redisClient.del(name);

	}

	@Override
	public E get(int index) {
		return deSerializer.apply(redisClient.lindex(name, index));
	}

	@Override
	public E set(int index, E element) {
		return deSerializer.apply(redisClient.lset(name, index, serializer.apply(element)));
	}

	@Override
	public void add(int index, E element) {
		redisClient.linsert(name, BinaryClient.LIST_POSITION.BEFORE, redisClient.lindex(name, index),
				serializer.apply(element));

	}

	@Override
	public E remove(int index) {
		String uuid = UUID.randomUUID().toString();
		E ret = deSerializer.apply(redisClient.lindex(name, index));
		redisClient.lset(name, index, uuid);
		redisClient.lrem(name, 1, uuid);
		return ret;
	}

	@Override
	public int indexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int lastIndexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ListIterator<E> listIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}

	private static class RedisListIterator<T> implements Iterator<T> {
		private int currentIndex = 0;
		private String name;
		private RedisClient redisClient;
		private Function<String, T> deSerializer;

		public RedisListIterator(String name, RedisClient redisClient, Function<String, T> deSerializer) {
			this.name = name;
			this.redisClient = redisClient;
			this.deSerializer = deSerializer;
		}

		@Override
		public boolean hasNext() {
			return currentIndex < redisClient.llen(name);
		}

		@Override
		public T next() {
			T result = deSerializer.apply(redisClient.lindex(name, currentIndex));
			currentIndex++;
			return result;
		}

	}

}
