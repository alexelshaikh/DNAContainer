package datastructures.container;

import utils.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public interface Container<K, V> extends Streamable<Pair<K, V>> {
    void put(K key, V value);
    boolean remove(K key);
    V get(K key);

    long size();
    Collection<V> values();
    Set<K> keys();

    boolean isPersistent();

    default boolean contains(K key) {
        return get(key) != null;
    }

    @Override
    default Iterator<Pair<K, V>> iterator() {
        return stream().iterator();
    }
    @Override
    default Stream<Pair<K, V>> stream() {
        return keys().stream().map(k -> new Pair<>(k, get(k)));
    }

    static <K, V> Container<K, V> readWriteSynchronizedContainer(Container<K, V> container) {
        return new Container<>() {
            final ReadWriteLock lock = new ReentrantReadWriteLock();

            @Override
            public void put(K key, V value) {
                writeLocked(Executors.callable(() -> container.put(key, value)));
            }

            @Override
            public boolean remove(K key) {
                return writeLocked(() -> container.remove(key));
            }

            @Override
            public V get(K key) {
                return readLocked(() -> container.get(key));
            }

            @Override
            public long size() {
                return readLocked(container::size);
            }

            @Override
            public Collection<V> values() {
                return readLocked(container::values);
            }

            @Override
            public Set<K> keys() {
                return readLocked(container::keys);
            }

            @Override
            public boolean isPersistent() {
                return container.isPersistent();
            }

            private <T> T readLocked(Callable<T> callable) {
                lock.readLock().lock();
                T result = FuncUtils.safeCall(callable);
                lock.readLock().unlock();
                return result;
            }

            private <T> T writeLocked(Callable<T> callable) {
                lock.writeLock().lock();
                T result = FuncUtils.safeCall(callable);
                lock.writeLock().unlock();
                return result;
            }
        };
    }



    static <K, V1, V2> Container<K, V1> transform(Container<K, V2> container, Coder<V1, V2> mapper) {
        return new Container<>() {
            @Override
            public void put(K key, V1 value) {
                container.put(key, mapper.encode(value));
            }
            @Override
            public V1 get(K key) {
                return mapper.decode(container.get(key));
            }
            @Override
            public long size() {
                return container.size();
            }
            @Override
            public Collection<V1> values() {
                return container.values().stream().map(mapper::decode).toList();
            }
            @Override
            public Set<K> keys() {
                return container.keys();
            }
            @Override
            public boolean remove(K key) {
                return container.remove(key);
            }

            @Override
            public boolean isPersistent() {
                return container.isPersistent();
            }
        };
    }

    static <K, V> Container<K, V> discardingContainer() {
        return new Container<>() {
            @Override
            public void put(K key, V value) {
            }
            @Override
            public V get(K key) {
                return null;
            }
            @Override
            public long size() {
                return 0L;
            }
            @Override
            public Collection<V> values() {
                return Collections.emptyList();
            }
            @Override
            public boolean remove(K key) {
                return true;
            }
            @Override
            public Set<K> keys() {
                return Collections.emptySet();
            }
            @Override
            public boolean isPersistent() {
                return false;
            }
        };
    }

    class MapContainer<K, V> implements Container<K, V> {
        protected final ConcurrentHashMap<K, V> store;

        public MapContainer() {
            this.store = new ConcurrentHashMap<>();
        }

        @Override
        public boolean remove(K key) {
            return store.remove(key) != null;
        }

        @Override
        public boolean isPersistent() {
            return false;
        }

        @Override
        public void put(K key, V value) {
            store.put(key, value);
        }

        @Override
        public V get(K key) {
            return store.get(key);
        }

        @Override
        public long size() {
            return store.size();
        }

        @Override
        public Collection<V> values() {
            return store.values();
        }

        @Override
        public Set<K> keys() {
            return store.keySet();
        }
    }

    abstract class LinearLongContainer<V> implements Container<Long, V> {
        protected final UniqueIDGenerator gen;

        public LinearLongContainer() {
            this(0L);
        }

        public LinearLongContainer(long startId) {
            this.gen = new UniqueIDGenerator(startId);
        }

        public long put(V value) {
            long id = registerId();
            put(id, value);
            return id;
        }

        public long registerId() {
            return gen.get();
        }


        public long[] registerIds(int n) {
            return gen.getN(n);
        }

        @Override
        public Collection<V> values() {
            return LongStream.range(0L, size()).mapToObj(this::get).toList();
        }

        @Override
        public Set<Long> keys() {
            return LongStream.range(0L, size()).boxed().collect(Collectors.toSet());
        }
    }
}
