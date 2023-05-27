package datastructures.container;

import utils.UniqueIDGenerator;
import utils.Coder;
import utils.Pair;
import utils.Streamable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.LongStream;

public interface Container<K, V, T> extends Streamable<Pair<K, V>> {
    K put(V value);
    void put(K key, V value);
    V get(K key);
    long size();
    K[] registerIds(int n);
    Collection<V> values();
    Set<K> keys();
    AddressTranslationManager<K, T> getAddressTranslationManager();

    default K registerId() {
        return registerIds(1)[0];
    }

    static <K, V1, V2, T> Container<K, V1, T> transform(Container<K, V2, T> container, Coder<V1, V2> mapper) {
        return new Container<>() {
            @Override
            public K put(V1 value) {
                return container.put(mapper.encode(value));
            }
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
            public K[] registerIds(int n) {
                return container.registerIds(n);
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
            public AddressTranslationManager<K, T> getAddressTranslationManager() {
                return container.getAddressTranslationManager();
            }

            @Override
            public Iterator<Pair<K, V1>> iterator() {
                return new Iterator<>() {
                    Iterator<Pair<K, V2>> it = container.iterator();
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }
                    @Override
                    public Pair<K, V1> next() {
                        var p = it.next();
                        return new Pair<>(p.getT1(), mapper.decode(p.getT2()));
                    }
                };
            }
        };
    }

    static <K, V, T> Container<K, V, T> discardingContainer() {
        return new Container<>() {
            @Override
            public K put(V value) {
                return null;
            }
            @Override
            public void put(K key, V value) {
            }
            @Override
            public V get(K key) {
                return null;
            }
            @Override
            public long size() {
                return 0;
            }
            @Override
            public K[] registerIds(int n) {
                return (K[]) new Object[0];
            }
            @Override
            public Collection<V> values() {
                return Collections.emptyList();
            }
            @Override
            public Set<K> keys() {
                return Collections.emptySet();
            }
            @Override
            public AddressTranslationManager<K, T> getAddressTranslationManager() {
                return null;
            }
            @Override
            public Iterator<Pair<K, V>> iterator() {
                return Collections.emptyIterator();
            }
        };
    }

    abstract class LongContainer<V, T> implements Container<Long, V, T> {
        protected final UniqueIDGenerator idsGen;
        public LongContainer(UniqueIDGenerator idsGen) {
            this.idsGen = idsGen;
        }
        public LongContainer() {
            this(new UniqueIDGenerator());
        }
        @Override
        public Long[] registerIds(int n) {
            return idsGen.getN(n);
        }
        @Override
        public Iterator<Pair<Long, V>> iterator() {
            return LongStream.range(idsGen.getStart(), size()).mapToObj(id -> new Pair<>(id, get(id))).iterator();
        }
    }

    abstract class MapContainer<K, V> implements Container<K, V, K> {
        private final ConcurrentHashMap<K, V> store;

        public MapContainer() {
            this.store = new ConcurrentHashMap<>();
        }

        public static <K, V> MapContainer<K, V> of(Function<Integer, K[]> reservationFunc) {
            return new MapContainer<>() {
                @Override
                public K[] registerIds(int n) {
                    return reservationFunc.apply(n);
                }
                @Override
                public AddressTranslationManager<K, K> getAddressTranslationManager() {
                    return AddressTranslationManager.identity();
                }
            };
        }

        public static <V> MapContainer<Long, V> ofLong() {
            UniqueIDGenerator gen = new UniqueIDGenerator();
            return of(gen::getN);
        }

        @Override
        public K put(V value) {
            K key = registerId();
            store.put(key, value);
            return key;
        }

        @Override
        public Iterator<Pair<K, V>> iterator() {
            return new Iterator<>() {
                final Iterator<Map.Entry<K, V>> it = store.entrySet().iterator();
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }
                @Override
                public Pair<K, V> next() {
                    Map.Entry<K, V> n = it.next();
                    return new Pair<>(n.getKey(), n.getValue());
                }
            };
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
}
