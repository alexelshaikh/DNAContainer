package datastructures.container.translation;

import datastructures.container.Container;
import utils.Coder;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public interface AddressManager<F, T> extends Coder<F, AddressManager.ManagedAddress<F, T>> {
    RoutingManager<F> addressRoutingManager();
    TranslationManager<F, T> addressTranslationManager();

    default int addressSize() {
        return addressTranslationManager().addressSize();
    }

    default boolean addressIsFixedSize() {
        return addressTranslationManager().addressIsFixedSize();
    }

    default ManagedAddress<F, T> get(F original) {
        RoutingManager.RoutedAddress<F> routed = addressRoutingManager().get(original);
        return ManagedAddress.of(
                routed,
                addressTranslationManager().get(routed)
        );
    }

    @Override
    default ManagedAddress<F, T> encode(F f) {
        return routeAndTranslate(f);
    }

    @Override
    default F decode(ManagedAddress<F, T> managedAddress) {
        return managedAddress.original();
    }

    default ManagedAddress<F, T> routeAndTranslate(F original) {
        RoutingManager.RoutedAddress<F> routed = addressRoutingManager().route(original);
        TranslationManager.TranslatedAddress<F, T> translatedAddress = addressTranslationManager().translate(routed);

        return ManagedAddress.of(routed, translatedAddress);
    }

    default RoutingManager.RoutedAddress<F> route(F original) {
        return addressRoutingManager().route(original);
    }

    default TranslationManager.TranslatedAddress<F, T> translate(RoutingManager.RoutedAddress<F> routed) {
        return addressTranslationManager().translate(routed);
    }

    default ManagedAddress<F, T> get(RoutingManager.RoutedAddress<F> routedAddress) {
        return ManagedAddress.of(
                routedAddress,
                addressTranslationManager().get(routedAddress)
        );
    }
    default void remove(RoutingManager.RoutedAddress<F> routedAddress) {
        addressRoutingManager().remove(routedAddress);
        TranslationManager.TranslatedAddress<F, T> translatedAddress = addressTranslationManager().get(routedAddress);
        addressTranslationManager().remove(translatedAddress);
    }

    default void put(F original, F routed, T translated) {
        RoutingManager.RoutedAddress<F> routedAddress = new RoutingManager.RoutedAddress<>(original, routed);
        TranslationManager.TranslatedAddress<F, T> translatedAddress = TranslationManager.TranslatedAddress.of(routedAddress, translated);

        put(routedAddress, translatedAddress);
    }

    default void put(ManagedAddress<F, T> managedAddress) {
        RoutingManager.RoutedAddress<F> routedAddress = managedAddress.routedAddress();
        TranslationManager.TranslatedAddress<F, T> translatedAddress = TranslationManager.TranslatedAddress.of(routedAddress, managedAddress.translated());

        put(routedAddress, translatedAddress);
    }

    default void put(RoutingManager.RoutedAddress<F> routedAddress, TranslationManager.TranslatedAddress<F, T> translatedAddress) {
        addressRoutingManager().put(routedAddress);
        addressTranslationManager().put(translatedAddress);
    }


    static <F> AddressManager<F, F> identity() {
        return new AddressManager<>() {
            @Override
            public TranslationManager<F, F> addressTranslationManager() {
                return TranslationManager.identity();
            }
            @Override
            public RoutingManager<F> addressRoutingManager() {
                return RoutingManager.identity();
            }
        };
    }

    static <F, T> AddressManager<F, T> of(RoutingManager<F> routingManager, TranslationManager<F, T> translationManager) {
        return new AddressManager<>() {
            @Override
            public TranslationManager<F, T> addressTranslationManager() {
                return translationManager;
            }

            @Override
            public RoutingManager<F> addressRoutingManager() {
                return routingManager;
            }
        };
    }

    static <K, T, V> Container<K, V> managedContainer(Container<T, V> container, AddressManager<K, T> addressManager) {
        return new Container<>() {
            @Override
            public void put(K key, V value) {
                container.put(addressManager.routeAndTranslate(key).translated(), value);
            }

            @Override
            public boolean remove(K key) {
                return container.remove(addressManager.routeAndTranslate(key).translated());
            }

            @Override
            public V get(K key) {
                return container.get(addressManager.routeAndTranslate(key).translated());
            }

            @Override
            public long size() {
                return container.size();
            }

            @Override
            public Collection<V> values() {
                return container.values();
            }

            @Override
            public Set<K> keys() {
                return addressManager.addressRoutingManager().container().keys();
            }

            @Override
            public boolean isPersistent() {
                return container.isPersistent();
            }
        };
    }


    class ManagedAddress<F, T> {
        protected F original;
        protected F routed;
        protected T translated;

        public ManagedAddress(F original, F routed, T translated) {
            this.original = original;
            this.routed = routed;
            this.translated = translated;
        }

        public F original() {
            return original;
        }

        public F routed() {
            return routed;
        }

        public T translated() {
            return translated;
        }

        public RoutingManager.RoutedAddress<F> routedAddress() {
            return new RoutingManager.RoutedAddress<>(original(), routed());
        }

        public TranslationManager.TranslatedAddress<F, T> asTranslatedAddress() {
            return TranslationManager.TranslatedAddress.of(routedAddress(), translated());
        }

        public static <F, T> ManagedAddress<F, T> of(RoutingManager.RoutedAddress<F> routed, TranslationManager.TranslatedAddress<F, T> translatedAddress) {
            return of(
                    routed,
                    translatedAddress.translated()
            );
        }

        public static <F, T> ManagedAddress<F, T> ofLazy(RoutingManager.RoutedAddress<F> routed, Supplier<T> translatedAddress) {
            return new LazyManagedAddress<>(
                    routed.original(),
                    routed.routed(),
                    translatedAddress
            );
        }

        public static <F, T> ManagedAddress<F, T> of(RoutingManager.RoutedAddress<F> routed, T translatedAddress) {
            return new ManagedAddress<>(
                    routed.original(),
                    routed.routed(),
                    translatedAddress
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o instanceof ManagedAddress<?, ?> that)
                return Objects.equals(original(), that.original()) && Objects.equals(routed(), that.routed()) && Objects.equals(translated(), that.translated());

            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(original(), routed(), translated());
        }

        @Override
        public String toString() {
            return "ManagedAddress{" +
                    "original=" + original() +
                    ", routed=" + routed() +
                    ", translated=" + translated() +
                    '}';
        }
    }

    class LazyManagedAddress<F, T> extends ManagedAddress<F, T> {
        final Supplier<T> translatedSupp;

        public LazyManagedAddress(F original, F routed, Supplier<T> translatedSupp) {
            super(original, routed, null);
            this.translatedSupp = translatedSupp;
        }

        @Override
        public synchronized T translated() {
            if (translated == null)
                translated = translatedSupp.get();

            return translated;
        }
    }
}