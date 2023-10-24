package datastructures.container.translation;

import datastructures.container.Container;

public interface TranslationManager<F, T> {

    Container<F, T> container();
    long size();

    int addressSize();
    boolean addressIsFixedSize();
    TranslatedAddress<F, T> compute(RoutingManager.RoutedAddress<F> routedAddress);

    default void put(F routed, T translated) {
        container().put(routed, translated);
    }
    default void put(TranslatedAddress<F, T> ta) {
        put(ta.routed(), ta.translated());
    }

    default boolean remove(F routed) {
        return container().remove(routed);
    }

    default boolean remove(RoutingManager.RoutedAddress<F> routed) {
        return remove(routed.routed());
    }

    default boolean remove(TranslatedAddress<F, T> translatedAddress) {
        return remove(translatedAddress.routed());
    }

    default TranslatedAddress<F, T> translate(RoutingManager.RoutedAddress<F> routedAddress) {
        T t = container().get(routedAddress.routed());
        if (t != null)
            return TranslatedAddress.of(routedAddress, t);

        TranslatedAddress<F, T> translatedAddress = compute(routedAddress);
        put(translatedAddress);
        return translatedAddress;
    }

    default TranslatedAddress<F, T> get(RoutingManager.RoutedAddress<F> f) {
        return get(f.routed());
    }

    default TranslatedAddress<F, T> get(F routed) {
        return new TranslatedAddress<>(routed, container().get(routed));
    }


    static <F> TranslationManager<F, F> identity() {
        return new TranslationManager<>() {
            long size = 0L;

            @Override
            public Container<F, F> container() {
                return Container.discardingContainer();
            }

            @Override
            public long size() {
                return size;
            }

            @Override
            public void put(TranslatedAddress<F, F> ta) {
                if (ta.routed != ta.translated && !ta.routed.equals(ta.translated))
                    throw new RuntimeException("identity AddressTranslationManager failed to put: " + ta);

                size++;
            }

            @Override
            public int addressSize() {
                return -1;
            }

            @Override
            public boolean addressIsFixedSize() {
                return false;
            }

            @Override
            public TranslatedAddress<F, F> translate(RoutingManager.RoutedAddress<F> routedAddress) {
                return compute(routedAddress);
            }

            @Override
            public TranslatedAddress<F, F> get(RoutingManager.RoutedAddress<F> routedAddress) {
                return compute(routedAddress);
            }

            @Override
            public TranslatedAddress<F, F> compute(RoutingManager.RoutedAddress<F> routedAddress) {
                return new TranslatedAddress<>(routedAddress.routed(), routedAddress.routed());
            }
        };
    }

    record TranslatedAddress<F, T> (F routed, T translated) {

        static <F, T> TranslatedAddress<F, T> of(RoutingManager.RoutedAddress<F> routedAddress, T translated) {
            return new TranslatedAddress<>(routedAddress.routed(), translated);
        }
    }
}
