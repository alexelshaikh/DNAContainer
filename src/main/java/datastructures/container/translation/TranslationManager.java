package datastructures.container.translation;

import datastructures.container.Container;

public interface TranslationManager<F, T> {

    Container<F, T> container();
    void put(TranslatedAddress<F, T> ta);
    long size();

    int addressSize();
    boolean addressIsFixedSize();
    TranslatedAddress<F, T> compute(RoutingManager.RoutedAddress<F> routedAddress);

    default boolean remove(RoutingManager.RoutedAddress<F> routed) {
        return container().remove(routed.routed());
    }

    default boolean remove(TranslatedAddress<F, T> translatedAddress) {
        return remove(translatedAddress.routedAddress);
    }

    default TranslatedAddress<F, T> translate(RoutingManager.RoutedAddress<F> routedAddress) {
        T t = container().get(routedAddress.routed());
        if (t != null)
            return new TranslatedAddress<>(routedAddress, t);

        TranslatedAddress<F, T> translatedAddress = compute(routedAddress);
        put(translatedAddress);
        return translatedAddress;
    }

    default TranslatedAddress<F, T> get(RoutingManager.RoutedAddress<F> f) {
        return new TranslatedAddress<>(f, container().get(f.routed()));
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
                if (ta.routedAddress != ta.translated && !ta.routedAddress.routed().equals(ta.translated))
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
                return new TranslatedAddress<>(routedAddress, routedAddress.routed());
            }
        };
    }

    record TranslatedAddress<F, T> (RoutingManager.RoutedAddress<F> routedAddress, T translated) {

    }
}
