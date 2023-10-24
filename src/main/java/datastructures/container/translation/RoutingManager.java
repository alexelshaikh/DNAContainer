package datastructures.container.translation;

import datastructures.container.Container;
import utils.Streamable;
import java.util.Iterator;
import java.util.stream.Stream;

public interface RoutingManager<F> extends Streamable<RoutingManager.RoutedAddress<F>> {

    Container<F, F> container();

    RoutedAddress<F> compute(F original);

    default long size() {
        return container().size();
    }
    default void put(RoutedAddress<F> routedAddress) {
        put(routedAddress.original(), routedAddress.routed());
    }

    default void put(F from, F to) {
        container().put(from, to);
    }

    default boolean remove(RoutedAddress<F> routedAddress) {
        return container().remove(routedAddress.original());
    }

    default RoutedAddress<F> route(F original) {
        F routed = container().get(original);
        if (routed != null)
            return new RoutedAddress<>(original, routed);

        RoutedAddress<F> computed = compute(original);
        put(computed);
        return computed;
    }

    @Override
    default Stream<RoutedAddress<F>> stream() {
        return container().keys().stream().map(this::get);
    }

    @Override
    default Iterator<RoutedAddress<F>> iterator() {
        return  stream().iterator();
    }

    default RoutedAddress<F> get(F f) {
        return new RoutedAddress<>(f, container().get(f));
    }

    static <F> RoutingManager<F> identity() {
        return new RoutingManager<>() {
            long size = 0L;

            @Override
            public void put(RoutedAddress<F> routedAddress) {
                if (routedAddress.original() != routedAddress.routed() && !routedAddress.original().equals(routedAddress.routed()))
                    throw new RuntimeException("identity AddressRoutingManager failed to put: " + routedAddress);

                size++;
            }

            @Override
            public RoutedAddress<F> route(F original) {
                return compute(original);
            }

            @Override
            public RoutedAddress<F> get(F original) {
                return compute(original);
            }

            @Override
            public RoutedAddress<F> compute(F original) {
                return new RoutedAddress<>(original, original);
            }

            @Override
            public long size() {
                return size;
            }

            @Override
            public Container<F, F> container() {
                return Container.discardingContainer();
            }
        };
    }

    record RoutedAddress<F>(F original, F routed) {
    }
}
