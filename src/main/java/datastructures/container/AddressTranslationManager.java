package datastructures.container;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public interface AddressTranslationManager<F, T> {

    TranslatedAddress<T> translate(F addr);

    long size();
    long badAddressesSize();

    int addressSize();
    boolean isPersistent();

    Iterator<F> usedAddressesIterator();

    default List<? extends TranslatedAddress<T>> translateN(F[] addr) {
        return translateN(Arrays.asList(addr));
    }

    default List<? extends TranslatedAddress<T>> translateN(List<F> addr) {
       return addr.stream().parallel().map(this::translate).toList();
    }

    static <F, T> AddressTranslationManager<F, T> of(Function<F, T> translator) {
        return new AddressTranslationManager<>() {
            @Override
            public TranslatedAddress<T> translate(F addr) {
                return TranslatedAddress.of(translator.apply(addr));
            }

            @Override
            public long size() {
                return 0;
            }

            @Override
            public long badAddressesSize() {
                return 0;
            }

            @Override
            public int addressSize() {
                return 0;
            }

            @Override
            public boolean isPersistent() {
                return false;
            }

            @Override
            public Iterator<F> usedAddressesIterator() {
                return Collections.emptyIterator();
            }
        };
    }

    interface TranslatedAddress<T> {
        T address();

        default boolean isEqual(TranslatedAddress<T> ta) {
            return address().equals(ta.address());
        }

        static <T> TranslatedAddress<T> of(T addr) {
            return () -> addr;
        }
    }

    static <T> AddressTranslationManager<T, T> identity() {
        return new AddressTranslationManager<>() {
            @Override
            public TranslatedAddress<T> translate(T addr) {
                return TranslatedAddress.of(addr);
            }
            @Override
            public long size() {
                return 0L;
            }
            @Override
            public long badAddressesSize() {
                return 0L;
            }
            @Override
            public boolean isPersistent() {
                return false;
            }
            @Override
            public int addressSize() {
                return 0;
            }
            @Override
            public Iterator<T> usedAddressesIterator() {
                return Collections.emptyIterator();
            }
        };
    }

    static <T1, T2, T3> AddressTranslationManager<T1, T3> fuse(AddressTranslationManager<T1, T2> atm1, AddressTranslationManager<T2, T3> atm2) {
        return new AddressTranslationManager<>() {
            @Override
            public TranslatedAddress<T3> translate(T1 addr) {
                return atm2.translate(atm1.translate(addr).address());
            }

            @Override
            public long size() {
                return atm2.size();
            }

            @Override
            public long badAddressesSize() {
                return atm1.badAddressesSize() + atm2.badAddressesSize();
            }

            @Override
            public int addressSize() {
                return atm2.addressSize();
            }

            @Override
            public boolean isPersistent() {
                return atm2.isPersistent();
            }

            @Override
            public Iterator<T1> usedAddressesIterator() {
                return atm1.usedAddressesIterator();
            }
        };
    }
}
