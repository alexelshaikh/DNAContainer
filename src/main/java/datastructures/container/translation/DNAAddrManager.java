package datastructures.container.translation;

import core.BaseSequence;
import core.dnarules.BasicDNARules;
import core.dnarules.DNARule;
import datastructures.container.Container;
import datastructures.container.impl.PersistentContainer;
import dnacoders.DistanceCoder;
import dnacoders.GCFiller;
import dnacoders.dnaconvertors.RotatingTre;
import dnacoders.headercoders.BasicDNAPadder;
import dnacoders.headercoders.PermutationCoder;
import dnacoders.headercoders.ReedSolomonCoder;
import utils.*;
import utils.lsh.LSH;
import utils.lsh.minhash.MinHashLSH;
import utils.serializers.FixedSizeSerializer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class DNAAddrManager implements AddressManager<Long, BaseSequence> {

    private final LSH<BaseSequence> lsh;
    private final Function<Long, BaseSequence> coder;
    private final int addressSize;
    private final AtomicLong badAddresses;
    private final UniqueIDGenerator addrGen;
    private final double minDist;
    private final int addressTranslationTrials;
    private final AtomicLong size;
    private final ReadWriteLock addressManagerLock;

    private final AddressTranslationManager addressTranslationManager;
    private final AddressRoutingManager addressRoutingManager;

    private DNAAddrManager(
            LSH<BaseSequence> lsh,
            double minDist,
            int addressSize,
            Coder<Long, BaseSequence> coder,
            int translationTrials,
            Container<Long, Long> addressRoutingContainer,
            Container<Long, BaseSequence> addressTranslationContainer
    ) {
        this.lsh = lsh;
        this.coder = coder;
        this.minDist = minDist;
        this.addressSize = addressSize;
        this.addressTranslationTrials = translationTrials;
        this.addrGen = new UniqueIDGenerator();
        this.addressRoutingManager = new AddressRoutingManager(addressRoutingContainer);
        this.addressTranslationManager = new AddressTranslationManager(addressTranslationContainer);
        this.badAddresses = new AtomicLong(0L);
        this.addressManagerLock = new ReentrantReadWriteLock();
        this.size = new AtomicLong(0L);
    }

    @Override
    public ManagedAddress<Long, BaseSequence> routeAndTranslate(Long addr) {
        return routeAndTranslate(addr, true);
    }

    private ManagedAddress<Long, BaseSequence> readManagedAddress(long addr, boolean lockContainers) {
        try {
            if (lockContainers)
                addressManagerLock.readLock().lock();
            RoutingManager.RoutedAddress<Long> routedAddress = addressRoutingManager.get(addr);
            if (routedAddress.routed() != null)
                return ManagedAddress.ofLazy(routedAddress, () -> addressTranslationManager.get(routedAddress).translated());

            return null;
        }
        finally {
            if (lockContainers)
                addressManagerLock.readLock().unlock();
        }
    }

    private void writeNewManagedAddress(long addr, long routed, BaseSequence translated, long addedBadAddresses) {
        lsh.insert(translated);
        addressRoutingManager.container.put(addr, routed);
        addressTranslationManager.container.put(routed, translated);
        size.incrementAndGet();
        badAddresses.addAndGet(addedBadAddresses);
    }

    private ManagedAddress<Long, BaseSequence> routeAndTranslate(Long addr, boolean store) {
        ManagedAddress<Long, BaseSequence> managed = readManagedAddress(addr, true);
        if (managed != null)
            return managed;

        return compute(addr, store);
    }

    public ManagedAddress<Long, BaseSequence> compute(long addr, boolean store) {
        int trials = 0;
        long routed = addrGen.get();
        BaseSequence barcode = coder.apply(routed);
        long numBadAddresses = 0L;
        while(trials++ < addressTranslationTrials) {
            if (isSufficientDistance(barcode)) {
                addressManagerLock.writeLock().lock();
                ManagedAddress<Long, BaseSequence> managed = readManagedAddress(addr, false);
                if (managed != null) {
                    addressManagerLock.writeLock().unlock();
                    return managed;
                }
                if (store)
                    writeNewManagedAddress(addr, routed, barcode, numBadAddresses);
                addressManagerLock.writeLock().unlock();
                return new ManagedAddress<>(addr, routed, barcode);
            }
            else {
                routed = addrGen.get();
                barcode = coder.apply(routed);
                numBadAddresses++;
            }
        }
        if (store) {
            addressManagerLock.writeLock().lock();
            ManagedAddress<Long, BaseSequence> managed = readManagedAddress(addr, false);
            if (managed != null) {
                addressManagerLock.writeLock().unlock();
                return managed;
            }
            writeNewManagedAddress(addr, routed, barcode, numBadAddresses);
            addressManagerLock.writeLock().unlock();
        }

        return new ManagedAddress<>(addr, routed, barcode);
    }

    public int getAddressSize() {
        return addressSize;
    }

    public long size() {
        return size.get();
    }
    public long badAddressesCount() {
        return badAddresses.longValue();
    }

    public int getAddressTranslationTrials() {
        return addressTranslationTrials;
    }

    private boolean isSufficientDistance(BaseSequence barcode) {
        return DistanceCoder.distanceScore(barcode, lsh) >= minDist && DistanceCoder.distanceScore(barcode.complement(), lsh) >= minDist;
    }

    public LSH<BaseSequence> getLsh() {
        return lsh;
    }

    @Override
    public RoutingManager<Long> addressRoutingManager() {
        return addressRoutingManager;
    }

    @Override
    public TranslationManager<Long, BaseSequence> addressTranslationManager() {
        return addressTranslationManager;
    }

    public static Builder builder() {
        return new Builder();
    }

    private class AddressRoutingManager implements RoutingManager<Long> {
        Container<Long, Long> container;

        public AddressRoutingManager(Container<Long, Long> container) {
            this.container = container;
        }

        @Override
        public Container<Long, Long> container() {
            return container;
        }

        @Override
        public RoutedAddress<Long> compute(Long original) {
            return DNAAddrManager.this.routeAndTranslate(original, false).routedAddress();
        }
    }

    private class AddressTranslationManager implements TranslationManager<Long, BaseSequence> {
        Container<Long, BaseSequence> container;

        public AddressTranslationManager(Container<Long, BaseSequence> container) {
            this.container = container;
        }

        @Override
        public int addressSize() {
            return addressSize;
        }

        @Override
        public boolean addressIsFixedSize() {
            return true;
        }

        @Override
        public long size() {
            return container.size();
        }

        @Override
        public TranslatedAddress<Long, BaseSequence> compute(RoutingManager.RoutedAddress<Long> routedAddress) {
            return DNAAddrManager.this.routeAndTranslate(routedAddress.original(), false).asTranslatedAddress();
        }

        @Override
        public Container<Long, BaseSequence> container() {
            return container;
        }
    }

    public static class Builder {
        public static final int DEFAULT_ADDRESS_TRANSLATION_TRIALS = 1000;
        public static final int DEFAULT_ADDRESS_SIZE = 80;
        public static final int DEFAULT_MIN_ADDRESS_SIZE = 20;
        public static final int DEFAULT_ADDR_NUM_PERMUTATIONS = 16;
        public static final int DEFAULT_ECC_LEN = 0;
        public static final double DEFAULT_MIN_DIST = 0.3d;
        public static final boolean DEFAULT_DEEP_LSH = true;
        public static final Coder<String, BaseSequence> DEFAULT_STRING_CODER = RotatingTre.INSTANCE;
        public static final BiFunction<Integer, Boolean, LSH<BaseSequence>> DEFAULT_LSH = (addrSize, deep) -> deep ? MinHashLSH.newSeqLSHTraditional(5, 5) : MinHashLSH.newSeqLSHLight(5, 5);
        public static final Supplier<DNARule> DEFAULT_DNA_RULES = () -> BasicDNARules.INSTANCE;

        // only supported in single-thread mode
        public static final Supplier<Container<Long, Long>> DEFAULT_ADDRESS_ROUTING_CONTAINER_PERSISTENT = () -> new PersistentContainer<>("routing.table", FixedSizeSerializer.LONG);

        public static final Supplier<Container<Long, Long>> DEFAULT_ADDRESS_ROUTING_CONTAINER_NOT_PERSISTENT = Container.MapContainer::new;

        // only supported in single-thread mode
        public static final Function<Integer, Container<Long, BaseSequence>> DEFAULT_ADDRESS_TRANSLATION_CONTAINER_PERSISTENT = addrSize -> new PersistentContainer<>("translation.table", new FixedSizeSerializer<>() {
            @Override
            public int serializedSize() {
                return 2 + addrSize / 4;
            }
            @Override
            public byte[] serialize(BaseSequence seq) {
                return Packer.withBytePadding(SeqBitStringConverter.transform(seq)).toBytes();
            }
            @Override
            public BaseSequence deserialize(byte[] bs) {
                return SeqBitStringConverter.transform(Packer.withoutBytePadding(new BitString(bs)));
            }
        });
        public static final Function<Integer, Container<Long, BaseSequence>> DEFAULT_ADDRESS_TRANSLATION_CONTAINER_NOT_PERSISTENT = __ -> new Container.MapContainer<>();

        private LSH<BaseSequence> lsh;
        private Integer numPermutations;
        private Integer addressEccSize;
        private Integer addrSize;
        private DNARule dnaRules;
        private Container<Long, Long> addressRoutingContainer;
        private Container<Long, BaseSequence> addressTranslationContainer;
        private Coder<String, BaseSequence> stringCoder;
        private Double minDist;
        private Integer addressTranslationTrials;
        private Boolean deepLSH;

        public DNAAddrManager build() {
            this.addressEccSize = FuncUtils.conditionOrElse(ecc -> ecc != null && ecc >= 0, addressEccSize, () -> DEFAULT_ECC_LEN);
            this.addrSize = FuncUtils.conditionOrElse(s -> s != null && s >= DEFAULT_MIN_ADDRESS_SIZE, addrSize, () -> DEFAULT_ADDRESS_SIZE);
            if (addressEccSize > 0 && addrSize % 4 != 0)
                throw new RuntimeException("addressSize % 4 != 0");

            this.deepLSH = FuncUtils.nullEscape(deepLSH, () -> DEFAULT_DEEP_LSH);
            this.lsh = FuncUtils.nullEscape(lsh, () -> DEFAULT_LSH.apply(addrSize, deepLSH));
            this.numPermutations = FuncUtils.conditionOrElse(n -> n != null && n >= 0, numPermutations, () -> DEFAULT_ADDR_NUM_PERMUTATIONS);
            this.dnaRules = FuncUtils.nullEscape(dnaRules, DEFAULT_DNA_RULES);
            this.addressRoutingContainer = FuncUtils.nullEscape(addressRoutingContainer, DEFAULT_ADDRESS_ROUTING_CONTAINER_NOT_PERSISTENT);
            this.addressTranslationContainer = FuncUtils.nullEscape(addressTranslationContainer, () -> DEFAULT_ADDRESS_TRANSLATION_CONTAINER_NOT_PERSISTENT.apply(addrSize));
            this.minDist = FuncUtils.conditionOrElse(d -> d != null && d > 0d && d <= 1d, minDist, () -> DEFAULT_MIN_DIST);
            this.stringCoder = FuncUtils.nullEscape(stringCoder, () -> DEFAULT_STRING_CODER);
            this.addressTranslationTrials = FuncUtils.conditionOrElse(t -> t != null && t > 0, addressTranslationTrials, () -> DEFAULT_ADDRESS_TRANSLATION_TRIALS);

            PermutationCoder addressPermutationCoder = numPermutations <= 0 ? new ZeroPermCoder(false) : new PermutationCoder(false, numPermutations, seq -> -dnaRules.evalErrorProbability(seq));
            int addrPermOffset = addressPermutationCoder.getLengthBase().totalSize();
            int offsetAddress = addrPermOffset + (addressEccSize > 0 ? ReedSolomonCoder.overhead(addrSize, addressEccSize) : 0);
            var paddingCoder = new BasicDNAPadder(addrSize - offsetAddress);

            Coder<String, BaseSequence> addrCoder = Coder.fuse(
                    stringCoder,
                    paddingCoder,
                    addressPermutationCoder
            );


            Coder<String, BaseSequence> addressGenerator = addressEccSize > 0 ? Coder.fuse(addrCoder, new ReedSolomonCoder(addressEccSize)) : addrCoder;
            Coder<Long, BaseSequence> coder = Coder.fuse(
                    Coder.of(String::valueOf, Long::parseLong),
                    addressGenerator,
                    Coder.of(seq -> seq.length() > addrSize ? seq.window(0, addrSize) : seq, null)
            );

            return new DNAAddrManager(
                    lsh,
                    minDist,
                    addrSize,
                    coder,
                    addressTranslationTrials,
                    addressRoutingContainer,
                    addressTranslationContainer
            );
        }

        public Builder setLsh(LSH<BaseSequence> lsh) {
            this.lsh = lsh;
            return this;
        }

        public Builder setDeepLSH(boolean deepLSH) {
            this.deepLSH = deepLSH;
            return this;
        }

        public Builder setMinDist(double minDist) {
            this.minDist = minDist;
            return this;
        }

        public Builder setAddressEccSize(int addressEccSize) {
            this.addressEccSize = addressEccSize;
            return this;
        }

        public Builder setDnaRules(DNARule dnaRules) {
            this.dnaRules = dnaRules;
            return this;
        }

        public Builder setAddressRoutingContainer(Container<Long, Long> addressRoutingContainer) {
            this.addressRoutingContainer = addressRoutingContainer;
            return this;
        }

        public Builder setAddressTranslationContainer(Container<Long, BaseSequence> addressTranslationContainer) {
            this.addressTranslationContainer = addressTranslationContainer;
            return this;
        }

        public Builder setMinDist(Double minDist) {
            this.minDist = minDist;
            return this;
        }

        public Builder setAddressTranslationTrials(int addressTranslationTrials) {
            this.addressTranslationTrials = addressTranslationTrials;
            return this;
        }

        public Builder setNumPermutations(int numPermutations) {
            this.numPermutations = numPermutations;
            return this;
        }

        public Builder setAddrSize(int addrSize) {
            if (addrSize < DEFAULT_MIN_ADDRESS_SIZE)
                throw new RuntimeException("address size must be >= " + DEFAULT_MIN_ADDRESS_SIZE);
            this.addrSize = addrSize;
            return this;
        }

        public Builder setStringCoder(Coder<String, BaseSequence> stringCoder) {
            this.stringCoder = stringCoder;
            return this;
        }
    }

    private static class ZeroPermCoder extends PermutationCoder {
        public ZeroPermCoder(boolean parallel) {
            super(parallel, 1, null);
        }

        @Override
        public BaseSequence encode(BaseSequence seq) {
            return BaseSequence.join(GCFiller.getTrimmedFiller(seq, lengthBase.totalSize()), seq);
        }
        @Override
        public BaseSequence decode(BaseSequence encoded) {
            return extractPayload(encoded);
        }
        @Override
        public BaseSequence decode(BaseSequence payload, Integer header) {
            return payload;
        }
        @Override
        public Integer decodeHeader(BaseSequence encoded) {
            return 0;
        }
        @Override
        public BaseSequence extractPayload(BaseSequence encoded) {
            return encoded.window(lengthBase.totalSize());
        }
    }
}
