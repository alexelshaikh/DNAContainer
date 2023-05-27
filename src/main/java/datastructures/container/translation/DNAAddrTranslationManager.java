package datastructures.container.translation;

import core.BaseSequence;
import core.dnarules.BasicDNARules;
import core.dnarules.DNARule;
import datastructures.container.AddressTranslationManager;
import datastructures.container.Container;
import datastructures.container.impl.PersistentContainer;
import dnacoders.GCFiller;
import utils.UniqueIDGenerator;
import dnacoders.DistanceCoder;
import dnacoders.dnaconvertors.RotatingTre;
import dnacoders.headercoders.BasicDNAPadder;
import dnacoders.headercoders.PermutationCoder;
import dnacoders.headercoders.ReedSolomonCoder;
import utils.*;
import utils.lsh.LSH;
import utils.lsh.minhash.MinHashLSH;
import utils.lsh.minhash.MinHashLSHLight;
import utils.serializers.FixedSizeSerializer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class DNAAddrTranslationManager implements AddressTranslationManager<Long, BaseSequence> {

    private final LSH<BaseSequence> lsh;
    private final Function<Long, BaseSequence> coder;
    private final int addressSize;
    private final Container<Long, Long, ?> translationContainer;
    private final Container<Long, BaseSequence, ?> barcodeContainer;
    private final AtomicLong badAddresses;
    private final UniqueIDGenerator addrGen;
    private final double minDist;
    private final boolean persistentTranslation;
    private final boolean persistentBarcodes;
    private final int addressTranslationTrials;


    private DNAAddrTranslationManager(LSH<BaseSequence> lsh, double minDist, int addressSize, Coder<Long, BaseSequence> coder, int translationTrials, Container<Long, Long, ?> translationContainer, Container<Long, BaseSequence, ?> barcodesContainer, boolean persistentTranslation, boolean persistentBarcodes) {
        this.lsh = lsh;
        this.coder = coder;
        this.minDist = minDist;
        this.addressSize = addressSize;
        this.addressTranslationTrials = translationTrials;
        this.addrGen = new UniqueIDGenerator();
        this.persistentTranslation = persistentTranslation;
        this.persistentBarcodes = persistentBarcodes;
        this.translationContainer = translationContainer;
        this.barcodeContainer = barcodesContainer;
        this.badAddresses = new AtomicLong(0L);
    }

    @Override
    public DNATranslatedAddress translate(Long addr) {
        Long translatedAddr = translationContainer.get(addr);
        return translatedAddr == null ? computeTranslation(addr) : new DNATranslatedAddress(translatedAddr, barcodeContainer.get(addr));
    }

    public DNATranslatedAddress translateOptimistic(Long addr) {
        Long translatedAddr = translationContainer.get(addr);
        return translatedAddr == null ? computeTranslationOptimistic(addr) : new DNATranslatedAddress(translatedAddr, barcodeContainer.get(addr));
    }

    @Override
    public int addressSize() {
        return addressSize;
    }

    private DNATranslatedAddress computeTranslation(Long addr) {
        int trials = 0;
        long translatedAddr = addr;
        BaseSequence barcode = null;
        while(trials++ < addressTranslationTrials) {
            translatedAddr = addrGen.get();
            barcode = coder.apply(translatedAddr);
            synchronized (lsh) {
                if (isSufficientDistance(barcode)) {
                    lsh.insert(barcode);
                    translationContainer.put(addr, translatedAddr);
                    barcodeContainer.put(addr, barcode);
                    return new DNATranslatedAddress(translatedAddr, barcode);
                }
                else {
                    badAddresses.incrementAndGet();
                }
            }
        }
        return new DNATranslatedAddress(translatedAddr, barcode);
        //throw new RuntimeException("failed translating: " + addr);
    }

    public DNATranslatedAddress computeTranslationOptimistic(Long addr) {
        int trials = 0;
        long translatedAddr = addr;
        BaseSequence barcode = null;
        while(trials++ < addressTranslationTrials) {
            translatedAddr = addrGen.get();
            barcode = coder.apply(translatedAddr);
            if (isSufficientDistance(barcode)) {
                lsh.insert(barcode);
                translationContainer.put(addr, translatedAddr);
                barcodeContainer.put(addr, barcode);
                return new DNATranslatedAddress(translatedAddr, barcode);
            }
            else {
                badAddresses.incrementAndGet();
            }
        }
        return new DNATranslatedAddress(translatedAddr, barcode);
        //throw new RuntimeException("failed translating: " + addr);
    }


    private boolean isSufficientDistance(BaseSequence barcode) {
        return DistanceCoder.distanceScore(barcode, lsh) >= minDist && DistanceCoder.distanceScore(barcode.complement(), lsh) >= minDist;
    }

    public LSH<BaseSequence> getLsh() {
        return lsh;
    }

    @Override
    public long size() {
        return translationContainer.size();
    }

    @Override
    public long badAddressesSize() {
        return badAddresses.get();
    }

    public Container<Long, BaseSequence, ?> getBarcodeContainer() {
        return barcodeContainer;
    }

    @Override
    public Iterator<Long> usedAddressesIterator() {
        return new Iterator<>() {
            final long start = addrGen.getStart();
            long current = start;
            @Override
            public boolean hasNext() {
                return current < addrGen.getCurrentNextFreeId();
            }
            @Override
            public Long next() {
                return current++;
            }
        };
    }

    public Container<Long, Long, ?> getTranslationContainer() {
        return translationContainer;
    }

    @Override
    public boolean isPersistent() {
        return persistentTranslation;
    }

    public boolean isPersistentBarcodes() {
        return persistentBarcodes;
    }

    public record DNATranslatedAddress(long addr, BaseSequence barcode) implements TranslatedAddress<BaseSequence> {
        @Override
        public BaseSequence address() {
            return barcode;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        public static final int DEFAULT_ADDRESS_TRANSLATION_TRIALS = 1000;
        public static final int DEFAULT_ADDRESS_SIZE = 80;
        public static final int DEFAULT_MIN__ADDRESS_SIZE = 20;
        public static final int DEFAULT_ADDR_NUM_PERMUTATIONS = 16;
        public static final int DEFAULT_ECC_LEN = 0;
        public static final double DEFAULT_MIN_DIST = 0.3d;
        public static final boolean DEFAULT_PERSISTENT_CONTAINERS = true;
        public static final boolean DEFAULT_DEEP_LSH = true;
        public static final Coder<String, BaseSequence> DEFAULT_STRING_CODER = RotatingTre.INSTANCE;
        public static final BiFunction<Integer, Boolean, LSH<BaseSequence>> DEFAULT_LSH = (addrSize, deep) -> deep ? MinHashLSH.newLSHForBaseSequences(4 * Math.max(1, addrSize / 200), 200, 20) : MinHashLSHLight.newLSHForBaseSequences(4 * Math.max(1, addrSize / 200), 200, 20);
        public static final Supplier<UniqueIDGenerator> DEFAULT_ADDR_GEN = UniqueIDGenerator::new;
        public static final Supplier<DNARule> DEFAULT_DNA_RULES = () -> BasicDNARules.INSTANCE;
        public static final Function<Boolean, Container<Long, Long, Long>> DEFAULT_TRANSLATION_CONTAINER = persistent -> persistent ? new PersistentContainer<>("translation.table", FixedSizeSerializer.LONG) : Container.MapContainer.ofLong();
        public static final BiFunction<Integer, Boolean, Container<Long, BaseSequence, Long>> DEFAULT_BARCODE_CONTAINER = (addrSize, persistent) -> persistent ? new PersistentContainer<>("barcodes.table", new FixedSizeSerializer<>() {
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
        }) : Container.MapContainer.ofLong();

        private LSH<BaseSequence> lsh;
        private int numPermutations;
        private int addressEccSize;
        private int addrSize;
        private DNARule dnaRules;
        private Container<Long, Long, ?> translationContainer;
        private Container<Long, BaseSequence, ?> barcodeContainer;
        private UniqueIDGenerator addrGen;
        private Coder<String, BaseSequence> stringCoder;
        private Double minDist;
        private Boolean translationIsPersistent;
        private Boolean barcodesIsPersistent;
        private int addressTranslationTrials;
        private Boolean deepLSH;

        public DNAAddrTranslationManager build() {
            this.addressEccSize = FuncUtils.conditionOrElse(addressEccSize, ecc -> ecc >= 0, () -> DEFAULT_ECC_LEN);
            this.addrSize = FuncUtils.conditionOrElse(addrSize, s -> s >= DEFAULT_MIN__ADDRESS_SIZE, () -> DEFAULT_ADDRESS_SIZE);
            if (addressEccSize > 0 && addrSize % 4 != 0)
                throw new RuntimeException("addressSize % 4 != 0");

            this.deepLSH = FuncUtils.nullEscape(deepLSH, () -> DEFAULT_DEEP_LSH);
            this.lsh = FuncUtils.nullEscape(lsh, () -> DEFAULT_LSH.apply(addrSize, deepLSH));
            this.numPermutations = FuncUtils.conditionOrElse(numPermutations, n -> n >= 0, () -> DEFAULT_ADDR_NUM_PERMUTATIONS);
            this.dnaRules = FuncUtils.nullEscape(dnaRules, DEFAULT_DNA_RULES);
            this.translationIsPersistent = FuncUtils.nullEscape(translationIsPersistent, () -> DEFAULT_PERSISTENT_CONTAINERS);
            this.barcodesIsPersistent = FuncUtils.nullEscape(barcodesIsPersistent, () -> DEFAULT_PERSISTENT_CONTAINERS);
            this.translationContainer = FuncUtils.nullEscape(translationContainer, () -> DEFAULT_TRANSLATION_CONTAINER.apply(translationIsPersistent));
            this.barcodeContainer = FuncUtils.nullEscape(barcodeContainer, () -> DEFAULT_BARCODE_CONTAINER.apply(addrSize, barcodesIsPersistent));
            this.minDist = FuncUtils.conditionOrElse(minDist, d -> d != null && d > 0d && d <= 1d, () -> DEFAULT_MIN_DIST);
            this.stringCoder = FuncUtils.nullEscape(stringCoder, () -> DEFAULT_STRING_CODER);
            this.addrGen = FuncUtils.nullEscape(addrGen, DEFAULT_ADDR_GEN);
            this.addressTranslationTrials = FuncUtils.conditionOrElse(addressTranslationTrials, t -> t > 0, () -> DEFAULT_ADDRESS_TRANSLATION_TRIALS);

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

            return new DNAAddrTranslationManager(
                    lsh,
                    minDist,
                    addrSize,
                    coder,
                    addressTranslationTrials,
                    translationContainer,
                    barcodeContainer,
                    translationIsPersistent,
                    barcodesIsPersistent
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

        public Builder setAddrGen(UniqueIDGenerator addrGen) {
            this.addrGen = addrGen;
            return this;
        }

        public Builder setMinDist(double minDist) {
            this.minDist = minDist;
            return this;
        }

        public Builder setContainers(Container<Long, Long, ?> translationContainer, Container<Long, BaseSequence, ?> barcodeContainer, boolean translationIsPersistent, boolean barcodesIsPersistent) {
            this.translationContainer = translationContainer;
            this.barcodeContainer = barcodeContainer;
            this.translationIsPersistent = translationIsPersistent;
            this.barcodesIsPersistent = barcodesIsPersistent;
            return this;
        }

        public Builder setPersistent(boolean translationIsPersistent, boolean barcodesIsPersistent) {
            this.translationIsPersistent = translationIsPersistent;
            this.barcodesIsPersistent = barcodesIsPersistent;
            this.translationContainer = null;
            this.barcodeContainer = null;
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
            return null;
        }
        @Override
        public BaseSequence extractPayload(BaseSequence encoded) {
            return encoded.window(lengthBase.totalSize());
        }
    }
}
