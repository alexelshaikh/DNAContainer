package datastructures.container.impl;

import core.BaseSequence;
import core.dnarules.BasicDNARules;
import core.dnarules.DNARule;
import dnacoders.GCFiller;
import utils.AddressedDNA;
import datastructures.container.Container;
import datastructures.container.DNAContainer;
import datastructures.container.AddressTranslationManager;
import datastructures.container.translation.DNAAddrTranslationManager;
import dnacoders.BasicSegmentationCoder;
import dnacoders.DistanceCoder;
import dnacoders.SegmentationCoder;
import dnacoders.headercoders.ReedSolomonCoder;
import utils.*;
import utils.lsh.LSH;
import utils.lsh.minhash.MinHashLSH;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class SizedDNAContainer extends Container.LongContainer<BaseSequence, BaseSequence> implements DNAContainer {

    private final SegmentationCoder segmentationCoder;
    private final LSH<BaseSequence> oligLSH;
    private final int payloadSize;
    private final DNAPacker.LengthBase sizedHeaderLengthBase;
    private final Container<Long, AddressedDNA[], ?> store;
    private final Coder<AddressedDNA, AddressedDNA> oligoDistanceCoder;
    private final AddressTranslationManager<Long, BaseSequence> addressTranslationManager;
    private final int payloadOffset;

    private SizedDNAContainer(SegmentationCoder segmentationCoder, Coder<AddressedDNA, AddressedDNA> oligoDistanceCoder, LSH<BaseSequence> oligLSH, int payloadSize, DNAPacker.LengthBase sizedHeaderLengthBase, Container<Long, AddressedDNA[], ?> store, AddressTranslationManager<Long, BaseSequence> addressTranslationManager) {
        super();
        this.segmentationCoder = segmentationCoder;
        this.oligLSH = oligLSH;
        this.payloadSize = payloadSize;
        this.sizedHeaderLengthBase = sizedHeaderLengthBase;
        this.store = store;
        this.addressTranslationManager = addressTranslationManager;
        this.payloadOffset = sizedHeaderLengthBase.totalSize() + DNAPacker.LengthBase.INT_64.totalSize();
        this.oligoDistanceCoder = oligoDistanceCoder;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Long put(BaseSequence seq) {
        long id = registerId();
        put(id, seq);
        return id;
    }

    @Override
    public void put(Long id, BaseSequence seq) {
        int numSegmentsMinusOne = segmentationCoder.numSegments(seq.length() + sizedHeaderLengthBase.totalSize() + DNAPacker.LengthBase.INT_64.totalSize()) - 1;
        if (new BitString().append(numSegmentsMinusOne, false).length() > sizedHeaderLengthBase.bitCount)
            throw new RuntimeException("payload too large for the specified sizedHeaderLengthBase. Consider using a larger Packer.Type!");

        Long[] ids = this.registerIds(numSegmentsMinusOne);
        BaseSequence sizedPayload = BaseSequence.join(
                DNAPacker.pack(ids.length > 0 ? ids[0] : -1L, DNAPacker.LengthBase.INT_64),
                DNAPacker.pack(numSegmentsMinusOne, sizedHeaderLengthBase),
                seq);

        var segments = segmentationCoder.encode(sizedPayload);
        var addressedSegments = new AddressedDNA[segments.length];
        addressedSegments[0] = oligoDistanceCoder.encode(new AddressedDNA(addressTranslationManager.translate(id).address(), segments[0]));

        var translatedIds = addressTranslationManager.translateN(ids);
        IntStream.range(0, translatedIds.size()).forEach(i -> addressedSegments[i + 1] = oligoDistanceCoder.encode(new AddressedDNA(translatedIds.get(i).address(), segments[i + 1])));
        store.put(id, addressedSegments);
    }

    @Override
    public BaseSequence get(Long id) {
        var oligos = getOligos(id);
        return oligos == null? null : assembleFromOligos(oligos);
    }

    @Override
    public long size() {
        return store.size();
    }

    public BaseSequence getSized(long id) {
        var oligos = getOligos(id);
        return oligos == null? null : assembleSizedFromOligos(oligos);
    }

    @Override
    public AddressTranslationManager<Long, BaseSequence> getAddressTranslationManager() {
        return addressTranslationManager;
    }

    @Override
    public Container<Long, AddressedDNA[], ?> getOligoStore() {
        return store;
    }

    public int unpackSizeFromSizedPayload(BaseSequence sizedPayload) {
        return DNAPacker.unpack(sizedPayload.window(DNAPacker.LengthBase.INT_64.totalSize()), false).intValue() + 1;
    }

    private BaseSequence assembleFromOligos(AddressedDNA[] oligos) {
        return assembleSizedFromOligos(oligos).window(payloadOffset);
    }

    private BaseSequence assembleSizedFromOligos(AddressedDNA[] oligos) {
        return segmentationCoder.decode(Arrays.stream(oligos).map(oligoDistanceCoder::decode).map(AddressedDNA::payload).toArray(BaseSequence[]::new));
    }

    @Override
    public Set<Long> keys() {
        return store.keys();
    }

    @Override
    public int getAddressSize() {
        return addressTranslationManager.addressSize();
    }

    @Override
    public int getPayloadSize() {
        return payloadSize;
    }

    public LSH<BaseSequence> getOligoLSH() {
        return oligLSH;
    }


    public static class Builder {
        public static final int DEFAULT_PAYLOAD_SIZE = 150;
        public static final int DEFAULT_PAYLOAD_ECC_SIZE = 0;

        public static final float DEFAULT_OLIGO_ERROR_WEIGHT = 1.0f;
        public static final float DEFAULT_OLIGO_DISTANCE_WEIGHT = 1.0f;
        public static final BiFunction<Integer, Integer, LSH<BaseSequence>> DEFAULT_OLIGO_LSH = (addrSize, payloadSize) -> MinHashLSH.newLSHForBaseSequences(1 + 4 * Math.max(1, (addrSize + payloadSize) / 200), 200, 20);

        public static final int DEFAULT_PAYLOAD_NUM_PERMUTATIONS = 16;
        public static final DNAPacker.LengthBase DEFAULT_SIZED_HEADER = DNAPacker.LengthBase.SHORT;
        public static final DNARule DEFAULT_DNA_RULES = BasicDNARules.INSTANCE;
        public static final Supplier<Container<Long, AddressedDNA[], ?>> DEFAULT_STORE = MapContainer::ofLong;

        public static final Supplier<AddressTranslationManager<Long, BaseSequence>> DEFAULT_ADDRESS_TRANSLATION_MANAGER =
                () -> DNAAddrTranslationManager
                        .builder()
                        .build();

        private DNARule dnaRules;

        private int payloadSize;
        private int payloadEccSize;
        private int numGcCorrectionsPayload = -1;

        private Float oligoErrorWeight;
        private Float oligoDistanceWeight;

        private int numPayloadPermutations;

        private DNAPacker.LengthBase sizedHeaderLengthBase;
        private Container<Long, AddressedDNA[], ?> store;
        private AddressTranslationManager<Long, BaseSequence> addressTranslationManager;
        private LSH<BaseSequence> oligoLSH;

        public SizedDNAContainer build() {
            this.store = FuncUtils.nullEscape(store, DEFAULT_STORE);
            this.addressTranslationManager = FuncUtils.nullEscape(addressTranslationManager, DEFAULT_ADDRESS_TRANSLATION_MANAGER);
            this.payloadSize = FuncUtils.conditionOrElse(payloadSize, x -> x > 0, () -> DEFAULT_PAYLOAD_SIZE);
            this.payloadEccSize = FuncUtils.conditionOrElse(payloadEccSize, x -> x >= 0, () -> DEFAULT_PAYLOAD_ECC_SIZE);
            if (payloadEccSize > 0 && payloadSize % 4 != 0)
                throw new RuntimeException("infoDnaSize % 4 != 0");
            this.oligoErrorWeight = FuncUtils.nullEscape(oligoErrorWeight, () -> DEFAULT_OLIGO_ERROR_WEIGHT);
            this.oligoDistanceWeight = FuncUtils.nullEscape(oligoDistanceWeight, () -> DEFAULT_OLIGO_DISTANCE_WEIGHT);
            this.sizedHeaderLengthBase = FuncUtils.nullEscape(sizedHeaderLengthBase, () -> DEFAULT_SIZED_HEADER);
            this.numPayloadPermutations = FuncUtils.conditionOrElse(numPayloadPermutations, x -> x >= 0, () -> DEFAULT_PAYLOAD_NUM_PERMUTATIONS);
            this.dnaRules = FuncUtils.nullEscape(dnaRules, () -> DEFAULT_DNA_RULES);
            ReedSolomonCoder payloadEccCoder = FuncUtils.conditionOrElse(null, x -> payloadEccSize <= 0, () -> new ReedSolomonCoder(payloadEccSize));

            int payloadEccOverhead = FuncUtils.conditionOrElse(0, x -> payloadEccCoder == null, () -> payloadEccCoder.overhead(payloadSize));
            this.oligoLSH = FuncUtils.conditionOrElse(oligoLSH, Objects::nonNull, () -> DEFAULT_OLIGO_LSH.apply(addressTranslationManager.addressSize(), payloadSize));

            OligoDistanceCoder oligoDistanceCoder = new OligoDistanceCoder(payloadEccCoder != null ? payloadEccCoder :  Coder.identity(), numPayloadPermutations, oligoLSH, dnaRules, oligoErrorWeight, oligoDistanceWeight);

            this.numGcCorrectionsPayload = FuncUtils.conditionOrElse(numGcCorrectionsPayload, x -> x >= 0, () -> (int) (0.1f * payloadSize));
            SegmentationCoder segmentationCoder = new BasicSegmentationCoder(payloadSize - payloadEccOverhead - oligoDistanceCoder.permutationOverhead(), numGcCorrectionsPayload);

            return new SizedDNAContainer(
                    segmentationCoder,
                    oligoDistanceCoder,
                    oligoLSH,
                    payloadSize,
                    sizedHeaderLengthBase,
                    store,
                    addressTranslationManager
            );
        }

        public Builder setPayloadSize(int payloadSize) {
            this.payloadSize = payloadSize;
            return this;
        }

        public Builder setSizedHeaderLengthBase(DNAPacker.LengthBase sizedHeaderLengthBase) {
            this.sizedHeaderLengthBase = sizedHeaderLengthBase;
            return this;
        }

        public Builder setStore(Container<Long, AddressedDNA[], ?> store) {
            this.store = store;
            return this;
        }

        public Builder setOligoLSH(LSH<BaseSequence> oligoLSH) {
            this.oligoLSH = oligoLSH;
            return this;
        }

        public Builder setAddressTranslationManager(AddressTranslationManager<Long, BaseSequence> addressTranslationManager) {
            this.addressTranslationManager = addressTranslationManager;
            return this;
        }

        public Builder setPayloadEccSize(int payloadEccSize) {
            this.payloadEccSize = payloadEccSize;
            return this;
        }

        public Builder setNumGcCorrectionsPayload(int numGcCorrectionsPayload) {
            this.numGcCorrectionsPayload = numGcCorrectionsPayload;
            return this;
        }

        public Builder setDnaRules(DNARule dnaRules) {
            this.dnaRules = dnaRules;
            return this;
        }

        public Builder setOligoErrorWeight(float oligoErrorWeight) {
            this.oligoErrorWeight = oligoErrorWeight;
            return this;
        }

        public Builder setOligoDistanceWeight(float oligoDistanceWeight) {
            this.oligoDistanceWeight = oligoDistanceWeight;
            return this;
        }

        public Builder setNumPayloadPermutations(int numPayloadPermutations) {
            this.numPayloadPermutations = numPayloadPermutations;
            return this;
        }


        private static class OligoDistanceCoder implements Coder<AddressedDNA, AddressedDNA> {
            private final Coder<BaseSequence, BaseSequence> payloadEccCoder;
            private final int permutationOverhead;
            private final DNAPacker.LengthBase lb;
            private final Function<BaseSequence, Float> scoreFunc;
            private final LSH<BaseSequence> oligoLSH;
            private final int permsCount;

            public OligoDistanceCoder(Coder<BaseSequence, BaseSequence> payloadEccCoder, int numPayloadPermutations, LSH<BaseSequence> oligoLSH, DNARule rules, float errorWeight, float distanceWeight) {
                this.scoreFunc = seq -> -errorWeight * rules.evalErrorProbability(seq) + distanceWeight * Math.min(DistanceCoder.distanceScore(seq, oligoLSH), DistanceCoder.distanceScore(seq.complement(), oligoLSH));
                this.oligoLSH = oligoLSH;
                this.payloadEccCoder = payloadEccCoder;
                this.lb = DNAPacker.LengthBase.fromUnsignedNumber(numPayloadPermutations);
                this.permsCount = numPayloadPermutations;
                this.permutationOverhead = lb.totalSize();
            }

            @Override
            public AddressedDNA encode(AddressedDNA addressedDNA) {
                if (permsCount <= 0) {
                    BaseSequence payload = addressedDNA.payload();
                    BaseSequence paddedPayload = payloadEccCoder.encode(BaseSequence.join(GCFiller.getTrimmedFiller(payload, permutationOverhead), payload));
                    AddressedDNA encoded = new AddressedDNA(addressedDNA.address(), paddedPayload);
                    oligoLSH.insert(encoded.join());
                    return encoded;
                }

                var payload = addressedDNA.payload();
                var address = addressedDNA.address();
                long seed = payload.seed();
                int payloadLength = payload.length();
                var encoded = IntStream.range(0, permsCount)
                        .mapToObj(i -> {
                            BaseSequence payloadPermuted = payload.permute(FuncUtils.getUniformPermutation(seed + i, payloadLength));
                            BaseSequence result = DNAPacker.pack(i, lb);
                            result.append(payloadPermuted);
                            var ad = new AddressedDNA(address, payloadEccCoder.encode(result));
                            return new Pair<>(ad, ad.join());
                        })
                        .max(Comparator.comparing(p -> scoreFunc.apply(p.getT2())))
                        .orElseThrow();

                oligoLSH.insert(encoded.getT2());
                return encoded.getT1();
            }

            @Override
            public AddressedDNA decode(AddressedDNA encoded) {
                var payload = payloadEccCoder.decode(encoded.payload());
                if (permsCount <= 0)
                    return new AddressedDNA(encoded.address(), payload.window(permutationOverhead));

                var offset = lb.unpackSingle(payload, false).intValue();
                BaseSequence payloadWithoutHeader = payload.window(permutationOverhead);
                return new AddressedDNA(encoded.address(), payloadWithoutHeader.permute(FuncUtils.getUniformPermutation(payloadWithoutHeader.seed() + offset, payloadWithoutHeader.length()).reverseInPlace()));
            }

            private int permutationOverhead() {
                return permutationOverhead;
            }
        }
    }
}
