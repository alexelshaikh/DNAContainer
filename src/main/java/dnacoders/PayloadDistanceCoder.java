package dnacoders;

import core.BaseSequence;
import core.dnarules.DNARule;
import utils.AddressedDNA;
import utils.Coder;
import utils.DNAPacker;
import utils.FuncUtils;
import utils.lsh.LSH;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;

public class PayloadDistanceCoder implements Coder<AddressedDNA, AddressedDNA> {
    public static final boolean DEFAULT_PARALLEL = false;
    public static final boolean DEFAULT_PARTITIONED_MODE = true;
    public static final int DEFAULT_PARTITIONED_SIZE = 300;

    private final Coder<BaseSequence, BaseSequence> payloadEccCoder;

    private final int permutationOverhead;
    private final int payloadPermutations;

    private final float errorWeight;
    private final float distWeight;

    private final boolean partitionedMode;
    private final int partitionedSize;

    private final DNARule errorRule;

    private final DNAPacker.LengthBase lb;
    private final LSH<BaseSequence> lsh;

    private final boolean parallel;

    public PayloadDistanceCoder(boolean parallel, boolean partitionedMode, int partitionedSize, LSH<BaseSequence> lsh, DNARule errorRule, Coder<BaseSequence, BaseSequence> payloadEccCoder, int payloadPermutations, float errorWeight, float distWeight) {
        this.payloadPermutations = payloadPermutations;
        this.lsh = lsh;
        this.errorRule = errorRule;
        this.errorWeight = errorWeight;
        this.distWeight = distWeight;
        this.lb = DNAPacker.LengthBase.fromUnsignedNumber(payloadPermutations);
        this.permutationOverhead = lb.totalSize();
        this.payloadEccCoder = payloadEccCoder;
        this.parallel = parallel;
        this.partitionedMode = partitionedMode;
        this.partitionedSize = partitionedSize;
    }

    public PayloadDistanceCoder(LSH<BaseSequence> lsh, DNARule errorRule, Coder<BaseSequence, BaseSequence> payloadEccCoder, int payloadPermutations, float errorWeight, float distWeight) {
        this(DEFAULT_PARALLEL, DEFAULT_PARTITIONED_MODE, DEFAULT_PARTITIONED_SIZE, lsh, errorRule, payloadEccCoder, payloadPermutations, errorWeight, distWeight);
    }

    public PayloadDistanceCoder(LSH<BaseSequence> lsh, DNARule errorRule, int payloadPermutations, float errorWeight, float distWeight) {
        this(
                lsh,
                errorRule,
                Coder.identity(),
                payloadPermutations,
                errorWeight,
                distWeight
        );
    }

    public PayloadDistanceCoder(boolean parallel, boolean partitionedMode, int partitionedSize, LSH<BaseSequence> lsh, DNARule errorRule, int payloadPermutations, float errorWeight, float distWeight) {
        this(
                parallel,
                partitionedMode,
                partitionedSize,
                lsh,
                errorRule,
                Coder.identity(),
                payloadPermutations,
                errorWeight,
                distWeight
        );
    }

    @Override
    public AddressedDNA encode(AddressedDNA addressedDNA) {
        if (payloadPermutations <= 0) {
            BaseSequence payload = addressedDNA.payload();
            BaseSequence paddedPayload = payloadEccCoder.encode(BaseSequence.join(GCFiller.getTrimmedFiller(payload, permutationOverhead), payload));
            ScoredAddressedDNA encoded = new ScoredAddressedDNA(addressedDNA.address(), paddedPayload);
            lsh.insert(encoded.oligo);
            return encoded;
        }

        var payload = addressedDNA.payload();
        var address = addressedDNA.address();
        long seed = payload.seed();
        int payloadLength = payload.length();
        var encoded = FuncUtils.stream(IntStream.range(0, payloadPermutations), parallel)
                .mapToObj(i -> {
                    BaseSequence payloadPermuted = payload.permute(FuncUtils.getUniformPermutation(seed + i, payloadLength));
                    BaseSequence resultPayload = DNAPacker.pack(i, lb);
                    resultPayload.append(payloadPermuted);
                    return new ScoredAddressedDNA(address, payloadEccCoder.encode(resultPayload));
                })
                .peek(jad -> jad.score = score(jad.oligo))
                .max(Comparator.comparing(ad -> ad.score))
                .orElseThrow();

        insertIntoLSH(encoded.oligo);
        return encoded;
    }

    public float score(BaseSequence seq) {
        return partitionedMode ?
                Arrays.stream(seq.splitEvery(partitionedSize)).map(this::scoreUnPartitioned).reduce(Float::sum).orElse(Float.NEGATIVE_INFINITY)  :
                scoreUnPartitioned(seq);
    }

    public float scoreUnPartitioned(BaseSequence seq) {
        return -errorWeight * errorRule.evalErrorProbability(seq)
                + distWeight * Math.min(DistanceCoder.distanceScore(seq, lsh), DistanceCoder.distanceScore(seq.complement(), lsh));
    }

    public void insertIntoLSH(BaseSequence oligo) {
        if (partitionedMode)
            Arrays.stream(oligo.splitEvery(partitionedSize)).forEach(lsh::insert);
        else
            lsh.insert(oligo);
    }

    @Override
    public AddressedDNA decode(AddressedDNA encoded) {
        var payload = payloadEccCoder.decode(encoded.payload());
        if (payloadPermutations <= 0)
            return new AddressedDNA(encoded.address(), payload.window(permutationOverhead));

        var offset = lb.unpackSingle(payload, false).intValue();
        BaseSequence payloadWithoutHeader = payload.window(permutationOverhead);
        return new AddressedDNA(encoded.address(), payloadWithoutHeader.permute(FuncUtils.getUniformPermutation(payloadWithoutHeader.seed() + offset, payloadWithoutHeader.length()).reverseInPlace()));
    }

    public int permutationOverhead() {
        return permutationOverhead;
    }

    public static class ScoredAddressedDNA extends AddressedDNA {
        private volatile BaseSequence oligo;
        private volatile float score;

        public ScoredAddressedDNA(BaseSequence addr, BaseSequence payload) {
            super(addr, payload);
            this.oligo = BaseSequence.join(addr, payload);
            this.score = Float.NaN;
        }

        public static ScoredAddressedDNA of(AddressedDNA ad) {
            if (ad instanceof ScoredAddressedDNA jad)
                return jad;

            return new ScoredAddressedDNA(ad.address(), ad.payload());
        }

        public ScoredAddressedDNA setScore(float score) {
            this.score = score;
            return this;
        }

        public float score() {
            return score;
        }

        @Override
        public BaseSequence join() {
            return oligo;
        }
    }
}
