package dnacoders;

import core.BaseSequence;
import core.dnarules.DNARule;
import dnacoders.headercoders.PermutationCoder;
import utils.FuncUtils;
import utils.lsh.LSH;
import utils.lsh.minhash.MinHashLSH;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class DistanceCoder extends PermutationCoder {

    public static final int LARGE_K = 9;
    private static final int PARALLEL_SIZE_TRIGGER = 5_000;

    private final LSH<BaseSequence> lsh;

    /**
     * Creates an instance of DistanceCoder that optimizes a given DNA sequence by applying a number of permutations to it and returning that permuted DNA sequence that maximizes the score.
     * @param permsCount the number of permutations.
     * @param lsh the LSH instance to use for similarity checks.
     * @param distanceWeight the weight for the distance computed by LSH.
     * @param errorWeight the weight for the error by the DNA rules.
     * @param rules the DNA rules.
     */
    public DistanceCoder(int permsCount, LSH<BaseSequence> lsh, DNARule rules, float errorWeight, float distanceWeight) {
        this(false, permsCount, lsh, rules, errorWeight, distanceWeight);
    }

    /**
     * Creates an instance of DistanceCoder that optimizes a given DNA sequence by applying a number of permutations to it and returning that permuted DNA sequence that maximizes the score.
     * @param permsCount the number of permutations.
     * @param lsh the LSH instance to use for similarity checks.
     * @param rules the DNA rules.
     */
    public DistanceCoder(int permsCount, LSH<BaseSequence> lsh, DNARule rules) {
        this(permsCount, lsh, rules, 1.0f, 1.0f);
    }

    /**
     * Creates an instance of DistanceCoder that optimizes a given DNA sequence by applying a number of permutations to it and returning that permuted DNA sequence that maximizes the score.
     * @param parallel true to compute permutations in parallel, and false to compute them sequentially.
     * @param permsCount the number of permutations.
     * @param lsh the LSH instance to use for similarity checks.
     * @param distanceWeight the weight for the distance computed by LSH.
     * @param errorWeight the weight for the error by the DNA rules.
     * @param rules the DNA rules.
     */
    public DistanceCoder(boolean parallel, int permsCount, LSH<BaseSequence> lsh, DNARule rules, float errorWeight, float distanceWeight) {
        super(parallel, permsCount, seq -> errorWeight * -rules.evalErrorProbability(seq) + distanceWeight * Math.min(distanceScore(seq, lsh), distanceScore(seq.complement(), lsh)));
        this.lsh = lsh;
    }

    @Override
    public BaseSequence encode(BaseSequence seq) {
        BaseSequence result = super.encode(seq);
        lsh.insert(result);
        return result;
    }

    public static float jaccardDistanceLowK(BitSet km1, BitSet km2) {
        BitSet intersectBitSet = (BitSet) km1.clone();
        intersectBitSet.and(km2);
        BitSet km1Copy = (BitSet) km1.clone();
        km1Copy.or(km2);

        return 1.0f - intersectBitSet.cardinality() / (float) km1Copy.cardinality();
    }

    public static BitSet kmersJaccard(BaseSequence seq, int k) {
        BitSet bs = new BitSet();
        for (BaseSequence km : seq.kmers(k))
            bs.set((int) km.toBase4());

        return bs;
    }

    public static float jaccardDistanceHighK(Set<BaseSequence> kmers1, List<BaseSequence> kmers2) {
        Set<BaseSequence> union = new HashSet<>(kmers1);
        Set<BaseSequence> intersection = new HashSet<>(kmers1);
        union.addAll(kmers2);
        intersection.retainAll(kmers2);
        return 1.0f - ((float) intersection.size() / union.size());
    }

    /**
     * Returns the minimum distance (or maximum similarity) of a BaseSequence to a collection of BaseSequence instances inserted into an LSH instance.
     * @param seq the BaseSequence to check.
     * @param lsh the LSH instance that contains the collection of BaseSequence to check seq against.
     * @return the minimum distance of seq to the BaseSequence instances in the LSH.
     */
    public static float distanceScore(BaseSequence seq, LSH<BaseSequence> lsh) {
        return distanceScoreFilter(seq, lsh, __ -> true);
    }

    public static float distanceScoreExclusive(BaseSequence seq, LSH<BaseSequence> lsh) {
        return distanceScoreFilter(seq, lsh, can -> seq != can);
    }

    public static float distanceScoreFilter(BaseSequence seq, LSH<BaseSequence> lsh, Predicate<BaseSequence> filter) {
        if (lsh instanceof MinHashLSH.Traditional<BaseSequence> deep) {
            Set<BaseSequence> hits = deep.candidates(seq);
            if (hits.isEmpty()) {
                return 1.0f;
            }
            else {
                int k = deep.getK();
                Function<BaseSequence, Float> distFunc;
                if (k < LARGE_K) {
                    BitSet km1 = kmersJaccard(seq, k);
                    distFunc = can -> jaccardDistanceLowK(km1, kmersJaccard(can, k));
                }
                else {
                    Set<BaseSequence> kmers = new HashSet<>(seq.kmers(k));
                    distFunc = can -> jaccardDistanceHighK(kmers, can.kmers(k));
                }

                return FuncUtils.stream(hits.stream(), hits.size() > PARALLEL_SIZE_TRIGGER).filter(filter).map(distFunc).min(Float::compare).orElse(1.0f);
            }
        }

        return lsh.query(seq) ? 0.0f : 1.0f;
    }
}

