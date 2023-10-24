package dnacoders;

import core.BaseSequence;
import dnacoders.headercoders.BasicDNAPadder;
import utils.Coder;
import java.util.function.BiFunction;

public class BasicSegmentationCoder implements SegmentationCoder {
    private static final BaseSequence DELIM = new BaseSequence("AC");
    private static final BaseSequence DELIM_ESC = new BaseSequence("GT");

    private final int splitLen;
    private final Coder<BaseSequence, BaseSequence> paddingCoder;

    public BasicSegmentationCoder(int targetLength) {
        this(targetLength, 0);
    }


    public BasicSegmentationCoder(int targetLength, int numGcCorrection) {
        this.splitLen = targetLength - DELIM.length() - 1 - numGcCorrection;
        if (splitLen <= 0)
            throw new RuntimeException("targetLength too small");

        this.paddingCoder = new BasicDNAPadder(targetLength, DELIM, DELIM_ESC);
    }

    public BasicSegmentationCoder(int targetLength, int numGcCorrection, BiFunction<BaseSequence, Integer, BaseSequence> fillerFunc) {
        this.splitLen = targetLength - DELIM.length() - 1 - numGcCorrection;
        if (splitLen <= 0)
            throw new RuntimeException("targetLength too small");

        this.paddingCoder = new BasicDNAPadder(targetLength, DELIM, DELIM_ESC, fillerFunc);
    }

    @Override
    public BaseSequence[] encode(BaseSequence seq) {
        BaseSequence[] split = seq.splitEvery(splitLen);
        BaseSequence[] result = new BaseSequence[split.length];
        for (int i = 0; i < split.length; i++)
            result[i] = paddingCoder.encode(split[i]);

        return result;
    }

    @Override
    public BaseSequence decode(BaseSequence[] seqs) {
        BaseSequence result = new BaseSequence();
        for (BaseSequence seq : seqs)
            result.append(paddingCoder.decode(seq));

        return result;
    }

    @Override
    public int numSegments(int len) {
        return (int) Math.ceil((double) len / splitLen);
    }
}
