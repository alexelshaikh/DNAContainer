package utils;

import core.BaseSequence;

public class AddressedDNA extends Pair<BaseSequence, BaseSequence> {
    public AddressedDNA(BaseSequence seq1, BaseSequence seq2) {
        super(seq1, seq2);
    }

    public BaseSequence address() {
        return t1;
    }

    public BaseSequence payload() {
        return t2;
    }

    public BaseSequence join() {
        return BaseSequence.join(address(), payload());
    }
}
