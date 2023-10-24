package utils;

import core.BaseSequence;

public class AddressedDNA extends Pair<BaseSequence, BaseSequence> {
    public AddressedDNA(BaseSequence seq1, BaseSequence seq2) {
        super(seq1, seq2);
    }

    public static AddressedDNA of(BaseSequence oligo, int addrSize) {
        return new AddressedDNA(oligo.window(0, addrSize), oligo.window(addrSize, oligo.length()));
    }

    public BaseSequence address() {
        return t1;
    }

    public BaseSequence payload() {
        return t2;
    }

    public int length() {
        return t1.length() + t2.length();
    }

    public BaseSequence join() {
        return BaseSequence.join(address(), payload());
    }
}
