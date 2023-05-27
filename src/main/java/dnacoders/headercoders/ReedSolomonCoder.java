package dnacoders.headercoders;

import core.BaseSequence;
import utils.Coder;
import utils.Packer;
import utils.ReedSolomonClient;
import utils.SeqBitStringConverter;

public class ReedSolomonCoder implements Coder<BaseSequence, BaseSequence> {

    private final static Coder<BaseSequence, BaseSequence> SEQ_PACKER = Coder.of(
            seq -> SeqBitStringConverter.transform(Packer.withBytePadding(SeqBitStringConverter.transform(seq))),
            seq -> SeqBitStringConverter.transform(Packer.withoutBytePadding(SeqBitStringConverter.transform(seq))));


    private final ReedSolomonClient rsClient;
    private final int eccLength;

    public ReedSolomonCoder() {
        this(ReedSolomonClient.DEFAULT_ECC_LENGTH);
    }

    public ReedSolomonCoder(int eccLength) {
        this.eccLength = eccLength;
        this.rsClient = ReedSolomonClient.getInstance();
    }

    @Override
    public BaseSequence decode(BaseSequence seq) {
        return decode(seq, eccLength);
    }

    @Override
    public BaseSequence encode(BaseSequence seq) {
        return encode(seq, eccLength);
    }

    public BaseSequence encode(BaseSequence seq, int eccLength) {
        seq = SEQ_PACKER.encode(seq);
        BaseSequence eccBases = rsClient.encode(0, eccLength, seq);
        return BaseSequence.join(seq, eccBases);
    }

    public BaseSequence decode(BaseSequence seq, int eccLength) {
        return SEQ_PACKER.decode(rsClient.decode(seq, eccLength));
    }

    public int overhead(int seqLen) {
        return overhead(seqLen, eccLength);
    }

    public static int overhead(int seqLen, int eccLength) {
        return 8 + eccLength * 4 + 4 - (seqLen % 4);
    }

    public int getEccLength() {
        return eccLength;
    }
}
