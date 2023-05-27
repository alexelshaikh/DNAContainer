package dnacoders;

import core.BaseSequence;
import utils.Coder;

public interface SegmentationCoder extends Coder<BaseSequence, BaseSequence[]> {

    int numSegments(int len);
}
