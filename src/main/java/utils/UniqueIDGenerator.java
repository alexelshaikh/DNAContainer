package utils;

import java.util.function.Supplier;

public class UniqueIDGenerator implements Supplier<Long> {

    private long nextFreeId;
    private final long start;

    public UniqueIDGenerator() {
        this(0L);
    }
    public UniqueIDGenerator(long start) {
        this.nextFreeId = start;
        this.start = start;
    }

    @Override
    public Long get() {
        return getN(1)[0];
    }

    public synchronized Long[] getN(int n) {
        long start = nextFreeId;
        nextFreeId += n;
        Long[] result = new Long[n];
        for (int i = 0; i < n; i++)
            result[i] = start++;

       return result;
    }

    public long getStart() {
        return start;
    }

    public synchronized long getCurrentNextFreeId() {
        return nextFreeId;
    }
}
