package utils;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.IntFunction;

public interface Coder<F, T> extends AsymmetricCoder<F, F, T> {

    default Coder<T, F> reverse() {
        return new Coder<>() {
            @Override
            public F encode(T t) {
                return Coder.this.decode(t);
            }
            @Override
            public T decode(F f) {
                return Coder.this.encode(f);
            }
        };
    }

    static <T1, T2, T3> Coder<T1, T3> fuse(Coder<T1, T2> coder1, Coder<T2, T3> coder2) {
        return new Coder<>() {
            @Override
            public T3 encode(T1 t1) {
                return coder2.encode(coder1.encode(t1));
            }
            @Override
            public T1 decode(T3 t3) {
                return coder1.decode(coder2.decode(t3));
            }
        };
    }

    static <T1, T2, T3, T4> Coder<T1, T4> fuse(Coder<T1, T2> coder1, Coder<T2, T3> coder2, Coder<T3, T4> coder3) {
        return new Coder<>() {
            @Override
            public T4 encode(T1 f) {
                return coder3.encode(coder2.encode(coder1.encode(f)));
            }
            @Override
            public T1 decode(T4 t) {
                return coder1.decode(coder2.decode(coder3.decode(t)));
            }
        };
    }

    static<T> Coder<T, T> identity() {
        return new Coder<>() {
            @Override
            public T encode(T t) {
                return t;
            }
            @Override
            public T decode(T t) {
                return t;
            }
        };
    }

    static<F, T> Coder<F, T> of(Function<F, T> encoder, Function<T, F> decoder) {
        return new Coder<>() {
            @Override
            public T encode(F f) {
                return encoder.apply(f);
            }

            @Override
            public F decode(T t) {
                return decoder.apply(t);
            }
        };
    }


    static <F, T> Coder<F, T> of(AsymmetricCoder<F, F, T> asymmetricCoder) {
        return Coder.of(asymmetricCoder::encode, asymmetricCoder::decode);
    }

    static<F, T> Coder<F[], T[]> arrayMapper(Coder<F, T> coder, IntFunction<T[]> tSupp, IntFunction<F[]> fSupp) {
        return new Coder<>() {
            @Override
            public T[] encode(F[] fs) {
                return Arrays.stream(fs).map(coder::encode).toArray(tSupp);
            }

            @Override
            public F[] decode(T[] ts) {
                return Arrays.stream(ts).map(coder::decode).toArray(fSupp);
            }
        };
    }

    static<F, T> Coder<F[], T[]> arrayMapperParallel(Coder<F, T> coder, IntFunction<T[]> tSupp, IntFunction<F[]> fSupp) {
        return new Coder<>() {
            @Override
            public T[] encode(F[] fs) {
                return Arrays.stream(fs).parallel().map(coder::encode).toArray(tSupp);
            }

            @Override
            public F[] decode(T[] ts) {
                return Arrays.stream(ts).parallel().map(coder::decode).toArray(fSupp);
            }
        };
    }
}
