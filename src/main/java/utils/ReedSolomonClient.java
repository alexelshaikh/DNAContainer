package utils;

import core.BaseSequence;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReedSolomonClient implements AutoCloseable {

    private static ReedSolomonClient INSTANCE;
    public static String HOST = "127.0.0.1";
    public static int START_PORT = 7000;
    public static int DEFAULT_ECC_LENGTH = 2;
    public static int CHANNELS_COUNT = Runtime.getRuntime().availableProcessors();

    public enum Mode {
        ENCODE(0), DECODE(1);
        private final int modeInt;

        Mode(int modeInt) {
            this.modeInt = modeInt;
        }
    }

    private final ChannelHandler[] handlers;

    public synchronized static ReedSolomonClient getInstance() {
        if (INSTANCE == null)
            INSTANCE = new ReedSolomonClient(START_PORT, CHANNELS_COUNT);

        return INSTANCE;
    }

    private ReedSolomonClient(int startPort, int count) {
        this.handlers = new ChannelHandler[count];
        for (int i = 0; i < count; i++)
            handlers[i] = new ChannelHandler(startPort + i);
    }

    public static void setHost(String host) {
        ReedSolomonClient.HOST = host;
    }

    public static void setStartPort(int startPort) {
        ReedSolomonClient.START_PORT = startPort;
    }

    public static void setChannelsCount(int channelsCount) {
        ReedSolomonClient.CHANNELS_COUNT = channelsCount;
    }

    public boolean isConnected() {
        for (ChannelHandler ch : handlers) {
            if (!ch.isConnected())
                return false;
        }
        return false;
    }

    public int getChannelsCount() {
        return ReedSolomonClient.CHANNELS_COUNT;
    }

    @Override
    public void close() {
        for (ChannelHandler ch : handlers)
            ch.close();
    }

    public BaseSequence encode(BaseSequence seq) {
        return encode(0, seq);
    }

    public BaseSequence encode(int fromSocketNum, BaseSequence seq) {
        return encode(fromSocketNum, DEFAULT_ECC_LENGTH, seq);
    }

    public BaseSequence encode(int fromSocketNum, int ecc_length, BaseSequence seq) {
        int safeId = fromSocketNum % this.handlers.length;
        ChannelHandler ch;
        while (!(ch=this.handlers[safeId]).lock.tryLock()) {
            safeId = (safeId + 1) % this.handlers.length;
        }
        try {
            return ch.encodeLockFree(seq, ecc_length);
        }
        finally {
            ch.lock.unlock();
        }
    }

    public BaseSequence decode(BaseSequence seq) {
        return decode(0, seq);
    }

    public BaseSequence decode(BaseSequence seq, int ecc_length) {
        return decode(0, ecc_length, seq);
    }

    public BaseSequence decode(int fromSocketNum, BaseSequence seq) {
        return decode(fromSocketNum, DEFAULT_ECC_LENGTH, seq);
    }

    public BaseSequence decode(int fromSocketNum, int ecc_length, BaseSequence seq) {
        int safeId = fromSocketNum % this.handlers.length;
        ChannelHandler ch;
        while (!(ch=this.handlers[safeId]).lock.tryLock()) {
            safeId = (safeId + 1) % this.handlers.length;
        }
        try {
            return ch.decodeLockFree(seq, ecc_length);
        }
        finally {
            ch.lock.unlock();
        }
    }

    private static class ChannelHandler implements AutoCloseable {
        private static final int MAX_BUFF_LEN = 8192;

        private SocketChannel channel;
        private final int port;
        private final Lock lock;
        private final ByteBuffer readBuffer;

        public ChannelHandler(int port) {
            this.port = port;
            this.lock = new ReentrantLock();
            this.readBuffer = ByteBuffer.allocate(MAX_BUFF_LEN).order(ByteOrder.LITTLE_ENDIAN);
            connect();
        }

        private boolean isConnected() {
            return this.channel != null && this.channel.isConnected();
        }

        private void connect() {
            if (isConnected())
                return;
            try {
                lock.lock();
                channel = SocketChannel.open();
                channel.connect(new InetSocketAddress(HOST, port));
                if (channel.isConnectionPending())
                    channel.finishConnect();

                channel.configureBlocking(true);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            finally {
                lock.unlock();
            }
        }

        private BitString readBitString() {
            readBuffer.clear();
            int bytesAvailable = Integer.MIN_VALUE;
            try{
                bytesAvailable = channel.read(readBuffer);
            } catch (IOException e) {
                closeAndThrow(e);
            }
            if (bytesAvailable <= 0) {
                closeAndThrow("connection closed from server");
            }
            if (bytesAvailable > MAX_BUFF_LEN)
                closeAndThrow("too long message from server");

            readBuffer.flip();
            int len = readBuffer.getInt();
            if (len <= 0)
                throw new RuntimeException("Error");

            byte[] readBytes = new byte[len];
            readBuffer.get(readBytes);

            return new BitString(readBytes);
        }

        public void send(ByteBuffer bf) {
            FuncUtils.safeRun(() -> channel.write(bf));
        }

        @Override
        public void close() {
            FuncUtils.superSafeRun(() -> channel.close());
        }


        private BaseSequence encodeLockFree(BaseSequence seq, int ecc_length) {
            return encodeOrDecode(seq, ecc_length, Mode.ENCODE);
        }

        private BaseSequence encodeOrDecode(BaseSequence seq, int ecc_length, Mode mode) {
            return SeqBitStringConverter.transform(encodeOrDecode(SeqBitStringConverter.transform(seq), ecc_length, mode));
        }

        private BitString encodeOrDecode(BitString bs, int ecc_length, Mode mode) {
            byte[] seqBytes = bs.toBytes();
            ByteBuffer bf = ByteBuffer.allocate(3 * Integer.BYTES + seqBytes.length).order(ByteOrder.LITTLE_ENDIAN);
            bf.putInt(seqBytes.length);
            bf.putInt(ecc_length);
            bf.putInt(mode.modeInt);
            bf.put(seqBytes);
            bf.flip();

            send(bf);

            return readBitString();
        }

        private BaseSequence decodeLockFree(BaseSequence seq, int ecc_length) {
            return encodeOrDecode(seq, ecc_length, Mode.DECODE);
        }

        private void closeAndThrow(String msg) {
            close();
            throw new RuntimeException(msg);
        }

        private void closeAndThrow(Exception e) {
            close();
            throw new RuntimeException(e);
        }
    }
}
