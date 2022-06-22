package io.github.lanicc.lamq.store;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.StringJoiner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created on 2022/6/22.
 *
 * @author lan
 */
public class CommitLog implements Closeable {

    private final MappedFileQueue mappedFileQueue;

    private final Lock putMessageLock;

    private final static int MAPPED_FILE_SIZE = 1024 * 1024 * 1024;

    public CommitLog(String commitLogDir) throws IOException {
        mappedFileQueue = new MappedFileQueue(commitLogDir, MAPPED_FILE_SIZE);
        putMessageLock = new ReentrantLock();
    }

    public WriteResult append(byte[] src) throws IOException {
        putMessageLock.lock();
        try {
            int length = src.length;
            MappedFile mappedFile = mappedFileQueue.getWriteable(length);
            MappedByteBuffer mappedByteBuffer = mappedFile.getMappedByteBuffer();
            long position = mappedByteBuffer.position() + mappedFile.getFileNameOffset();
            mappedByteBuffer.put(src);

            return WriteResult.ofSuccess(position, length);
        } finally {
            putMessageLock.unlock();
        }
    }

    public void read(long offset, byte[] dst) {
        mappedFileQueue.read(offset, dst);
    }

    @Override
    public void close() throws IOException {
        mappedFileQueue.close();
    }

    public static class WriteResult {

        private final boolean success;

        private final long position;

        private final int length;

        public static WriteResult ofSuccess(long position, int length) {
            return new WriteResult(true, position, length);
        }

        public WriteResult(boolean success, long position, int length) {
            this.success = success;
            this.position = position;
            this.length = length;
        }

        public boolean success() {
            return success;
        }

        public long getPosition() {
            return position;
        }

        public int getLength() {
            return length;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", WriteResult.class.getSimpleName() + "[", "]")
                    .add("success=" + success)
                    .add("position=" + position)
                    .add("length=" + length)
                    .toString();
        }
    }
}
