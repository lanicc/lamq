package io.github.lanicc.lamq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created on 2022/6/22.
 *
 * @author lan
 */
public class IndexFile implements Closeable {

    static Logger logger = LoggerFactory.getLogger(IndexFile.class);

    private final MappedFileQueue mappedFileQueue;

    private final Lock putMessageLock;

    private MappedFile mappedFile;

    private MappedByteBuffer mappedByteBuffer;

    private ByteBuffer header;

    private int length = 0;

    private final static int HEADER_LENGTH = 4;
    private final static int INDEX_LENGTH = 12;

    private final static int MAPPED_FILE_SIZE = 1024 * 1024 * 1024;

    public IndexFile(String indexLogDir) throws IOException {
        mappedFileQueue = new MappedFileQueue(indexLogDir, MAPPED_FILE_SIZE);
        putMessageLock = new ReentrantLock();
    }

    public void appendIndex(Index index) throws IOException {
        putMessageLock.lock();
        try {
            rollNextWriteable();
            mappedByteBuffer.putLong(index.getPosition());
            mappedByteBuffer.putInt(index.getLength());
            length += 12;
        } finally {
            putMessageLock.unlock();
        }
    }

    private void rollNextWriteable() throws IOException {
        if (Objects.nonNull(mappedFile)) {
            if (mappedFile.writeable(INDEX_LENGTH)) {
                return;
            } else {
                header.putInt(0, length);
                mappedFile.flush();
                length = 0;
            }
        } else {
            MappedFile last = mappedFileQueue.last();
            if (Objects.nonNull(last)) {
                last.setWriteable(true);
            }
        }
        mappedFile = mappedFileQueue.getWriteable(INDEX_LENGTH);
        mappedByteBuffer = mappedFile.getMappedByteBuffer();
        header = mappedByteBuffer.slice();
        length = header.getInt(0);
        mappedByteBuffer.position(HEADER_LENGTH + length);
    }

    public Index read(long offset) {
        int perIndexFileCommitLogCount = (MAPPED_FILE_SIZE - HEADER_LENGTH) / INDEX_LENGTH;
        long indexFileOffset = offset / perIndexFileCommitLogCount;
        long indexFilePosition = (offset % perIndexFileCommitLogCount) * INDEX_LENGTH + HEADER_LENGTH;
        long mappedFileOffset = MAPPED_FILE_SIZE * indexFileOffset + indexFilePosition;
        byte[] bytes = new byte[INDEX_LENGTH];
        mappedFileQueue.read(mappedFileOffset, bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long position = buffer.getLong();
        int length = buffer.getInt();
        return Index.of(position, length);
    }


    @Override
    public void close() throws IOException {
        if (Objects.nonNull(header)) {
            header.putInt(0, length);
        }
        mappedFileQueue.close();
    }

    public static class Index {

        private final long position;

        private final int length;

        public static Index of(CommitLog.WriteResult result) {
            return new Index(result.getPosition(), result.getLength());
        }

        public static Index of(long position, int length) {
            return new Index(position, length);
        }

        public Index(long position, int length) {
            this.position = position;
            this.length = length;
        }

        public long getPosition() {
            return position;
        }

        public int getLength() {
            return length;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Index.class.getSimpleName() + "[", "]")
                    .add("position=" + position)
                    .add("length=" + length)
                    .toString();
        }
    }


}
