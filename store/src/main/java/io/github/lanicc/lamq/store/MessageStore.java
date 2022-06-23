package io.github.lanicc.lamq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created on 2022/6/23.
 *
 * @author lan
 */
public class MessageStore implements Closeable {

    static Logger logger = LoggerFactory.getLogger(MessageStore.class);

    private final CommitLog commitLog;

    private final IndexFile indexFile;

    public MessageStore(String dataDir) throws IOException {
        commitLog = new CommitLog(dataDir + "/commit");
        indexFile = new IndexFile(dataDir + "/index");
    }

    public void append(byte[] src) throws IOException {
        CommitLog.WriteResult result = commitLog.append(src);
        if (result.success()) {
            indexFile.appendIndex(IndexFile.Index.of(result));
        }
    }

    public byte[] read(long offset) {
        IndexFile.Index index = indexFile.read(offset);
        byte[] dst = new byte[index.getLength()];
        commitLog.read(index.getPosition(), dst);
        return dst;
    }

    @Override
    public void close() throws IOException {
        commitLog.close();
        indexFile.close();
    }
}
