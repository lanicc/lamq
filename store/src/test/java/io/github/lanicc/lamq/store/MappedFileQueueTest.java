package io.github.lanicc.lamq.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created on 2022/6/22.
 *
 * @author lan
 */
class MappedFileQueueTest {

    String dataDir = null;


    @BeforeEach
    void setUp() {
        String thisClassFile = this.getClass().getName().replaceAll("\\.", "/") + ".class";
        //noinspection ConstantConditions
        String path = Thread.currentThread().getContextClassLoader().getResource(thisClassFile).getPath();
        dataDir = path.replace(this.getClass().getSimpleName() + ".class", "") + "/dataDir";
    }
    @Test
    void getWriteable() throws IOException {
        MappedFileQueue mappedFileQueue = new MappedFileQueue(dataDir, 1024 * 4);

        byte[] bytes = "hello world".getBytes(StandardCharsets.UTF_8);
        MappedFile mappedFile = mappedFileQueue.getWriteable(bytes.length);
        MappedByteBuffer mappedByteBuffer = mappedFile.getMappedByteBuffer();
        while (mappedFile.writeable(bytes.length)) {
            mappedByteBuffer.put(bytes);
        }
        mappedFileQueue.close();
    }

    @Test
    void load() {
    }
}
