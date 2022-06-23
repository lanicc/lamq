package io.github.lanicc.lamq.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created on 2022/6/23.
 *
 * @author lan
 */
class MessageStoreTest {

    String dataDir = null;


    @BeforeEach
    void setUp() {
        String thisClassFile = this.getClass().getName().replaceAll("\\.", "/") + ".class";
        //noinspection ConstantConditions
        String path = Thread.currentThread().getContextClassLoader().getResource(thisClassFile).getPath();
        dataDir = path.replace(this.getClass().getSimpleName() + ".class", "") + "/dataDir/topic1";
    }

    @AfterEach
    void afterAll() throws IOException {
        //FileUtil.delete(dataDir);
    }

    @Test
    void append() throws IOException {
        long testSize = 1024 * 1024;
        try (MessageStore store = new MessageStore(dataDir)) {

            for (long i = 0; i < testSize; i++) {
                store.append(String.format("hello %s", i).getBytes(StandardCharsets.UTF_8));
            }

            for (long i = 0; i < testSize; i++) {
                String s = new String(store.read(i), StandardCharsets.UTF_8);
                Assertions.assertEquals(s, String.format("hello %s", i));
                //System.out.println(new String(store.read(i), StandardCharsets.UTF_8));
            }
        }

    }

}
