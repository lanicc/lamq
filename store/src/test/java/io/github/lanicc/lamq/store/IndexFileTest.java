package io.github.lanicc.lamq.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Created on 2022/6/22.
 *
 * @author lan
 */
class IndexFileTest {


    String indexLogDir = null;


    @BeforeEach
    void setUp() {
        String thisClassFile = this.getClass().getName().replaceAll("\\.", "/") + ".class";
        //noinspection ConstantConditions
        String path = Thread.currentThread().getContextClassLoader().getResource(thisClassFile).getPath();
        indexLogDir = path.replace(this.getClass().getSimpleName() + ".class", "") + "/dataDir/index";
    }


    @Test
    void appendIndex() throws IOException {
        try (IndexFile indexFile = new IndexFile(indexLogDir)) {
            for (int i = 0; i < 10240; i++) {
                indexFile.appendIndex(new IndexFile.Index(i, i));
            }
        }
    }

    @Test
    void read() throws IOException {
        try (IndexFile indexFile = new IndexFile(indexLogDir)) {
            for (int i = 0; i < 10240; i++) {
                try {
                    System.out.println(indexFile.read(i));
                } catch (Exception e) {
                    System.out.println(i);
                }
            }
        }
    }
}
