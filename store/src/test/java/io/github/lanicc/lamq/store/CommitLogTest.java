package io.github.lanicc.lamq.store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created on 2022/6/22.
 *
 * @author lan
 */
class CommitLogTest {

    String commitLogDir = null;


    @BeforeEach
    void setUp() {
        String thisClassFile = this.getClass().getName().replaceAll("\\.", "/") + ".class";
        //noinspection ConstantConditions
        String path = Thread.currentThread().getContextClassLoader().getResource(thisClassFile).getPath();
        commitLogDir = path.replace(this.getClass().getSimpleName() + ".class", "") + "/dataDir/commit";
    }

    @Test
    void append() throws IOException {
        CommitLog commitLog = new CommitLog(commitLogDir);

        byte[] bytes = "hello world".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 102400; i++) {
            CommitLog.WriteResult result = commitLog.append(bytes);
            Assertions.assertTrue(result.success());
            System.out.println(result);
        }

        commitLog.close();
    }
}
