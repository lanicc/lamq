package io.github.lanicc.lamq.store;

import java.io.File;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Created on 2022/6/23.
 *
 * @author lan
 */
public class FileUtil {

    public static void delete(String file) {
        File f = Paths.get(file)
                .toFile();
        delete(f);
    }

    public static void delete(File f) {
        if (f.exists()) {
            if (f.isDirectory()) {
                File[] files = f.listFiles();
                if (Objects.nonNull(files)) {
                    for (File file1 : files) {
                        delete(file1);
                    }
                }
            }
            //noinspection ResultOfMethodCallIgnored
            f.delete();
        }
    }
}
