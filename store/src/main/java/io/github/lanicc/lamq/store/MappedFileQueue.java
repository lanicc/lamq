package io.github.lanicc.lamq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Created on 2022/6/22.
 *
 * @author lan
 */
public class MappedFileQueue implements Closeable {

    static Logger logger = LoggerFactory.getLogger(MappedFileQueue.class);

    private final static int MAX_MESSAGE_SIZE = 1024;

    private final String dataDir;

    private final int mappedFileSize;

    private final CopyOnWriteArrayList<MappedFile> mappedFiles;

    public MappedFileQueue(String dataDir, int mappedFileSize) throws IOException {
        this.dataDir = dataDir;
        this.mappedFileSize = mappedFileSize;
        this.mappedFiles = new CopyOnWriteArrayList<>();
        init();
    }

    public void read(long offset, byte[] dst) {
        long fileNameOffset = mappedFileSize * (offset / mappedFileSize);
        int position = (int) offset % mappedFileSize;
        MappedFile first = first();
        if (Objects.isNull(first)) {
            throw new IllegalArgumentException("no such file");
        }
        MappedFile mappedFile = indexOf((int) ((fileNameOffset - first.getFileNameOffset()) / mappedFileSize));
        if (Objects.isNull(mappedFile)) {
            throw new IllegalArgumentException("no such file");
        }
        mappedFile.read(position, dst);
    }

    public MappedFile getWriteable(int size) throws IOException {
        if (size > MAX_MESSAGE_SIZE) {
            throw new IllegalArgumentException("cannot big than " + MAX_MESSAGE_SIZE);
        }
        MappedFile mappedFile = last();
        if (Objects.nonNull(mappedFile)) {
            if (mappedFile.writeable(size)) {
                return mappedFile;
            }
            flushAsync(mappedFile);
        }
        newMappedFile();
        return last();
    }

    private void newMappedFile() throws IOException {
        MappedFile last = last();
        long fileNameOffset = 0;
        if (Objects.nonNull(last)) {
            fileNameOffset = last.getFileNameOffset() + mappedFileSize;
        }
        MappedFile mappedFile = new MappedFile(Paths.get(dataDir, formatFileName(fileNameOffset)), fileNameOffset, mappedFileSize);
        mappedFiles.add(mappedFile);
    }


    private String formatFileName(long fileNameOffset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(fileNameOffset);
    }

    public MappedFile first() {
        return indexOf(0);
    }

    public MappedFile last() {
        return indexOf(mappedFiles.size() - 1);
    }

    private MappedFile indexOf(int index) {
        if (mappedFiles.size() > index && index > -1) {
            return mappedFiles.get(index);
        }
        return null;
    }

    private void flushAsync(MappedFile file) {
        file.flush();
    }

    private void init() throws IOException {
        ensureDataDir();
        load();
    }

    private void ensureDataDir() throws IOException {
        Path dataDirPath = Paths.get(dataDir);
        if (!Files.exists(dataDirPath)) {
            Files.createDirectories(dataDirPath);
        } else {
            if (!Files.isDirectory(dataDirPath)) {
                throw new IllegalArgumentException("dataDir " + dataDirPath + " is not a dir");
            }
        }
    }

    private void load() throws IOException {
        List<File> files =
                Files.list(Paths.get(dataDir))
                        .map(Path::toFile)
                        .sorted(Comparator.comparing(File::getName))
                        .collect(Collectors.toList());

        for (File file : files) {
            if (file.length() != mappedFileSize) {
                logger.warn("file: {} length: {} not match mapped file size {}", file.getName(), file.length(), mappedFileSize);
                return;
            }
        }
        List<MappedFile> mappedFileList =
                files.stream()
                        .map(file -> {
                            try {
                                return new MappedFile(file.toPath(), Long.parseLong(file.getName()), mappedFileSize, false);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .collect(Collectors.toList());

        mappedFiles.addAll(mappedFileList);
    }

    @Override
    public void close() throws IOException {
        for (MappedFile mappedFile : mappedFiles) {
            try {
                mappedFile.close();
            } catch (Exception e) {
                logger.warn("close mapped file error", e);
            }
        }
    }

}
