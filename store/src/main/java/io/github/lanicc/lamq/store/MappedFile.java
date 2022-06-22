package io.github.lanicc.lamq.store;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Created on 2022/6/22.
 *
 * @author lan
 */
public class MappedFile implements Closeable {

    private final Path file;

    private final long fileNameOffset;

    private final int fileSize;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    private ByteBuffer readBuffer;
    private boolean writeable;

    public MappedFile(Path file, long fileNameOffset, int fileSize) throws IOException {
        this(file, fileNameOffset, fileSize, true);
    }

    public MappedFile(Path file, long fileNameOffset, int fileSize, boolean writeable) throws IOException {
        this.file = file;
        this.fileNameOffset = fileNameOffset;
        this.fileSize = fileSize;
        this.writeable = writeable;
        init();
    }

    private void init() throws IOException {
        fileChannel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        readBuffer = mappedByteBuffer.slice().asReadOnlyBuffer();
    }

    public void read(int position, byte[] dst) {
        ByteBuffer byteBuffer = readBuffer.slice();
        byteBuffer.position(position);
        byteBuffer.get(dst);
    }
    public boolean writeable(long size) {
        return writeable && mappedByteBuffer.remaining() >= size;
    }


    public MappedFile setWriteable(boolean writeable) {
        this.writeable = writeable;
        return this;
    }

    public String getFileName() {
        return file.toFile().getName();
    }

    public long getFileNameOffset() {
        return fileNameOffset;
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public void flush() {
        mappedByteBuffer.force();
    }

    @Override
    public void close() throws IOException {
        flush();
        MappedFile.clean(this.mappedByteBuffer);
        this.fileChannel.close();
    }


    private static void clean(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            invoke(invoke(viewed(buffer), "cleaner"), "clean");
        }
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method method = method(target, methodName, args);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (Method method : methods) {
            if (method.getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null) {
            return buffer;
        } else {
            return viewed(viewedBuffer);
        }
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }
}
