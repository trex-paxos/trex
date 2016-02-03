package com.github.trex_paxos.javademo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class StringStackImpl implements StringStack {
    private java.util.Stack<String> stack = new java.util.Stack<String>();

    public StringStackImpl() throws IOException {
        this(File.createTempFile("stack", "data"));
    }

    private final RandomAccessFile file;

    final static int END_MARKER = 0;

    public StringStackImpl(File location) throws IOException {
        file = new RandomAccessFile(location.getCanonicalPath(), "rws");
        // initialize an empty file with an end marker
        if (file.length() == 0) {
            file.writeInt(END_MARKER);
            file.seek(file.getFilePointer()-4);
        }
        // read the first record length
        int l = file.readInt();
        // until you hit the end marker slurp the data
        while (l != 0) {
            byte[] bytes = new byte[l];
            file.read(bytes);
            String read = new String(bytes);
            stack.push(read);
            l = file.readInt();
        }
        // backup 4 bytes so that the next write overwrites the end marker
        file.seek(file.getFilePointer() - 4);
    }

    @Override
    public String push(String item) {
        byte[] bytes = item.getBytes();
        try {
            byte[] bs = ByteBuffer.allocate(4 + bytes.length + 4)
                    .putInt(bytes.length)
                    .put(bytes)
                    .putInt(END_MARKER)
                    .array();
            file.write(bs);
            file.getFD().sync();
            // backup 4 bytes so that the next write overwrites the end marker
            file.seek(file.getFilePointer() - 4);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return stack.push(item);
    }

    @Override
    public String pop() {
        if (!stack.empty()) {
            String value = stack.peek();
            byte[] bytes = value.getBytes();
            try {
                // write the end marker over the length of the last record
                long cur = file.getFilePointer();
                long nnew = cur - bytes.length - 4;
                file.seek(nnew);
                file.writeInt(END_MARKER);
                // backup 4 bytes so that the next write overwrites the end marker
                file.seek(nnew);
            } catch (IOException e) {
                System.err.println(e.toString());
                throw new AssertionError(e);
            }
        }
        return stack.pop();
    }

    @Override
    public String peek() {
        return stack.peek();
    }

    @Override
    public boolean empty() {
        return stack.empty();
    }

    @Override
    public int search(Object o) {
        return stack.search(o);
    }
}

