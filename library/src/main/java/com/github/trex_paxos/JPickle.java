package com.github.trex_paxos;

import com.github.trex_paxos.library.Accept;
import com.github.trex_paxos.library.Progress;
import com.github.trex_paxos.util.ByteChain$;
import com.github.trex_paxos.util.Pickle$;

public class JPickle {
    public static byte[] pickleProgress(final Progress progress) {
        return Pickle$.MODULE$.pickleProgress(progress).toBytes();
    }

    public static Progress unpickleProgress(final byte[] bytes) {
        return Pickle$.MODULE$.unpickleProgress(ByteChain$.MODULE$.apply(bytes).iterator());
    }

    public static byte[] pickleAccept(final Accept accept) {
        return Pickle$.MODULE$.pickleAccept(accept).toBytes();
    }

    public static Accept unpickleAccept(final byte[] bytes) {
        return Pickle$.MODULE$.unpickleAccept(ByteChain$.MODULE$.apply(bytes).iterator());
    }
}
