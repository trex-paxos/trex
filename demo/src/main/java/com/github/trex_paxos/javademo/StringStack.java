package com.github.trex_paxos.javademo;

public interface StringStack {
    String push(String item);

    String pop();

    String peek();

    boolean empty();

    int search(Object o);
}
