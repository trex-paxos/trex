package com.github.trex_paxos.javademo;

public class StringStackImpl implements StringStack {
    private java.util.Stack<String> stack = new java.util.Stack<String>();

    @Override
    public String push(String item) {
        System.err.println("push: "+item);
        return stack.push(item);
    }

    @Override
    public String pop() {
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

