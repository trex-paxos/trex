
![Build Status](https://github.com/trex-paxos/trex/actions/workflows/scala.yml/badge.svg)

## Trex: An embeddable Paxos engine for the JVM

Checkout the [blog posts](https://simbo1905.wordpress.com/2016/01/09/trex-a-paxos-replication-engine/) for a description of this implementation of [Paxos Made Simple](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/12/paxos-simple-Copy). 

## Building

```
# Kick the tires
sbt clean test it:test
sbt coverageReport
```

## Java Demo

Make some state folders with: 

```shell
mkdir -p /private/tmp/2552
mkdir -p /private/tmp/2562
mkdir -p /private/tmp/2572
```

Note that those won't automatically be cleaned up so that you can experiment with crashes.

Then run three versions of `com.github.trex_paxos.javademo.StackClusterNode` assigning them node
numbers `2552`, `2562` and `2572`. In IntelliJ simply run the main method then add the following 
args `server3.conf xxxx` replacing the xxxx with the node numbers. 

Once the cluster is up run the client main method with:

```shell
com.github.trex_paxos.javademo.StackClient clustered client3.conf 127.0.0.1
```

This will appear to do nothing after logging a lot of debug as it is awaiting user input. You can
`push`, `pop` and `peek` some integers such as `push 99`. The values will be replicated across the 
nodes. An example session might look like: 

```
push 1
peek
1
push 2
push 3
pop
3
peek
2
pop
2
pop
1
```

You can experiment with killing nodes and restarting them to see what happens. As long as two our of 
three nodes are running the client should get responses. 

# Releases

Create a release first edit version.sbt, commit, tag, push and then:

```shell script
sbt> publishSigned
sbt> sonatypeReleaseAll
```


## Status /  Work Plan

0.1 - library (released)

- [x] replace pickling
- [x] fix driver
- [x] is retransmission of accepted values actually safe?
- [x] overrideable send methods
- [x] fix the fixmes
- [x] extract a core functional library with no dependencies
- [x] breakup monolithic actor and increase unit test coverage
- [x] java demo

0.2 - Flexible paxos (FPaxos) hooks

- [x] pluggable quorum strategy in the library

0.3 - integrity

- [x] crc32 message integrity 

0.4.0 - 

- [x] expose more methods for new rsocket code 

0.4.1 - 

- [x] Unpickle `byte[]` for Java clients

0.5.0 - JPickle for Java Journal (latest Scala 2.12 support)

- [x] Wrapper that acts as sugar to make it easier to pickle Accepts and Prepares from Java 

0.5.1 - Scala 2.13 support

- [x] Bump to Scala 2.13 and fix all deprecation warnings in preparation for Scala 3. Unfortunately my test dependencies do not yet support Scala 3. 

0.6.0 - MVStore as Journal with backup method

- [ ] Replace MapDB with H2 MVStore

0.a - practical

- [ ] dynamic cluster membership with UPaxos 

0.b - enhanced 

- [ ] learners / scale-out multicast
- [ ] timeline reads
- [ ] noop heartbeats (less duels and partitioned leader detection)
- [ ] snapshots and out of band retransmission
- [ ] metrics/akka-tracing
- [ ] binary tracing 
- [ ] jumbo UDP packets
- [ ] complete the TODOs

0.c - performance

- [ ] strong reads
- [ ] outdated reads
- [ ] optimised journal 
- [ ] batching 
- [ ] remove remote actor from client driver
- [ ] replica strong reads
- [ ] compression 
- [ ] journal truncation by size 
- [ ] periodically leader number boosting

0.d 

- [ ] final API
- [ ] hand-off reads? 

M1

- [ ] transaction demo
- [ ] ???

## Attribution

The TRex icon is Tyrannosaurus Rex by Raf Verbraeken from the Noun Project licensed under [CC3.0](http://creativecommons.org/licenses/by/3.0/us/)
