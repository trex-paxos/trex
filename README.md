
[![Build Status](https://travis-ci.org/trex-paxos/trex.svg?branch=master)](https://travis-ci.org/trex-paxos/trex)

## Trex: An embeddable Paxos engine for the JVM

Checkout the [blog posts](https://simbo1905.wordpress.com/2016/01/09/trex-a-paxos-replication-engine/) for a description of this implementation of [Paxos Made Simple](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/12/paxos-simple-Copy). 
s
## Building

```
# Kick the tires
sbt clean test it:test
sbt coverageReport
```

# Releasing

Create a release:

```shell script
sbt> sonatypeDrop comgithubtrex-paxos-1029
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

0.5.0 - JPickle for Java Journal

0.6.0 - MVStore as Journal with backup method

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
