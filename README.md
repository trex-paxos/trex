
[![Build Status](https://travis-ci.org/trex-paxos/trex.svg?branch=master)](https://travis-ci.org/trex-paxos/trex)

## Trex: An embeddable Paxos engine for the JVM

Checkout the [blog posts](https://simbo1905.wordpress.com/2016/01/09/trex-a-paxos-replication-engine/) for a description of this implementation of [Paxos Made Simple](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/12/paxos-simple-Copy). 

## Releases

[Trex 0.2](https://github.com/trex-paxos/trex/tree/0.2) is now released which allows for pluggable a `QuroumStrategy` which would allow for a more [flexible paxos](https://ssougou.blogspot.co.uk/2016/08/a-more-flexible-paxos.html?m=1) known as [FPaxos](https://arxiv.org/pdf/1608.06696v1.pdf)

[Trex 0.1](https://github.com/trex-paxos/trex/tree/0.1) is now released to [Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.trex-paxos%22)! This release has what is believed to be a correct and [functional paxos library](http://search.maven.org/#artifactdetails%7Ccom.github.trex-paxos%7Ctrex-library_2.11%7C0.1%7Cjar). The other jars ( `core` server and `demo` module) are only enough to run simple demos. A key missing features is that `core` has no logic for dynamic cluster membership. 

## Building

```
# Kick the tires
sbt clean coverage test it:test
sbt coverageReport
```

See the `.travis.yml` for supported jdk and scala versions. 

# Releasing

Create a release:

```shell script
sbt> sonatypeOpen "com.github.trex-paxos" "0.3.2"
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

0.4.0 - Remove Akka dependence from core (keep in tests) 

- [z] expose more methods for new rsocket code 

05 - practical

- [ ] dynamic cluster membership with UPaxos 

0.6 - enhanced 

- [ ] learners / scale-out multicast
- [ ] timeline reads
- [ ] noop heartbeats (less duels and partitioned leader detection)
- [ ] snapshots and out of band retransmission
- [ ] metrics/akka-tracing
- [ ] binary tracing 
- [ ] jumbo UDP packets
- [ ] complete the TODOs

0.7 - performance

- [ ] strong reads
- [ ] outdated reads
- [ ] optimised journal 
- [ ] batching 
- [ ] remove remote actor from client driver
- [ ] replica strong reads
- [ ] compression 
- [ ] journal truncation by size 
- [ ] periodically leader number boosting

0.8 

- [ ] final API
- [ ] hand-off reads? 

M1

- [ ] transaction demo
- [ ] ???

## Attribution

The TRex icon is Tyrannosaurus Rex by Raf Verbraeken from the Noun Project licensed under [CC3.0](http://creativecommons.org/licenses/by/3.0/us/)
