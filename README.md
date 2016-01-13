## Building

```
# Kick the tires
sbt clean coverage test it:test
sbt coverageReport
```
[![Build Status](https://travis-ci.org/trex-paxos/trex.svg?branch=master)](https://travis-ci.org/trex-paxos/trex)
[![Codacy Badge](https://www.codacy.com/project/badge/73b345d5a4c74a4d9d458596e64fe212)](https://www.codacy.com/app/simbo1905remixed/trex)
[![Join the chat at https://gitter.im/trex-paxos/trex](https://badges.gitter.im/trex-paxos/trex.svg)](https://gitter.im/trex-paxos/trex?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## What is this? 

Checkout the [GitHub pages] (http://trex-paxos.github.io/trex/) for more information.

## Status /  Work Plan

0.5 - library

- [x] replace pickling
- [x] fix driver
- [x] is retransmission of accepted values actually safe?
- [x] overrideable send methods
- [x] fix the fixmes
- [x] extract a core functional library with no dependencies
- [x] breakup monolithic actor and increase unit test coverage
- [ ] weak reads
- [ ] java demo
- [ ] jepsen destruction tests

0.6 - practical

- [ ] dynamic cluster membership  
- [ ] snapshots and out of band retransmission
- [ ] metrics/akka-tracing
- [ ] binary tracing 
- [ ] jumbo UDP packets
- [ ] learners / scale-out multicast
- [ ] complete the TODOs

0.7 - performance

- [ ] optimised journal 
- [ ] batching 
- [ ] remove remote actor use akka tcp
- [ ] leases
- [ ] noop heartbeats to suppress duels
- [ ] compression 
- [ ] journal truncation by size 
- [ ] periodically leader number boosting

0.8 

- [ ] final API
- [ ] forwarded reads

M1

- [ ] transaction demo
- [ ] ???

## Attribution

The TRex icon is Tyrannosaurus Rex by Raf Verbraeken from the Noun Project licensed under [CC3.0](http://creativecommons.org/licenses/by/3.0/us/)
