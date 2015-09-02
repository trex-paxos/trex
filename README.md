## Building

```
# Kick the tires
sbt clean coverage test it:test
sbt coverageReport
```
[![Build Status](https://travis-ci.org/trex-paxos/trex.svg?branch=master)](https://travis-ci.org/trex-paxos/trex)
[![Codacy Badge](https://www.codacy.com/project/badge/73b345d5a4c74a4d9d458596e64fe212)](https://www.codacy.com/app/simbo1905remixed/trex)

## What is this? 

Checkout the [GitHub pages] (http://trex-paxos.github.io/trex/) for more information.

## Status /  Work Plan

- [x] replace pickling
- [x] fix driver
- [x] is retransmission of accepted values actually safe?
- [x] overrideable send methods
- [ ] fix the fixmes
- [ ] actor refactor to handlers
- [ ] nemesis (jepsen style destruction testing)

0.6 - practical

- [ ] java demo
- [ ] complete the TODOs
- [ ] dynamic cluster membership  
- [ ] snapshots and out of band retransmission
- [ ] metrics/akka-tracing
- [ ] binary tracing 
- [ ] jumbo UDP packets
- [ ] learners

0.7 - performance

- [ ] batching 
- [ ] remove remote actor use akka tcp
- [ ] multicast 
- [ ] strong read optimisation
- [ ] noop heartbeats to suppress duels
- [ ] compression 
- [ ] journal truncation by size 

0.8 

- [ ] final API 
- [ ] mapdb compression
- [ ] weak reads demo

M1

- [ ] transaction demo
- [ ] ???

## Attribution

The TRex icon is Tyrannosaurus Rex by Raf Verbraeken from the Noun Project licensed under [CC3.0](http://creativecommons.org/licenses/by/3.0/us/)

