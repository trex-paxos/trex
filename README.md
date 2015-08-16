## Building

```
# kick the tires
sbt clean coverage test it:test
sbt coverageReport
```
[![Build Status](https://travis-ci.org/simbo1905/trex.svg?branch=master)](https://travis-ci.org/simbo1905/trex)
[![Codacy Badge](https://www.codacy.com/project/badge/dd8dd20b797a4e288213f45c10fae494)](https://www.codacy.com/app/doug/trex)

## What is this? 

This is TRex which has a [write up here](https://simbo1905.wordpress.com/2014/10/28/transaction-log-replication-with-paxos/). 

More to come...

## How do I use this? 

Below is a rough sketch of an article to introduce the demo app. 

## Attribution

The TRex icon is Tyrannosaurus Rex by Raf Verbraeken from the Noun Project licensed under [CC3.0](http://creativecommons.org/licenses/by/3.0/us/)

### TRex The Paxos Engine

Previous [posts](https://simbo1905.wordpress.com/2014/10/28/transaction-log-replication-with-paxos/) described how the Paxos algorithm can be applied to binary log replication. This post introduces a Paxos state machine replication engine called TRex. TRex is implemented in Scala using Akka FSM. It can be used to reliably replicate messages, commands or method invocations to create a strongly consistent and fault tolerate cluster with automatic leader failover. The demo code for TRex shows how to wrap an object so that it is replicated across a Paxos cluster. You can fork the code over at TBD.

This post will provide a walk-through of the demo application. A detailed description of how multi-Paxos is implemented in TRex is given in a previous post. TRex also comes with extensive unit tests that are an executable specification of its implementation of the multi-Paxos consensus algorithm. 

Before we begin I should make a statement about the status of the codebase. Currently this post describes version 0.5.0 of the code. Some features that a production replication service would need to provide (e.g. metrics, dynamic cluster membership) are not yet implemented. This blog post will be updated as new features are added to TRex. 

The demo application implements a key-value store that has the following interface:

```Scala
trait ConsistentKVStore {
  /**
   * Add a value into the KV store
   */
  def put(key: String, value: String): Unit

  /**
   * Add a value into the KV store only if the current version number is 'version'
   * @return True if the operation succeeded else False
   */
  def put(key: String, value: String, version: Long): Boolean

  /**
   * Remove a value form the store.
   */
  def remove(key: String): Unit

  /**
   * Remove a value from the KV store only if the current version number is 'version'
   * @return
   */
  def remove(key: String, version: Long): Boolean

  /**
   * Read a value and its version number from the KV store.
   * The setting of the ‘consistent’ flag is used
   * to choose the consistency level. Setting it to ‘true’
   * chooses strong consistency, and the latest value is always
   * returned. Setting it to ‘false’ chooses timeline
   * consistency, and a possibly stale value is returned in
   * exchange for better performance.
   * @param key The key of the value to get
   * @return A tuple of the value and the version number of the value of the key
   */
  def get(key: String): Option[(String,Long)]
}
``` 

The actual implementation isn't important for the purposes of the demo. It simply represents an application service that we will replicate to achieve fault tolerance. We should note that the write methods are safe to repeat during crash recovery. TRex journals that it has completed chosen commands immediately after they have been run. If a crash happens before the journal is flushed then a command will be rerun after the node is restarted. If your application API is not "recovery replay safe" you should code a custom journal class which participates in your application transactions. You would also need to override the TRex Paxos message sending methods to buffer outbound messages. Either send the buffered massages post-commit else drop them post-rollback. 

Now that we understand the object we wish to replicate we can go step-by-step through [the demo code]. It starts with a static TRex cluster configuration:

```
# trex simple cluster configuration
trex {
  # folder to use to persist data at each node
  data-folder="/tmp"
  # number of slots entries to retain in the log to support retransmission
  data-retained=1048576
  # static cluster definintion
  cluster {
    name = "PaxosKVStore"
    nodes = "2552,2562,2572"
    node-2552 {
      host = "127.0.0.1"
      client-port = 2552
      node-port = 2553
    }
    node-2562 {
      host = "127.0.0.1"
      client-port = 2562
      node-port = 2563
    }
    node-2572 {
      host = "127.0.0.1"
      client-port = 2572
      node-port = 2573
    }
  }
  # timeouts
  leader-timeout-max=4000
  leader-timeout-min=2000
}
```

That defines a TRex cluster with three server nodes on localhost exposing a TCP port to clients and a UDP port to the other cluster nodes. A more typical deployment would use three separate hosts and the same ports on each. The client application loads the configuration then creates a dynamic proxy backed by the Paxos cluster:

```Scala
    val cluster = Cluster.parseConfig(config)

    val system =
      ActorSystem(cluster.name, ConfigFactory.load("client.conf"))

    val timeout = Timeout(100 millisecond)

    val driver = system.actorOf(Props(classOf[StaticClusterDriver], timeout, cluster, 20), "TrexDriver")

    val typedActor: ConsistentKVStore =
      TypedActor(system).
        typedActorOf(
          TypedProps[ConsistentKVStore],
          driver)
```
 
That code creates a TRex client driver passing in the cluster configuration. It then creates a TypedActor proxy which forwards onto the TRex driver. The TypedActor proxy pattern is an out-of-the-box Akka feature. It gives us an implementation of the application interface which serialises every method invocation as a MethodCall message. This is sent to the TRex driver which dispatches the message to the current Paxos distinguished leader. We should note that the TRex driver it is agnostic to the client-to-server application protocol. If your application is already using Akka you could just send your existing messages to the driver. You can even configure Akka with a custom serialiser to control the wire format. 

That's it. The remainder of the client application is a trivial loop reading input commands to invoke methods on the interface. The same loop would work invoking methods directly on a local object. We can think of the few lines of Akka code shown above as a drop-in replacement for a local object. The drop-in code creates a stub of the application which is a dynamic proxy backed by a cluster of replicated objects with automatic failover. Next we need to setup the server cluster. 

The server code is as follows:  

```Scala
      // the client app K-V store
      val dataFile = new java.io.File(folder.getCanonicalPath + "/kvstore")
      println(s"node kv data store is ${dataFile.getCanonicalPath}")
      val db: DB = DBMaker.newFileDB(dataFile).make
      val clientApp = new MapDBConsistentKVStore(db)
      val logFile = new java.io.File(folder.getCanonicalPath + "/paxos")
      println(s"paxos data log is ${logFile.getCanonicalPath}")
      val journal = new FileJournal(logFile, cluster.retained)
      // the node unique id in the paxos closter which is passed into main
      val node = nodeMap.get(nodeId).get
      // actor system with the node config
      val system =
        ActorSystem(cluster.name, ConfigFactory.load("server.conf").withValue("akka.remote.netty.tcp.port",ConfigValueFactory.fromAnyRef(node.clientPort) ))
      // generic entry point accepts TypedActor MethodCall messages and reflectively invokes them on our client app
      system.actorOf(Props(classOf[TypedActorPaxosEndpoint], cluster, PaxosActor.Configuration(config, cluster.nodes.size), node.id, journal, clientApp, "TrexServer"))
```

There is quite a bit going on there due to the separation of concerns in the code. The application object `val clientApp = new MapDBConsistentKVStore(db))` has its own concerns and has no dependencies on TRex. The journal object encapsulates the concern of journaling the progress of the consensus algorithm. As noted above an application which is not crash recovery replay safe would need to provide a custom journal that participates in application transactions. The `TypedActorPaxosEndpoint` is our Trex Server which runs the consensus algorithm.

The most interesting actor for the purpose of a demo of integrating TRex into an existing application is the `TypedActorPaxosEndpoint`. This runs the consensus algorithm over values sent from the TRex driver by clients. In this case the values are `MethodCall` messages sent from the `TypedActor` at the client. This endpoint reflectively invokes the select commands on its local copy of the replicated application object. The consensus logic is in a superclass so you can write a custom endpoint actor by overriding the `deliver` method. You can then compile the deliver logic directly against your own application code and message formats. 

Now we have both client and server its worth describing the full data flow created by the demo code. The client code invokes the interface methods on the `TypedActor`. The `TypedActor` forwards a `MethodCall` message to the TRex driver. The driver has Akka serialise the message and forwards it to the distinguished leader over TCP. At the other end the distinguished leader runs the multi-Paxos consensus algorithm over UDP. The chosen messages are invoked on all the replicated objects in consensus order. The method return value at the distinguished leader is sent back to the client TRex driver. It responds to the TypedActor which competes the method call for the client code. 

We can now start up the three server process and multiple clients. We can run commands though the clients and kill the leader node to see a failover. If we restart the failed node it will automatically sync up with the new leader. Not bad for a few lines of custom code and a bit of configuration. 
