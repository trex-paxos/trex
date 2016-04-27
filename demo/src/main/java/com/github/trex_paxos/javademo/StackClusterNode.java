package com.github.trex_paxos.javademo;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.github.trex_paxos.Cluster;
import com.github.trex_paxos.Node;
//import com.github.trex_paxos.TrexStaticMembershipServer;
import com.github.trex_paxos.TrexStaticMembershipServer;
import com.github.trex_paxos.TrexStaticMembershipServer$;
import com.github.trex_paxos.internals.FileJournal;
import com.github.trex_paxos.internals.PaxosActor;
import com.github.trex_paxos.library.Journal;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import java.io.File;
import java.io.IOException;

public class StackClusterNode {

    static void usage(int returned) {
        System.out.println("usage:   StackClusterNode config nodeId");
        System.out.println("example: StackClusterNode cluster3.conf 2552");
        System.exit(returned);
    }

    public static void main(String[] args) throws IOException {
        if( args.length != 2) {
            usage(1);
        }

        final String configName = args[0];
        final Integer nodeId = Integer.valueOf(args[1]);

        final StringStack stack = new StringStackImpl(new File(System.getProperty("java.io.tmpdir")+"/stack"+nodeId.toString()));

        Config config = ConfigFactory.load(configName);
        Cluster cluster = Cluster.parseConfig(config);

        Node node = cluster.nodeMap().get(nodeId).get();
        File folder = new File(cluster.folder() + "/" + nodeId);
        if (!folder.exists() || !folder.canRead() || !folder.canWrite() ) {
            System.err.println(folder.getCanonicalPath() + " does not exist or do not have permission to read and write. Exiting.");
            System.exit(-1);
        }
        Journal journal = new FileJournal(new File(folder, "journal"), cluster.retained());
        Config systemConfig = ConfigFactory.load(configName)
                .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(node.clientPort()))
                .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(node.host()));

        final ActorSystem system = ActorSystem.create(cluster.name(), systemConfig);

        PaxosActor.Configuration conf = new PaxosActor.Configuration(config);

        system.actorOf(TrexStaticMembershipServer$.MODULE$.apply(cluster, conf, nodeId, journal, stack));
    }
}
