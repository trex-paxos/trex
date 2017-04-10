package com.github.trex_paxos.javademo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class StackClient {



    static StringStack clusteredStack(final String configName, final String hostname) {
//        Config systemConfig = ConfigFactory.loadForHighestEra(configName).withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(hostname));
//        Config config = ConfigFactory.loadForHighestEra(configName);
//        Cluster cluster = Cluster.parseConfig(config);
//        ActorSystem system = ActorSystem.create("trex-java-demo", systemConfig);
//        ActorRef driver = system.actorOf(Props.create(DynamicClusterDriver.class, akka.util.Timeout.apply(100), 20));
//        DynamicClusterDriver.Initialize init = DynamicClusterDriver$.MODULE$.apply(cluster);
//        driver.tell(init, null);
//        StringStack stack = TypedActor.get(system).typedActorOf(new TypedProps<StringStackImpl>(StringStack.class, StringStackImpl.class), driver);
//        return stack;
        return null;
    }

    public static void usage(int returned) {
        System.err.println("usage    : StringStack local|clustered [config] [hostname]");
        System.err.println("example 1: StringStack local");
        System.err.println("example 2: StringStack clustered client3.conf 127.0.0.1");
        System.exit(returned);
    }

    public static void main(String[] args) throws IOException {

        if (args.length == 0) {
            usage(1);
        }

        StringStack stack;

        if (args[0].startsWith("local")) {
            System.out.println("using local stack");
            stack = new StringStackImpl();
        } else if (args[0].startsWith("clustered")) {
            if (args.length != 3) {
                usage(2);
            }
            System.out.println("using clustered stack");
            stack = clusteredStack(args[1], args[2]);
        } else {
            stack = null;
            System.err.println("neither local or clustered: " + args[0]);
            System.exit(2);
        }

        try {
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(System.in));

            String input;

            while ((input = br.readLine()) != null) {
                if (input.startsWith("push ")) {
                    stack.push(input.substring("push ".length()));
                } else if (input.startsWith("pop")) {
                    if (stack.empty()) {
                        System.err.println("empty");
                    } else {
                        System.out.println(stack.pop());
                    }

                } else if (input.startsWith("peek")) {
                    if (stack.empty()) {
                        System.err.println("empty");
                    } else {
                        System.out.println(stack.peek());
                    }

                }
                else {
                    System.err.println("not implemented: " + input);
                }
            }

        } catch (IOException io) {
            // ignore
        }
    }
}
