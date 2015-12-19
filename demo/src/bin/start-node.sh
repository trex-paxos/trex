CONFIG=$1
echo "CONFIG=$CONFIG"
NODE=$2
echo "NODE=$NODE"
NODE_NUM=$(echo $NODE | sed 's/n//1' )
echo "$NODE_NUM=NODE_NUM"
mkdir -p /tmp/$NODE_NUM
java -cp ./demo/src/main/resources:./demo/target/scala-2.11/trex-demo-assembly-0.1.jar com.github.trex_paxos.demo.TrexKVStore $CONFIG $NODE_NUM | tee /tmp/trex.$$

