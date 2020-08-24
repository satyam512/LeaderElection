import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher { //LeaderElection is an event handler class as "This(Watcher) interface specifies the public interface an event handler class must implement"

    /* A ZooKeeper client(zooKeeper) will get various events from the ZooKeepr server it connects to.
     An application using such a client handles these events by registering a callback object with the client. (line 24)
     The callback object is expected to be an instance of a class that implements Watcher interface.
     */

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodeName;

    private ZooKeeper zooKeeper; // zookeeper client API , this client talks to configured zookeeper server 

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectZooKeeper();

        leaderElection.volunteerForLeadership();
        leaderElection.reElection();
        leaderElection.run();
        leaderElection.close();

        System.out.println("Disconnected from ZooKeeper server, exiting application");
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();  // timeout in case server is down // that dude was talking about something like hard timout thingy
        }
    }
    public void close() throws InterruptedException {
        zooKeeper.close();
    }
    public void connectZooKeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte [] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        /*
            Ephemeral node if gets disconnected and timout occurs, it would get deleted; sequential is telling number
            to be appended at the name is generated sequentially by parent
         */
        System.out.println("Full path znodeName = " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE+"/" , "");
    }

    public void reElection() throws KeeperException, InterruptedException {
        String predecessorZnodeName = null;
        Stat predecessorStat = null;

        while(predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);

            String smallestChild = children.get(0);
            if(smallestChild.equals(this.currentZnodeName)) {
                System.out.println("I'm the leader ");
                return;
            }
            System.out.println("I'm not the leader, " + smallestChild + " is the leader");
            int predecessorIndex = Collections.binarySearch(children, this.currentZnodeName) - 1;
            predecessorZnodeName = children.get(predecessorIndex);
            predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE+"/"+predecessorZnodeName, this);
        }
        System.out.println("Watching Znode " + predecessorZnodeName);
    }
    public void process(WatchedEvent watchedEvent) {
        //System.out.println(watchedEvent.getType());
        switch (watchedEvent.getType()) {
            case None:
                if(watchedEvent.getState()== Event.KeeperState.SyncConnected) {
                    System.out.println("Synchronized and connected to Zookeeper server");
                }
                else{ // if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
                    synchronized (zooKeeper) { // some other thread might be using zookeeper client API somewhere
                        zooKeeper.notifyAll();  // on disconnection it will wake up all threads sleeping on zookeeper client object
                    }
                }
            case NodeDeleted:
                try {
                    this.reElection();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }
}
