import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.List;

public class WatcherDemo implements Watcher { //LeaderElection is an event handler class as "This(Watcher) interface specifies the public interface an event handler class must implement"

    /* A ZooKeeper client(zooKeeper) will get various events from the ZooKeepr server it connects to.
     An application using such a client handles these events by registering a callback object with the client. (line 24)
     The callback object is expected to be an instance of a class that implements Watcher interface.
     */

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String TARGET_ZNODE = "/target_node";

    private ZooKeeper zooKeeper; // zookeeper client API , this client talks to configured zookeeper server

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        WatcherDemo watcherDemo = new WatcherDemo();
        watcherDemo.connectZooKeeper();
        watcherDemo.watchTargetZnode();
        watcherDemo.run();
        watcherDemo.close();

        System.out.println("Disconnected from ZooKeeper server, exiting application");
    }

    public void watchTargetZnode() throws KeeperException, InterruptedException {
        // registering a watcher for creation or deletion of znode
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
        if(stat==null) { return; }
        // registering a watcher for data changes in znode
        byte [] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        // registering a watcher for data changes in znode
        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Stats : "+stat + " data : " + new String(data) + " Children : " + children);
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
                break;
            case NodeCreated:
                System.out.println("Node created");
                break;
            case NodeDeleted:
                System.out.println("Node deleted");
                break;
            case NodeDataChanged:
                System.out.println("Data in node changed");
                break;
            case NodeChildrenChanged:
                System.out.println("Children changed");
                break;
        }

        try {
            this.watchTargetZnode();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
