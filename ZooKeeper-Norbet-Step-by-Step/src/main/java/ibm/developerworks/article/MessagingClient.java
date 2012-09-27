package ibm.developerworks.article;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.NettyNetworkClient;
import com.linkedin.norbert.javacompat.network.NetworkClientConfig;
import com.linkedin.norbert.javacompat.network.RoundRobinLoadBalancerFactory;

/**
 * Wraps the Netty based norbert client for this server (one per JVM)
 * 
 * @author Mukul Gupta
 * @author Paresh Paladiya
 * 
 */
public class MessagingClient
{

   private static Logger logr = Logger.getLogger(MessagingClient.class);

   /*
    * The wrapped Netty messaging client instance
    */
   private static NettyNetworkClient nettyClient;

   private static final CommonSerializer serializer = new CommonSerializer();

   private static AtomicBoolean isClusterTopologyStable = new AtomicBoolean(true);

   /*
    * Tracks current set of active server nodes
    */
   private static Set<Node> CURRENT_ACTIVE_NODES = null;

   /*
    * need a map of current nodes since they might be accessed a lot. Represents the same
    * set as <code>CURRENT_NODES</code>
    */
   private static ConcurrentHashMap<Integer, Node> CURR_NODE_MAP = new ConcurrentHashMap<Integer, Node>(
         5);

   public static void init()
   {

      logr.info("Initializing ...");
      NetworkClientConfig config = new NetworkClientConfig();

      // [a] need instance of local norbert based zookeeper cluster client
      config.setClusterClient(MessagingServer.getZooKeeperClusterClient());

      nettyClient = new NettyNetworkClient(config, new RoundRobinLoadBalancerFactory());

      logr.debug("Netty client started");
   }

   /**
    * Called from cluster listener. Sets the current node set as well as make the same
    * available for quick lookup in the corresponding map
    * 
    * @param foCurrentNodes
    */
   public static void updateCurrentNodeSet(Set<Node> currentNodes)
   {
      isClusterTopologyStable.set(false);

      CURRENT_ACTIVE_NODES = currentNodes;

      // will put in new ones node list
      CURR_NODE_MAP.clear();
      logr.debug("New Node list size:" + currentNodes.size());

      if (CURRENT_ACTIVE_NODES != null)
      {
         for (Iterator<Node> iterator = currentNodes.iterator(); iterator.hasNext();)
         {
            Node node = (Node) iterator.next();
            CURR_NODE_MAP.put(node.getId(), node);
            logr.debug("Node active:" + node.getId() + " URL " + node.getUrl());

         }
      }

      // signal cluster topology registered
      isClusterTopologyStable.set(true);

      logr.debug("This server is leader in server cluster:" + Server.isLeader());

   }

   /**
    * Sends passed message to the specified server; if server id is 0 then send to a
    * server in the cluster using assigned load balancing strategy (default=round robin
    * selection)
    * 
    * @param messg
    *           the message to be sent
    * @param serverId
    *           Id of the server to which the message should be sent
    * @return
    * @throws Exception
    *            If destination server is not available
    * 
    */
   public static Future<String> sendMessage(AppRequestMsg messg, int serverId)
         throws Exception
   {

      // if the server cluster topology is changing (rare) - wait!
      while (!isClusterTopologyStable.get())
      {
         Thread.currentThread().sleep(40000);
         logr.info("Messaging thread waiting for current cluster topology to stabilize");
      }

      // load balance message to cluster- strategy registered with client
      if (serverId <= 0)
      {
         logr.debug("Sending message (id=):" + messg.getId() + " from server[:"
               + Server.getId() + "]  using round robin strategy");
         
         return nettyClient.sendRequest(messg, serializer);

      }
      else
      {
         // message to be sent to particular server node
         Node destNode = CURR_NODE_MAP.get(serverId);
         if (destNode != null)
         {
            logr.debug("Sending message:" + messg.getId() + " from server[:"
                  + Server.getId() + "]  using round robin strategy");
            return nettyClient.sendRequestToNode(messg, destNode, serializer);

         }

         throw new Exception("Could not send message." + " Destination server[id]="
               + serverId + " is not avaialble...");
      }

   }
}
