package ibm.developerworks.article;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import com.google.common.io.Files;

/**
 * Wraps a zookeeper server construct for the solution
 * 
 * @author Mukul Gupta
 * @author Paresh Paladiya
 */
public class ZooKeeperServer implements Runnable
{

   /**
    * The zooKeeper server config
    */
   private QuorumPeerConfig serverConfig = null;

   /**
    * The zookeeper server (eventually part of a server 'quorum' that must be realized for
    * any service to stand up). All updates to zookeeper must be 'approved' by a quorum of
    * servers within the originally defined group.
    */
   private QuorumPeer server = null;

   private static Logger logr = Logger.getLogger(ZooKeeperServer.class);

   /**
    * Constructor.
    * 
    * @param configFile
    *           config file for the zookeeper server (server instance)
    * @throws ConfigException
    * @throws IOException
    */
   public ZooKeeperServer(File configFile) throws ConfigException, IOException
   {
      serverConfig = new QuorumPeerConfig();
      logr.debug("zookeeper config file: "
            + Files.simplifyPath(configFile.getCanonicalPath()));
      serverConfig.parse(configFile.getCanonicalPath());
   }

   public void run()
   {
      NIOServerCnxn.Factory cnxnFactory;
      try
      {
         // supports client connections
         cnxnFactory = new NIOServerCnxn.Factory(serverConfig.getClientPortAddress(),
               serverConfig.getMaxClientCnxns());

         server = new QuorumPeer();

         // most properties defaulted from QuorumPeerConfig; can be overridden
         // by specifying in the zookeeper config file

         server.setClientPortAddress(serverConfig.getClientPortAddress());

         server.setTxnFactory(new FileTxnSnapLog(new File(serverConfig.getDataDir()),
               new File(serverConfig.getDataLogDir())));

         server.setQuorumPeers(serverConfig.getServers());
         server.setElectionType(serverConfig.getElectionAlg());
         server.setMyid(serverConfig.getServerId());
         server.setTickTime(serverConfig.getTickTime());
         server.setMinSessionTimeout(serverConfig.getMinSessionTimeout());
         server.setMaxSessionTimeout(serverConfig.getMaxSessionTimeout());
         server.setInitLimit(serverConfig.getInitLimit());
         server.setSyncLimit(serverConfig.getSyncLimit());
         server.setQuorumVerifier(serverConfig.getQuorumVerifier());
         server.setCnxnFactory(cnxnFactory);
         server.setZKDatabase(new ZKDatabase(server.getTxnFactory()));
         server.setLearnerType(serverConfig.getPeerType());

         server.start();

         // wait for server thread to die
         server.join();
      }
      catch (IOException e)
      {
         server = null;
         logr.error("Exception encountered starting server ...", e);
      }
      catch (InterruptedException e)
      {
         server = null;
         logr.error("Server thread interrupted ...", e);
      }

   }

   /**
    * Returns if the server is running or not
    * 
    * @return
    */
   public boolean isRunning()
   {
      if (server != null)
      {
         return server.isRunning();
      }

      return false;

   }

   /**
    * Returns the server config object
    * 
    * @return
    */
   public QuorumPeerConfig getServerConfig()
   {
      return serverConfig;
   }

   /**
    * Returns the wrapped zookeeper server instance
    * 
    * @return
    */
   public QuorumPeer getServer()
   {
      return server;
   }

   /**
    * Returns if this server is the leader process within the server group / cluster
    * 
    * @return
    */
   public boolean isLeader()
   {
      if (server != null)
      {
         return (server.leader != null);
      }

      return false;
   }

}
