package ibm.developerworks.article;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.Assert;

/**
 * This is singleton server class. This class must be initialized before starting any
 * remote communication.
 * 
 * @author Mukul Gupta
 * @author Paresh Paladiya
 * 
 */
public class Server
{
   private static Logger logr = Logger.getLogger(Server.class);

   /**
    * Identifies this Server
    */
   private static int serverId = -1;

   /**
    * The directory that simulates the input events data store.
    */
   private static File inputEventsDir = null;

   /**
    * The directory where events /messages are written as output. Simulates EAI event
    * processing workflow output app adapter
    */
   private static File outputEventsDir = null;

   /**
    * Wraps the zookeeper server instance
    */
   private static ZooKeeperServer server = null;

   private static File zooKeeperConfigFile = null;

   private static int nettyServerPort = -1;



   /*
    * Will be used to generate unique messge ids unique message id :
    * <SERVER_ID>_<SERVER_STRT_TS>_<MESSG_CTR>
    */
   private static long SERVER_STRT_TS = System.currentTimeMillis();

   /*
    * Will be used to generate unique messge ids unique message id :
    * <SERVER_ID>_<SERVER_STRT_TS>_<MESSG_CTR>
    */
   private static AtomicLong MESSG_CTR = new AtomicLong(1);

   /*
    * File poller (simulated application inbound event adapter). Will only be started on
    * the leader server.
    */
   private static ClassPathXmlApplicationContext filePollerFlow = null;

   /**
    * Locate the bootstrap properties file for the server and reads in initialization
    * parameters
    * 
    * @param zooKeeperFile
    *           the bootstrap properties file
    * @throws Exception
    */
   private static void bootstrapServer(File serverPropFile) throws Exception
   {

      // using commons configuration for reading bootstrap file
      Configuration config = null;
      try
      {
         logr.debug("Bootstrap properties file:" + serverPropFile.getCanonicalPath());

         config = new PropertiesConfiguration(serverPropFile);

         // initialize the server id
         serverId = config.getInt("server.id");
         logr.debug("starting server[id=" + serverId + "]...");

         // Get the zookeeper server config file location
         zooKeeperConfigFile = new File(
               serverPropFile.getParentFile().getCanonicalFile(),
               config.getString("zookeeperConfigFile"));

         Assert.isTrue(zooKeeperConfigFile.isFile() && zooKeeperConfigFile.exists(),
               "Require a valid zooKeeper server file");

         logr.debug("zookeeper config file:" + zooKeeperConfigFile.getCanonicalPath());

         // need to create data and log dirs if they do not exist; also deposit apposite
         // myid file with server id - required by default config parser included with the
         // zookeeper distribution
         preprocessZooKeeperSetup(zooKeeperConfigFile);

         // get the location of common input event store
         inputEventsDir = new File(config.getString("inputEventsDir"));
         // create dir if it does not exist
         if (!inputEventsDir.exists())
            inputEventsDir.mkdirs();

         Assert.isTrue(inputEventsDir.isDirectory() && inputEventsDir.exists(),
               "Require a valid input events folder");
         // write path to system.properties
         System.setProperty("input.dir", inputEventsDir.getCanonicalPath());

         logr.debug("input events fir:" + inputEventsDir.getCanonicalPath());

         // get the location of output event folder for this server
         outputEventsDir = new File(config.getString("outputEventsDir"));
         // create output directory if it does not exist
         if (!outputEventsDir.exists())
            outputEventsDir.mkdirs();
         Assert.isTrue(outputEventsDir.isDirectory() && outputEventsDir.exists(),
               "Require a valid input events folder");

         // get listener port for netty server that will be activated within this process
         nettyServerPort = config.getInt("messageServerPort");
         // add port valid rules if any

      }
      catch (Exception e)
      {
         logr.error("Could not initialize the server...", e);
      }

   }

   /**
    * If this server is the leader process in the zookeeper cluster
    * 
    * @return
    */
   public static boolean isLeader()
   {
      if (server != null)
      {
         return server.isLeader();
      }

      return false;
   }

   /**
    * Starts the underlying zookeeper server instance
    * 
    * @return
    * @throws ConfigException
    * @throws IOException
    */
   private static boolean startZooKeeperServer() throws ConfigException, IOException
   {
      server = new ZooKeeperServer(zooKeeperConfigFile);
      Executors.newSingleThreadExecutor().execute(server);

      // server start submitted to be started asynchronously for code / maint clarity
      // will need to wait (can be parameterized) in current thread for server to start
      int totalWaitTime = 30000;// milliseconds total wait
      int currWait = 0; // current wait in milliseconds
      int sleepInt = 5000;

      while (!server.isRunning())
      {
         currWait += sleepInt;
         if (currWait > totalWaitTime)
         {
            logr.error("The zookeeper server could not be started in the assigned "
                  + totalWaitTime + " milliseconds");
            return false;
         }

         try
         {
            
            Thread.currentThread().sleep(sleepInt);
            
         }
         catch (InterruptedException e)
         {
            logr.error("Could not start zookeeper server", e);
            return false;
         }
      }

      return true;

   }

   /**
    * Returns the server id assigned to this server
    * 
    * @return
    */
   public static int getId()
   {
      return serverId;
   }

   /**
    * Returns the port value for the Netty server using for intra-cluster messaging
    * 
    * @return
    */
   public static int getNettyServerPort()
   {
      return nettyServerPort;
   }

   /**
    * Starts the Spring integration file poller component that simulates application data
    * store access (inbound app adapter)
    */
   protected static void startFilePoller()
   {
      filePollerFlow = new ClassPathXmlApplicationContext(
            new String[] { "FilePoller_spring.xml" });
      filePollerFlow.start();
      logr.info("Started polling for events...");

   }

   /**
    * Creates a simple unique message identifier for all messages that are sent from this
    * server.
    * 
    * @return
    */
   public static String getUniqMessgId()
   {
      StringBuilder uniqId = new StringBuilder(64);
      uniqId.append(serverId).append("_").append(SERVER_STRT_TS).append("_")
            .append(MESSG_CTR.getAndIncrement());

      return uniqId.toString();
   }

   public static boolean isFilePollerRunning()
   {
      return (filePollerFlow != null);
   }

   private static void preprocessZooKeeperSetup(File zooKeeperConfigFile)
         throws ConfigurationException, IOException
   {
      Configuration config = new PropertiesConfiguration(zooKeeperConfigFile);

      // create datadir if it does not exist
      String dataDirPath = config.getString("dataDir");
      File dataDir = new File(dataDirPath);
      if (!dataDir.exists())
         dataDir.mkdirs();

      // clean data dir - group management relies only on ephemeral znodes - no other
      // application data being persisted
      FileUtils.cleanDirectory(dataDir);

      // reposit new file called myid with this server's id - default zookeeper distro
      // class QuorumPeerConfig uses that - reusing it to parse full zookeeper config
      FileUtils.writeStringToFile(new File(dataDir, "myid"),
            String.valueOf(Server.getId()));

      // create and clean log dir
      String dataLogDirPath = config.getString("dataLogDir");
      File dataLogDir = new File(dataLogDirPath);
      if (!dataLogDir.exists())
         dataLogDir.mkdirs();

      // clean data dir - group management relies only on ephemeral znodes - no other
      // application data being persisted
      FileUtils.cleanDirectory(dataLogDir);

   }
   
   /**
    * Returns the output events directory for this server
    * @return
    */
   public static File getOutputEventsDir()
   {
      return outputEventsDir;
   }

   public static void main(String[] args) throws Exception
   {
      Assert.isTrue(args != null && args.length >= 1,
            "Require a valid server properties bootstrap file argument");

      File serverPropFile = new File(args[0]);
      Assert.isTrue(serverPropFile.isFile() && serverPropFile.exists(),
            "Require a valid server properties bootstrap file...");

      // bootstrap server properties and invariants
      bootstrapServer(serverPropFile);

      // try and start local / in process zookeeper server
      if (!startZooKeeperServer())
      {
         // log and return
         logr.error("Could not start zooKeeper server.  Check logs.");
         return;
      }
      else
      {
         logr.debug("Zookeeper server instance started...");
      }

      // Start Netty based messaging server (Norbert)
      MessagingServer.init(server.getServerConfig());

      // initialize the Norbert / Netty client for this JVM
      MessagingClient.init();

   }



}
