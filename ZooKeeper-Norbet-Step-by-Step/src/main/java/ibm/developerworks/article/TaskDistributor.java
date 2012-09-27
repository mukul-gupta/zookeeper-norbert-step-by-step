package ibm.developerworks.article;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.integration.Message;

import com.google.common.io.Files;

/**
 * Models the task distributor  
 * 
 * @author Mukul Gupta
 * @author paresh.paladiya
 *
 */
public class TaskDistributor
{
	private static final Charset charset = Charset.forName("UTF-8");
	private static Logger logr = Logger.getLogger(TaskDistributor.class);
	
   public void processMessage(Message<File> polledMessg)
   {
      File polledFile = polledMessg.getPayload();
      
      String payload = null;
      String ack = null;
      try
      {
      	logr.info("Received file as input:" + polledFile.getCanonicalPath());
      	
      	//prefix file name and a delimiter for the rest of the payload
	      payload = polledFile.getName() + "|~" + Files.toString(polledFile, charset);
	      
	      logr.trace("sending message: payload:" + payload);
	      //create new message
	      AppRequestMsg newMessg = new AppRequestMsg(payload);
	      
	      try
	      {
   	      //loadbalance the request to operating servers without
   	      //targetting any one in particular
   	      Future<String> retAck = MessagingClient.sendMessage(newMessg, -1);
   	      
   	      //block for acknowledgement - could have processed acknowledgement
   	      //
   	      ack = retAck.get();
	      }
	      catch(Exception ex)
	      {
	         //Cluster topology not synchronized yet so lets wait till configured time for 
	         //zookeeperclusterclient session timeout
	         Thread.sleep(20000);
	         logr.trace("Retrying sending message: payload:" + payload);
	         Future<String> retAck = MessagingClient.sendMessage(newMessg, -1);
            
            //block for acknowledgement - could have processed acknowledgement
            //
            ack = retAck.get();
	      }
	      FileUtils.deleteQuietly(polledFile);
	      logr.info("sent message and received acknowledgement:" + ack);
      }
      catch (IOException e)
      {
         logr.error("Error handling message in task distributor", e);
	      
      }
      catch (Exception e)
      {
         logr.error("Error handling message in task distributor", e);
      }
      

   }
}
