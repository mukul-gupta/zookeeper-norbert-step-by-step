package ibm.developerworks.article;

import java.io.File;
import java.text.DateFormat;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.network.RequestHandler;

/**
 * Invoked asynchronously by the Norbert library when a new message is received on this
 * server. The received message can be used to trigger further processing assuming the
 * message wraps data and command instructions from the task distributor. For the
 * simulated problem / solution, the payload data is simply written out to the output
 * directory configured.
 * 
 * 
 * @author Mukul Gupta
 * @author paresh.paladiya
 * 
 */
public class AppMessageHandler implements RequestHandler<AppRequestMsg, String>
{

   private static Logger logr = Logger.getLogger(AppMessageHandler.class);

   public String handleRequest(AppRequestMsg requestObj) throws Exception
   {
      // message sent by local or remote task distributor (one per cluster)
      String message = requestObj.getPayload();
      logr.trace("Received message payload:" + message);

      String[] parts = StringUtils.split(message, "|~");

      // src file name parsed out
      String srcfileName = parts[0];
      logr.trace("Received message:src filename" + srcfileName);
      String actualPayload = message.substring(srcfileName.length() + 2);

      // output file name - cxreate a unique file name that has original file name part
      // also
      String outFileName = Server.getUniqMessgId() + "_" + srcfileName;

      // output - with processing header info
      StringBuilder outPayload = new StringBuilder(message.length() + 64);
      outPayload.append("Processed by server:" + Server.getId() + " at:"
            + DateFormat.getDateTimeInstance().format((new Date())));
      outPayload.append(System.getProperty("line.separator")).append(
            System.getProperty("line.separator"));
      outPayload.append(actualPayload);

      // write to output file
      FileUtils.writeStringToFile(new File(Server.getOutputEventsDir(), outFileName),
            outPayload.toString());

      logr.info("Received message:" + requestObj.toString() + " on Server:"
            + Server.getId());

      // sending simple acknowledgement back to src
      return "ack_from_server_" + Server.getId() + "_for_message_" + requestObj.getId();
   }

}
