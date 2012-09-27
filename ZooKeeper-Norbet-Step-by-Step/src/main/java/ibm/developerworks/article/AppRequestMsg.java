package ibm.developerworks.article;

import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Request message for wrapping EAI event payloads for the application
 * 
 * @author Mukul Gupta
 * @author paresh paladiya
 */
public class AppRequestMsg implements AppMessage
{

   /*
    * message id
    */
   private String id = null;

   /*
    * message payload
    */
   private String payload = null;

   /*
    * Character set used for serialization and deserialization
    */
   private static final Charset charset = Charset.forName("UTF-8");

   /*
    * delimter character used for serialization and deserialization
    */
   private static final char delim = '|';

   private static Logger logr = Logger.getLogger(AppRequestMsg.class);

   /**
    * Returns message with Id automatically generated
    * 
    * @param payload
    *           the message payload
    */
   public AppRequestMsg(String payload)
   {
      id = Server.getUniqMessgId();
      this.payload = payload;

      if (payload == null)
      {
         this.payload = "";
      }

   }

   public AppRequestMsg()
   {

   }

   public String getId()
   {
      return id;
   }

   public String getPayload()
   {
      return payload;
   }

   /**
    * Serializes this object
    */
   public byte[] serialize()
   {
      StringBuilder serObj = new StringBuilder(payload.length() + 64);
      serObj.append(id).append(delim).append(payload);

      return serObj.toString().getBytes(charset);
   }

   public AppMessage returnObject(byte[] obj)
   {
      return deserialize(obj);
   }

   public static AppRequestMsg deserialize(byte[] obj)
   {
      // this is the serialized string using our defined characterset
      String objectAsString = new String(obj, charset);
      String[] parts = StringUtils.split(objectAsString, delim);

      logr.trace("deserializing message:payload=" + objectAsString);

      AppRequestMsg returnObj = new AppRequestMsg();
      returnObj.setId(parts[0]);
      returnObj.setPayload(objectAsString.substring(parts[0].length() + 1));
      
      logr.trace("deserializing message:Id=" + returnObj.getId());
      logr.trace("deserializing message:payload="
            + returnObj.getPayload());

      return returnObj;
   }

   /**
    * Sets the Id for this message
    * 
    * @param id
    *           the message id
    */
   public void setId(String id)
   {
      this.id = id;
   }

   /**
    * Sets the payload for this message
    * 
    * @param payload
    *           the message payload
    */
   public void setPayload(String payload)
   {
      this.payload = payload;
   }

   @Override
   public String toString()
   {
      return "id:" + id + " payload:" + payload;
   }

}
