package ibm.developerworks.article;

import java.nio.charset.Charset;

import com.linkedin.norbert.network.Serializer;

/**
 * Message serializer.
 * 
 * @author Mukul Gupta
 * @author Paresh Paladiya
 */
public class CommonSerializer implements Serializer<AppRequestMsg, String>
{

	public AppRequestMsg requestFromBytes(byte[] obj)
   {
	   return AppRequestMsg.deserialize(obj);
   }

	public String requestName()
   {
	   return "Application Request";
   }

	public String responseFromBytes(byte[] obj)
   {
	   return new String(obj, Charset.forName("UTF-8"));
   }

	public byte[] requestToBytes(AppRequestMsg obj)
   {
	   return obj.serialize();
   }

	public String responseName()
   {
	   return "Application Response";
   }

	public byte[] responseToBytes(String obj)
   {
	   // TODO Auto-generated method stub
	   return obj.getBytes(Charset.forName("UTF-8"));
   }

	

	

}
