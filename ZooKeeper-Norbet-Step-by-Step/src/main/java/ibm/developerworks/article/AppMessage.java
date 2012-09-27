package ibm.developerworks.article;

/**
 * Contract for all message text exchanges for this application. UTF-8 charset used for
 * all operations.
 * 
 * @author Mukul Gupta
 * @author Paresh Paladiya
 * 
 */
public interface AppMessage
{
	/**
	 * Returns the unique Id for the message
	 */
	public String getId();

	/**
	 * Returns payload text
	 * 
	 * @return
	 */
	public String getPayload();

	/**
	 * Returns the serialized object
	 * 
	 * @return
	 */
	public byte[] serialize();

	/**
	 * Returns the object from 
	 * @return
	 */
	public AppMessage returnObject(byte[] obj);

}
