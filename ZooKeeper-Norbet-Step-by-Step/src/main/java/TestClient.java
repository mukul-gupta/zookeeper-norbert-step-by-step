

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**
 * Generates configured number of files to test cluster
 * task distribution.
 * 
 * @author Mukul Gupta
 * @author Paresh Paladiya
 * 
 */
public class TestClient
{
   //private Logger logr = Logger.getLogger(TestClient.class);

   private int fileCount = 100;
 
   public void createDirectory(String inputDir)
   {
      File directory = new File(inputDir);
      if (directory.exists())
      {
         directory.delete();
      }
      directory.mkdir();
   }

   public void testFileProcessing(String inputDir) throws Exception
   {
      System.out.println("Populating directory with files");
      
      for (int i = 0; i < fileCount; i++)
      {
         System.out.println("Writing file " + inputDir + "/file_" + i + ".txt");
         File file = new File(inputDir + "/file_" + i + ".txt");
         BufferedWriter out = new BufferedWriter(new FileWriter(file));
         out.write("hello " + i);
         out.close();

         Thread.sleep(4000);
      }
      System.out.println("Populated directory with files");

   }

   public static void main(String args[]) throws Exception
   {
      if (args == null || args.length == 0)
      {
         System.out
               .println("Please enter input directory name where files will be generated...");
         System.out.println("Usage :-  java TestClient C:/demo/in_events");
         return;
      }

      TestClient loClient = new TestClient();

      loClient.createDirectory(args[0]);
      loClient.testFileProcessing(args[0]);
   }

}
