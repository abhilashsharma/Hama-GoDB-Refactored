package in.dream_lab.goffish.godb;

import in.dream_lab.goffish.hama.GraphJob;
import in.dream_lab.goffish.hama.LongMapPartitionSuccinctReader12Implicit;
import in.dream_lab.goffish.hama.LongMapPartitionSuccinctReaderIn;
import in.dream_lab.goffish.hama.NonSplitTextInputFormat;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class reachabilityDistrSuccinctJob {

	 public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException, ParseException
	  {
		  HamaConfiguration conf = new HamaConfiguration();
		  GraphJob job = new GraphJob(conf, reachabilityDistrSuccinct.class);
		  job.setJobName("Reachability Succinct");
		  job.setInputFormat(TextInputFormat.class);
		  job.setInputKeyClass(LongWritable.class);
		  job.setInputValueClass(LongWritable.class);
		  job.setOutputFormat(TextOutputFormat.class);
		  job.setOutputKeyClass(LongWritable.class);
		  job.setOutputValueClass(LongWritable.class);
		  job.setMaxIteration(20);
		  job.setGraphMessageClass(Text.class);
		  job.setInputPath(new Path(args[0]));
		  job.setOutputPath(new Path(args[1]));
		  job.setInitialInput(readArgsFromFile(args[2]));
		  job.setSubgraphValueClass(reachabilityDistrSubgraphSuccinctState.class);
		  
		  job.setInputFormat(NonSplitTextInputFormat.class);
		    job.setInputReaderClass(LongMapPartitionSuccinctReaderIn.class);
		  //job.setSubgraphComputeClass(SubgraphComputeReduce.class);
		  job.waitForCompletion(true);
	  }
	 
	 static String  readArgsFromFile(String fileName) throws IOException{
         String Args="";
//         String fileName="/home/abhilash/abhilash/pathArguments.txt";
         FileReader fr = new FileReader(fileName);
         BufferedReader br = new BufferedReader(fr);

         String sCurrentLine;

         

         while ((sCurrentLine = br.readLine()) != null) {
             if(Args.equals(""))
                 Args=sCurrentLine;
             else
                 Args=Args+";" + sCurrentLine;
             
         }
         
         br.close();
         return Args;
       }	
}
