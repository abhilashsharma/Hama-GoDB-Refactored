package in.dream_lab.goffish.godb.bfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;

import com.sun.tools.javac.util.List;

import in.dream_lab.goffish.hama.GraphJob;

import in.dream_lab.goffish.hama.LongMapPartitionSubsetSuccinctGsonReader;
import in.dream_lab.goffish.hama.NonSplitTextInputFormat;

public class DoBFSSubgraphSuccinctJob {

	 public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException, ParseException
	  {
		  HamaConfiguration conf = new HamaConfiguration();
		  GraphJob job = new GraphJob(conf, DoBFSSubgraphSuccinct.class);
		  job.setJobName("BFS with revisits Succinct");
		  job.setInputFormat(TextInputFormat.class);
		  job.setInputKeyClass(LongWritable.class);
		  job.setInputValueClass(LongWritable.class);
		  job.setOutputFormat(TextOutputFormat.class);
		  job.setOutputKeyClass(LongWritable.class);
		  job.setOutputValueClass(LongWritable.class);
		  job.setMaxIteration(20);
		  job.setGraphMessageClass(BFSMessage.class);
		  job.setInputPath(new Path(args[0]));
		  job.setOutputPath(new Path(args[1]));
		  job.setInitialInput(readArgsFromFile(args[2]));
		  job.setSubgraphValueClass(BFSState.class);
		  /* Reader configuration */
		  job.setInputFormat(NonSplitTextInputFormat.class);
		  job.setInputReaderClass(LongMapPartitionSubsetSuccinctGsonReader.class);
		  
		  //job.setSubgraphComputeClass(SubgraphComputeReduce.class);
		  job.waitForCompletion(true);
	  }
	 
	 
	 static String  readArgsFromFile(String fileName) throws IOException{
	   String Args="";
//	   String fileName="/home/abhilash/abhilash/multipleArguments.txt";
	   FileReader fr = new FileReader(fileName);
           BufferedReader br = new BufferedReader(fr);

           String sCurrentLine;

           

           while ((sCurrentLine = br.readLine()) != null) {
               if(Args.equals(""))
                   Args="-i "+sCurrentLine;
               else
                   Args=Args+";-i " + sCurrentLine;
               
           }
           
           br.close();
	   return Args;
	 }
	
}
