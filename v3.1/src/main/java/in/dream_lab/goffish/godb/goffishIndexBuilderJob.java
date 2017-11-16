package in.dream_lab.goffish.godb;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;

import in.dream_lab.goffish.hama.GraphJob;
import in.dream_lab.goffish.hama.LongMapPartitionSubsetGsonReader;
import in.dream_lab.goffish.hama.LongMapPartitionSubsetGsonReaderSynthLongRemoteLong;
import in.dream_lab.goffish.hama.LongMapPartitionSubsetGsonReaderSynthlong;
import in.dream_lab.goffish.hama.NonSplitTextInputFormat;

public class goffishIndexBuilderJob {
  public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException, ParseException
  {
          HamaConfiguration conf = new HamaConfiguration();
          GraphJob job = new GraphJob(conf, goffishIndexBuilder.class);
          job.setJobName("Path");
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
          job.setInitialInput(args[2]);
          job.setSubgraphValueClass(pathDistrSubgraphState.class);
          job.setInputFormat(NonSplitTextInputFormat.class);
          job.setInputReaderClass(LongMapPartitionSubsetGsonReaderSynthLongRemoteLong.class);//LongMapPartitionSubsetGsonReader
          
          //job.setSubgraphComputeClass(SubgraphComputeReduce.class);
          job.waitForCompletion(true);
  }
}
