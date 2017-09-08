package in.dream_lab.goffish.godb;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HueristicsLoad {

	 private static Hueristics hueristics = null;
	 private static final Object Lock = new Object();
	 public static Hueristics getInstance(){
	   //add callback later....broadcast to all threads
	   synchronized(Lock){
		 if(hueristics!=null){
		   return hueristics;
		 }
		 else{
		   
			 try{/*Directly reading the gathered heuristics*/
				 //TODO: might have to change this when subgraphs run in parallel in a partition(Locking)
//					FileInputStream fis = new FileInputStream(ConfigFile.basePath +"Hue_FULL.ser"); 
//					ObjectInputStream ois = new ObjectInputStream(fis);
//					hueristics = (Hueristics)ois.readObject();
//					ois.close();
					
					// Creating FileSystem object, to be able to work with HDFS
			                 //hdfs://10.0.0.4:29000/
			                 //hdfs://orion-00:29000/
	     			Configuration config = new Configuration();
	     			config.set("fs.default.name","hdfs://10.0.0.4:29000");
	     			FileSystem dfs = FileSystem.get(config);
	     			FSDataInputStream in = null;
	     			//Exact statistics:/Stats/hue_FULL.ser
	     			//Approximate statistics://Stats/hue_FULLHist.ser
	     			in = dfs.open(new Path("/Stats/hue_FULL.ser"));
//	     			byte[] b=null;
//	     			in.readFully(b);	
	     			ObjectInputStream ois= new ObjectInputStream(in);
					hueristics=(Hueristics)ois.readObject();
					System.out.println("Hueristics Loaded");
					
				}catch(Exception e){e.printStackTrace();}
			 
			 
			 return hueristics;
		   }
		 }
		 
		
		 
	 }
	
}
