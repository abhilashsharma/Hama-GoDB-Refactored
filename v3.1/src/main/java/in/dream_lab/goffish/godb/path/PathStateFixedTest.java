package in.dream_lab.goffish.godb.path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.godb.ConfigFile;
import in.dream_lab.goffish.godb.EdgeAttr;
import in.dream_lab.goffish.godb.ResultSet;
import in.dream_lab.goffish.godb.SinkData;
import in.dream_lab.goffish.godb.Step;
import in.dream_lab.goffish.godb.path.DoTestFixedStart.OutputPathKey;
import in.dream_lab.goffish.godb.path.DoTestFixedStart.Pair;
import in.dream_lab.goffish.godb.path.DoTestFixedStart.RecursivePathKey;
import in.dream_lab.goffish.godb.path.DoTestFixedStart.RecursivePathMaintained;
import in.dream_lab.goffish.godb.path.TraversalStep.TraversalWithState;

public class PathStateFixedTest implements Writable {

	
	// Variables and DataStructures
	
		 PrintWriter writer;


		
		String Arguments=null;
		//made STATIC so that smallest subgraphID will read the whole heuristic(graph statistics)
		
		
		//logging result collection time
		long resultCollectionTime=0;
		
		//storing query cost for each query plan
		Double[] queryCostHolder=null;
		
		
		ArrayList<Step> path = null;
		
		LinkedList<TraversalWithState> forwardRemoteVertexList = new LinkedList<TraversalWithState>();
		LinkedList<TraversalWithState> revRemoteVertexList = new LinkedList<TraversalWithState>();

		HashMap<Long,ResultSet> resultsMap = new HashMap<Long,ResultSet>();
			
		LinkedList<TraversalWithState> forwardLocalVertexList;
		LinkedList<TraversalWithState> revLocalVertexList;

		Integer noOfSteps = null;
		
		//TODO: change to relative path
		String heuristicsBasePath = ConfigFile.basePath+"heuristics/hue_";
		
		final Base64 base64 = new Base64();
		
		int startPos  = 0;
		
		Double networkCoeff = new Double(0.116);


		//Data Structure for storing inedges 
                HashMap<Long,ArrayList<SinkData>>  InEdges = null;
                HashMap<Long,Long> remoteSourceToSubgraphMapping=null;
                
		/**
		 * HashMap for recursive path Maintenance
		 * Used when getting a remote message from  another subgraph say 'S', to record information about endVertex of 'S' so that if any
		 * output is created then the partial path generated in current subgraph has to be sent back to 'S' for merging.   
		 */
		HashMap<OutputPathKey,List<Pair>> outputPathMaintainance = new HashMap<OutputPathKey,List<Pair>>();
		
		/**
		 * For storing partial paths, <queryID,step,direction,endVertex> where endVertex is the vertex in the path that has remote edges.
		 * 	Used when sending messages to another subgraph for path traversal. It stores the partial path generated in this data structure and sends
		 *  message to remote subgraph to continue traversal.
		 */
		
		HashMap<RecursivePathKey, List<RecursivePathMaintained>> recursivePaths = new HashMap<RecursivePathKey, List<RecursivePathMaintained>>();
	
		/**
		 *  Used while creating Inedges.
		 */
		HashMap<Long,StringBuilder> MessagePacking=new HashMap<Long,StringBuilder>();
		
		//For Storing partial results
		HashMap<RecursivePathKey,List<String>> partialResultCache=new HashMap<RecursivePathKey,List<String>>();

		
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	
	
}
