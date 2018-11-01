package in.dream_lab.goffish.godb;

import in.dream_lab.goffish.godb.reachabilityDistrSuccinctSplitProperty.VertexMessageSteps;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

public class reachabilityDistrSubgraphSuccinctSplitPropertyState implements Writable {

	String Arguments=null;
	
	public PrintWriter writer;

	public long resultCollectionTime=0;
	//HACK: made static so that only one subgraph can load the global statistic object
	public static Hueristics hueristics = new Hueristics(); 
	
	public Double[] queryCostHolder;
	
	public ArrayList<Step> path = null;
	public Step startVertex = null;
	public Step endVertex = null;
	
	public LinkedList<VertexMessageSteps> forwardRemoteVertexList = new LinkedList<VertexMessageSteps>();
	public LinkedList<VertexMessageSteps> revRemoteVertexList = new LinkedList<VertexMessageSteps>();
	
	public LinkedList<VertexMessageSteps> forwardLocalVertexList;
	public LinkedList<VertexMessageSteps> revLocalVertexList;
	
	
	//public HashMap<Long,HashMap<String,LinkedList<Long>>> inVerticesMap;
	
	public ResultSet resultsSet = new ResultSet();
	
	public Integer noOfSteps = null;
	
	public String heuristicsBasePath = ConfigFile.basePath+"heuristics/hue_";
	
	public final Base64 base64 = new Base64();
	
	public Double networkCoeff = new Double(0.116);

	boolean stopProcessing = false;
	int stopTraversalLength = 0;

	
	


	//Data Structure for storing inedges 
	HashMap<Long,HashMap<Long,EdgeAttr>>  InEdges = null;
	
	HashMap<Long,StringBuilder> MessagePacking=new HashMap<Long,StringBuilder>();

	HashSet<Long> visitedSet=new HashSet<Long>();
	
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
