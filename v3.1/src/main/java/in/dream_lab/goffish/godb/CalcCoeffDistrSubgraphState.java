package in.dream_lab.goffish.godb;

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

import in.dream_lab.goffish.godb.CalcCoeffDistr.ResultSet;
import in.dream_lab.goffish.godb.CalcCoeffDistr.Step;
import in.dream_lab.goffish.godb.CalcCoeffDistr.VertexMessageSteps;

public class CalcCoeffDistrSubgraphState implements Writable  {

	PrintWriter writer;

	Integer searchInstanceStart = null;

	Integer searchInstanceEnd = null;

	//private ISubgraphInstance subgraphInstance = null;
	ArrayList<Step> path = null;
	
	LinkedList<VertexMessageSteps> forwardRemoteVertexList = new LinkedList<VertexMessageSteps>();
	//private LinkedList<VertexMessageSteps> revRemoteVertexList = new LinkedList<VertexMessageSteps>();

	HashMap<Long,ResultSet> resultsMap = new HashMap<Long,ResultSet>();
		
	LinkedList<VertexMessageSteps> forwardLocalVertexList;
	//private LinkedList<VertexMessageSteps> revLocalVertexList;
	//private HashMap<Long,HashMap<String,LinkedList<Long>>> inVerticesMap;
	HashMap<Long,Long> remoteSubgraphMap; 

	Integer noOfSteps = null;
	
	String heuristicsBasePath = ConfigFile.basePath+"heuristics/hue_";
	
	final Base64 base64 = new Base64();
	
	int startPos  = 0;
	
	double networkCoeff = 0.116;

	//for BFS
	
	int Depth;
	List<Long> visitedVertices=new ArrayList<Long>();

	
	Long TotalTime=0l;
	 
	
	String Arguments=null;
	private Double[] queryCostHolder;
	
	long resultCollectionTime=0;
	
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
