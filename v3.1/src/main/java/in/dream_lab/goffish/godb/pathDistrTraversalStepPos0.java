package in.dream_lab.goffish.godb;



import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraphWrapup;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.Step.Direction;
import in.dream_lab.goffish.godb.Step.Type;
import in.dream_lab.goffish.godb.pathDistrTraversalStepPos0.VertexMessageSteps;



public class pathDistrTraversalStepPos0 extends
AbstractSubgraphComputation<pathDistrTraversalStepPos0SubgraphState, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable> 
implements ISubgraphWrapup{
	
	public pathDistrTraversalStepPos0(String initMsg) {
		// TODO Auto-generated constructor stub
		Arguments=initMsg;
	}
	
	public static final Log LOG = LogFactory.getLog(pathDistrTraversalStepPos0.class);
	
	int fixedPos;
	String Arguments=null;
	//Required for lucene 
	static File vertexIndexDir;
	static Directory vertexDirectory;
	static Analyzer analyzer;
	static IndexReader indexReader;
	static IndexSearcher indexSearcher;
	static BooleanQuery query;
	static ScoreDoc[] hits;
	static boolean initDone = false ;
	static boolean queryMade = false;
	private static final Object initLock = new Object();
	private static final Object queryLock = new Object();
	private static boolean queryStart=false;//later lock this when multithreaded
        private static boolean queryEnd=false;//later lock this when multithreaded
        private static boolean gcCalled=false;

	static Hueristics hueristics = new Hueristics(); 
	


	/**
	 * Representative class to keep tab of next vertex to be processed, different for path query
	 */
	public class VertexMessageSteps{
		Long queryId;
		Long vertexId;
		String message;
		Integer stepsTraversed;
		Long previousSubgraphId;
		Integer previousPartitionId;
		Long startVertexId;
		Integer startStep;
		VertexMessageSteps(Long _queryId,Long _vertexId,String _message,Integer _stepsTraversed,Long _startVertexId,Integer _startStep,Long _previousSubgraphId, Integer _previousPartitionId){
			this.queryId=_queryId;
			this.vertexId = _vertexId;
			this.message = _message;
			this.stepsTraversed = _stepsTraversed;
			this.previousSubgraphId = _previousSubgraphId;
			this.startVertexId = _startVertexId;
			this.startStep=_startStep;
			this.previousPartitionId = _previousPartitionId;
		}
	}
	
	//this is used for storing output messages, output messages are partial messages that are sent to parent subgraph for recursive aggregation
	public class OutputMessageSteps{
		LongWritable targetSubgraphId;
		Text message;
		public OutputMessageSteps(Text _message, LongWritable _SubgraphId) {
			// TODO Auto-generated constructor stub
			this.message = _message;
			this.targetSubgraphId = _SubgraphId;
		}
	}
	
	

		
	// Recursive Output COllection data structures	
	// TODO: Populate the hash appropriately
	long time;
	class Pair{
		Long endVertex;
        Long prevSubgraphId;
        Integer prevPartitionId;    
        public Pair(Long _endVertex,Long _prevSubgraphId, Integer _prevPartitionId) {
        	this.endVertex=_endVertex;
            this.prevSubgraphId = _prevSubgraphId;
            this.prevPartitionId = _prevPartitionId;
        }
    }
		
		class RecursivePathMaintained{
			Long startVertex;
			Integer startStep;
			String path;
			int direction=0;
			
			public int hashCode(){
				return (int)(startStep+direction+startVertex + path.hashCode());
			}
			
			public boolean equals(Object obj){
				RecursivePathMaintained other=(RecursivePathMaintained)obj;
				return (this.direction==other.direction && this.startStep.intValue()==other.startStep.intValue() && this.startVertex.longValue()==other.startVertex.longValue() && this.path.equals(other.path));
			}
			
			
			public RecursivePathMaintained(Long _startVertex,Integer _startStep, String _path, int _direction){
				this.startVertex=_startVertex;
				this.startStep=_startStep;
				this.path=_path;
				this.direction = _direction;
			}
		}
		
		
	
		/**
		 * This is custom key used for storing partial paths.(Could be merged with OutputPathKey as only interpretation of keys are different)
		 * 
		 */
		 class RecursivePathKey {
	         long queryID;
	         int step;
	         boolean direction;
	         long endVertex;
	        
	         public int hashCode() {
	             return (int) (queryID + step + endVertex);          
	         }
	        
	         public boolean equals(Object obj) {
	        	RecursivePathKey other=(RecursivePathKey)obj;
	            return (this.queryID==other.queryID && this.step==other.step && this.direction==other.direction && this.endVertex==other.endVertex);
	         }
	         
	        public RecursivePathKey(long _queryID,int _step,boolean _direction,long _endVertex){
	        	this.queryID=_queryID;
	        	this.step=_step;
	        	this.direction=_direction;
	        	this.endVertex=_endVertex;	        	
	        }
	    }
		 
		/**
		 * This is custom key used for storing information required to perform recursive merging of result. 
		 * 
		 *
		 */
		class OutputPathKey{
			long queryID;
			int step;
			boolean direction;
			long startVertex;
			
			public int hashCode() {
				return (int) (queryID + step + startVertex);
			}
			
			public boolean equals(Object obj){
				OutputPathKey other=(OutputPathKey)obj;
				return (this.queryID==other.queryID && this.step==other.step && this.direction==other.direction && this.startVertex==other.startVertex);
			}
			
			public OutputPathKey(long _queryID,int _step,boolean _direction,long _startVertex){
				this.queryID=_queryID;
				this.step=_step;
				this.direction=_direction;
				this.startVertex=_startVertex;
				
			}
			
		}
	
	
	
		
	/**
	 * Initialize the class variables
	 * This method is called in first superstep, it parses the query passed.
	 * It also reads the Graph statistics(Called as Heuristics) from disk
	 */
	private void init(Iterable<IMessage<LongWritable, Text>> messageList){
		String arguments = Arguments;
		getSubgraph().getSubgraphValue().Arguments=Arguments;
	  
	
		fixedPos=Integer.parseInt(arguments.split(Pattern.quote("//"))[1]);
	    getSubgraph().getSubgraphValue().path = new ArrayList<Step>();
		Type previousStepType = Type.EDGE;
		for(String _string : arguments.split(Pattern.quote("//"))[0].split(Pattern.quote("@")) ){
			if(_string.contains("?")){
				if(previousStepType == Type.EDGE)
					getSubgraph().getSubgraphValue().path.add(new Step(Type.VERTEX,null, null, null));
				previousStepType = Type.EDGE;
				String[] _contents = _string.split(Pattern.quote("?")); 
				String p = null ;
				Object v = null ;
				Direction d = (_contents[0].equals("out") ) ? Direction.OUT : Direction.IN;
				if ( _contents.length > 1 )	{
					p = _contents[1].split(Pattern.quote(":"))[0];
					String typeAndValue = _contents[1].split(Pattern.quote(":"))[1];
					String type = typeAndValue.substring(0, typeAndValue.indexOf("["));
					if(type.equals("float")) {
						v = Float.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );
					}
					else if(type.equals("double")) { 
						v = Double.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

					}
					else if(type.equals("int")) { 
						v = Integer.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

					}
					else { 
						v = String.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

					}
				}
				getSubgraph().getSubgraphValue().path.add(new Step(Type.EDGE, d, p, v));
			}
			else{
				previousStepType = Type.VERTEX;
				String p = _string.split(Pattern.quote(":"))[0];
				String typeAndValue = _string.split(Pattern.quote(":"))[1];
				Object v = null;
				String type = typeAndValue.substring(0, typeAndValue.indexOf("["));
				if(type.equals("float")) {
					v = Float.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );
				}
				else if(type.equals("double")) { 
					v = Double.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

				}
				else if(type.equals("int")) { 
					v = Integer.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

				}
				else { 
					v = String.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

				}

				getSubgraph().getSubgraphValue().path.add(new Step(Type.VERTEX,null, p, v));
			}
			

		}
		if(previousStepType == Type.EDGE){
			getSubgraph().getSubgraphValue().path.add(new Step(Type.VERTEX,null, null, null));
		}
		getSubgraph().getSubgraphValue().noOfSteps = getSubgraph().getSubgraphValue().path.size();
		getSubgraph().getSubgraphValue().queryCostHolder = new Double[getSubgraph().getSubgraphValue().noOfSteps];
		for (int i = 0; i < getSubgraph().getSubgraphValue().queryCostHolder.length; i++) {
			getSubgraph().getSubgraphValue().queryCostHolder[i] = new Double(0);
			
		}
		getSubgraph().getSubgraphValue().forwardLocalVertexList = new LinkedList<VertexMessageSteps>();
		getSubgraph().getSubgraphValue().revLocalVertexList = new LinkedList<VertexMessageSteps>();
//		inVerticesMap = new HashMap<Long, HashMap<String,LinkedList<Long>>>();
//		remoteSubgraphMap = new HashMap<Long, Long>();
//		hueristics=HueristicsLoad.getInstance();//loading this at a different place

		
	}

	
	
	
	
	
	/**
	 * Initialize lucene
	 * 
	 */
	private void initLucene() throws InterruptedException, IOException{
		
		{
		  long pseudoPid=getSubgraph().getSubgraphId().get() >> 32;
			initDone = true;
			vertexIndexDir = new File(ConfigFile.basePath+ "/index/Partition"+pseudoPid+"/vertexIndex");
			vertexDirectory = FSDirectory.open(vertexIndexDir);
			analyzer = new StandardAnalyzer(Version.LATEST);
			indexReader  = DirectoryReader.open(vertexDirectory);
			indexSearcher = new IndexSearcher(indexReader);
		}
		
	}
	
	/**
	 * Initialize Lucene in memory
	 * searcher = new IndexSearcher (new RAMDirectory (indexDirectory)); 
	 */
	private void initInMemoryLucene() throws InterruptedException, IOException{
	     {
               long pseudoPid=getSubgraph().getSubgraphId().get() >> 32;
                     initDone = true;
                     vertexIndexDir = new File(ConfigFile.basePath+ "/index/Partition"+pseudoPid+"/vertexIndex");
                     vertexDirectory = FSDirectory.open(vertexIndexDir);
                     analyzer = new StandardAnalyzer(Version.LATEST);
                     indexReader  = DirectoryReader.open(new RAMDirectory(vertexDirectory, IOContext.READ));//passing RAM directory to load indexes in memory
                     indexSearcher = new IndexSearcher(indexReader);
             }
	  
	  
	}
	

	/**
	 * 
	 * uses index to query vertices/Edges that contain a particular property with specified value
	 */
	private void makeQuery(String prop,Object val) throws IOException{
		{
			queryMade = true;
			if(val.getClass()==String.class){
			query  = new BooleanQuery();
			query.add(new TermQuery(new Term(prop, (String)val)), BooleanClause.Occur.MUST);
			hits =  indexSearcher.search(query,40000).scoreDocs;
			}
			else if(val.getClass()==Integer.class)
			{
				Query q = NumericRangeQuery.newIntRange(prop,(Integer)val, (Integer)val, true, true);
				hits =  indexSearcher.search(q,40000).scoreDocs;
			}
			
		}
	}
	
	


	
	/**
	 * When getting a message that contains partial output, this method merges the partial output from the message and partial path stored currently. 
	 * The Merged output may be partial output that needs to be sent back further to its parent subgraph, that is checked by checking  'outputPathMaintainance' if there is an entry.
	 */
	private void join(IMessage<LongWritable, Text> _message) {
		
		long Time=System.currentTimeMillis();
		String message = _message.getMessage().toString();
//		System.out.println("RECEIVED JOIN MESSAGE:" +message);
		String[] split = message.split(Pattern.quote(";"));
		boolean direction =  split[1].equals("for()") ? true:false;
		Long endVertexId = Long.parseLong(split[2]);
		Long queryId= Long.parseLong(split[5]);
		Integer step=Integer.parseInt(split[6]);
		//Recently added line...Reminder
		step=direction?step-1:step+1;
		for (RecursivePathMaintained stuff : getSubgraph().getSubgraphValue().recursivePaths.get(new RecursivePathKey(queryId, step, direction,endVertexId))){
			StringBuilder result = new StringBuilder(split[4]);//partial result
			//*****Adding partial Result to partialResultCache********
			
//			if(!partialResultCache.containsKey(new RecursivePathKey(queryId, step, direction,endVertexId))){
//				List<String> pathList=new ArrayList<String>();
//				pathList.add(split[4]);
//				partialResultCache.put(new RecursivePathKey(queryId, step, direction,endVertexId),pathList);
//			}
//			else{
//				partialResultCache.get(new RecursivePathKey(queryId, step, direction,endVertexId)).add(split[4]);
//			}
			
			//***************************END***************************
			
			if (direction)
				result.insert(0, stuff.path);
			else
				result.append(stuff.path);
			
//			System.out.println("END:" + endVertexId + "PATH:"+stuff.path + "Merged path:" + result);
			Integer recursiveStartStep=stuff.startStep;
			boolean recursion=false;
//			System.out.println("JoinQuery:" + queryId+","+ recursiveStartStep+","+ direction+","+ stuff.startVertex);
			
			if ( getSubgraph().getSubgraphValue().outputPathMaintainance.containsKey(new OutputPathKey(queryId, recursiveStartStep, direction, stuff.startVertex)))
			{
				
				for ( Pair entry: getSubgraph().getSubgraphValue().outputPathMaintainance.get(new OutputPathKey(queryId,recursiveStartStep,direction,stuff.startVertex))){
					StringBuilder remoteMessage = new StringBuilder("output();");
					if(direction)
						remoteMessage.append("for();");
					else
						remoteMessage.append("rev();");
					remoteMessage.append(entry.endVertex.toString()).append(";");
					remoteMessage.append(entry.prevSubgraphId.toString()).append(";");
					remoteMessage.append(result).append(";").append(queryId).append(";").append(recursiveStartStep);
					Text remoteM = new Text(remoteMessage.toString());
					
//					System.out.println("Sending JOIN Message:" + remoteMessage.toString());
					getSubgraph().getSubgraphValue().outputList.add(new OutputMessageSteps(remoteM, new LongWritable(entry.prevSubgraphId)));
				}
				recursion=true;
			}
						
			if(!recursion){
				if ( !getSubgraph().getSubgraphValue().resultsMap.containsKey(stuff.startVertex) )
					getSubgraph().getSubgraphValue().resultsMap.put(stuff.startVertex, new ResultSet());
				if ( direction ) 
					getSubgraph().getSubgraphValue().resultsMap.get(stuff.startVertex).forwardResultSet.add(result.toString());
				else
					getSubgraph().getSubgraphValue().resultsMap.get(stuff.startVertex).revResultSet.add(result.toString());
			}
			
		}
						
		Time=System.currentTimeMillis()-Time;
		getSubgraph().getSubgraphValue().resultCollectionTime+=Time;
						
	}
	
	
	
	
	
	
	
	/**
	 * When there is match for a path and parent subgraph is not the current subgraph then this method is used to send partial results back to parent subgraph. 
	 * 
	 */
	private void forwardOutputToSubgraph(int direction,VertexMessageSteps step) {
		boolean d=false;
		if(direction==1){
			d=true;
		}
		long Time=System.currentTimeMillis();
		String dir="for()";
		if(direction==0)
			dir="rev()";
//		System.out.println("OUTPUT SIZE:"+step.startVertexId+":"+step.message+":"+getSubgraph().getSubgraphValue().outputPathMaintainance.get(new OutputPathKey(step.queryId,step.startStep,d,step.startVertexId)).size());
		
		for (Pair entry: getSubgraph().getSubgraphValue().outputPathMaintainance.get(new OutputPathKey(step.queryId,step.startStep,d,step.startVertexId) )){
			StringBuilder remoteMessage = new StringBuilder("output();"+dir+";");
			remoteMessage.append(entry.endVertex.toString()).append(";");
			remoteMessage.append(entry.prevSubgraphId.toString()).append(";");
			remoteMessage.append(step.message).append(";").append(step.queryId).append(";").append(step.startStep).append(";");
			Text remoteM = new Text(remoteMessage.toString());
			//remoteM.setTargetSubgraph(step.previousPartitionId);
			getSubgraph().getSubgraphValue().outputList.add(new OutputMessageSteps(remoteM, new LongWritable(entry.prevSubgraphId)));
//			System.out.println("SENDING OUTPUT MESSAGE:" + new String(remoteM.toString()));
		}
		
		Time=System.currentTimeMillis()-Time;
		getSubgraph().getSubgraphValue().resultCollectionTime+=Time;
		
	}

	
	
	
	
	
	/**
	 * SUPERSTEP 0:parse the query and initialize lucene
	 * SUPERSTEP 1 and 2: populating InEdges
	 * SUPERSTEP 3: Find execution plan and start execution
	 * ....
	 * REDUCE: write results
	 */
	
	
	@Override
	public void compute(Iterable<IMessage<LongWritable, Text>> messageList) {
		
		LOG.info("Compute Starts");
		
//		System.out.println("**********SUPERSTEPS***********:" + getSuperstep() +"Message List Size:" + messageList.size());
		
		
		// STATIC ONE TIME PROCESSES
		{
			// LOAD QUERY AND INITIALIZE LUCENE
			if(getSuperstep() == 0){
	
	
				if( Arguments==null ){
					System.out.println("START_ERROR:NO ARGUMENTS PROVIDED\tEXPECTED ARGUMENTS FORMAT\tvertexFilter@edgeDirection?edgeFilter@edgeDirection?edgeFilter@...|vertexFilter|edgeDirection?edgeFilter|...//instanceNumber\n");
					voteToHalt();
				}
				else
				{	
					init(messageList);
					// TODO: uncomment after indexing
					try{
						synchronized (initLock) {
							if ( !initDone )
							      initInMemoryLucene();
						}
					}catch(Exception e){e.printStackTrace();}
					
				}
			}
			
	
			else if (getSuperstep()==1) {
				// GATHER HEURISTICS FROM OTHER SUBGRAPHS
				

			//INEDGES COMMENTED	
//				if(getSubgraph().getSubgraphValue().InEdges==null){
//                                //Logic to Accumulate inedges
//				  getSubgraph().getSubgraphValue().InEdges=new HashMap<Long,HashMap<Long,EdgeAttr>>();  
//				time=System.currentTimeMillis();
//			
//				
//				String m="";
//				
//				for(IVertex<MapValue, MapValue, LongWritable, LongWritable> sourceVertex:getSubgraph().getLocalVertices())
//		        	for(IEdge<MapValue, LongWritable, LongWritable> edge : sourceVertex.getOutEdges()) {
//		        		
//		        		IVertex<MapValue, MapValue, LongWritable, LongWritable> sinkVertex=getSubgraph().getVertexById(edge.getSinkVertexId());
//	        		//if sink vertex is not remote then add inedge to appropriate data structure, otherwise send source value to remote partition
//	        		if(!sinkVertex.isRemote())
//	        		{
//	        			if(getSubgraph().getSubgraphValue().InEdges.containsKey(sinkVertex.getVertexId().get()))
//	        			{
//	        			   	if(!getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).containsKey(sourceVertex.getVertexId().get()))
//	        			   	{
//	        			   		
//	        			   		
////	        			   		ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
//	        			   		EdgeAttr attr= new EdgeAttr("relation","null" /*subgraphProperties.getValue("relation").toString()*/,edge.getEdgeId().get(),false,null);
//	        			   		getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).put(sourceVertex.getVertexId().get(), attr);
//	        			   		//System.out.println("Accumulation inedge for edge "+ edge.getId() + " Value " + subgraphProperties.getValue("relation").toString() );
//	        			   	}
//	        				
//	        			}
//	        			else
//	        			{
////	        				ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
//	    			   		EdgeAttr attr= new EdgeAttr("relation", "null"/*subgraphProperties.getValue("relation").toString()*/,edge.getEdgeId().get(),false,null);      				
//	    			   		getSubgraph().getSubgraphValue().InEdges.put(sinkVertex.getVertexId().get(), new HashMap<Long,EdgeAttr>());
//	    			   		getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).put(sourceVertex.getVertexId().get(), attr);
//	    			   		//System.out.println("Accumulation inedge for edge "+ edge.getId() + " Value " + subgraphProperties.getValue("relation").toString() );
//	    			   		
//	        			}
//	        			
//	        			//System.out.println(edge.getSource().getId() + " -->" + edge.getSink().getId());
//	        		}
//	        		else
//	        		{ //send message to remote partition
//	        		
//	        		//TODO: generalize this for all attributes
//	        			IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)sinkVertex;
//	        			remoteVertex.getSubgraphId().get();
//	        		if(!getSubgraph().getSubgraphValue().MessagePacking.containsKey(remoteVertex.getSubgraphId().get()))
//	        			getSubgraph().getSubgraphValue().MessagePacking.put(remoteVertex.getSubgraphId().get(),new StringBuilder("#|" + sourceVertex.getVertexId().get()  + "|" + sinkVertex.getVertexId().get() + "|" + "relation" + ":"  +"null" /*subgraphProperties.getValue("relation").toString()*/+"|" + edge.getEdgeId().get()+"|" + getSubgraph().getSubgraphId().get() + "|" +0));
//	        		else{
//	        			getSubgraph().getSubgraphValue().MessagePacking.get(remoteVertex.getSubgraphId().get()).append("$").append("#|").append(sourceVertex.getVertexId().get()).
//	        								append("|").append(sinkVertex.getVertexId().get()).
//	        								append("|").append("relation").append(":").append("null" /*subgraphProperties.getValue("relation").toString()*/).
//	        								append("|").append(edge.getEdgeId().get()).
//	        								append("|").append(getSubgraph().getSubgraphId().get()).
//	        								append("|").append(0);
//	        			
//	        		}
//	        		
//	        		
//	        		
//	        		}
//	        		
//		        	
//	        		
//	        		
//	        		
//	            }
//	        	
//	        	//Sending packed messages by iterating through MessagePacking Hashmap
//	        	for(Map.Entry<Long,StringBuilder> remoteSubgraphMessage: getSubgraph().getSubgraphValue().MessagePacking.entrySet()){
//	        		Text msg = new Text(remoteSubgraphMessage.getValue().toString());
//	                
//	                sendMessage(new LongWritable(remoteSubgraphMessage.getKey()),msg);
//	        	}
//	        	
//		}	
//				
			}
	
			//subgraphId/20:attr?21,12,23|attr?12,12
			else if ( getSuperstep()==2 ) {
			//TODO :remove this hack
			for (IMessage<LongWritable, Text> _message: messageList){
				
				String message = _message.getMessage().toString();

				
				String[] SubgraphMessages=message.split(Pattern.quote("$"));
				for(String subgraphMessage:SubgraphMessages){
					String[] values = subgraphMessage.split(Pattern.quote("|"));
		        	 long Source=Long.parseLong(values[1]);
		        	 long Sink=Long.parseLong(values[2]);
		        	 String[] attr_data=values[3].split(":");
		        	 if(getSubgraph().getSubgraphValue().InEdges.containsKey(Sink))
		        	  {
		        		 EdgeAttr attr= new EdgeAttr(attr_data[0],attr_data[1],Long.parseLong(values[4]),true,Long.parseLong(values[5]));      				
		        		 getSubgraph().getSubgraphValue().InEdges.get(Sink).put(Source, attr);
		        	  }
		        	 else
		        	  {
		        		 EdgeAttr attr= new EdgeAttr(attr_data[0],attr_data[1],Long.parseLong(values[4]),true,Long.parseLong(values[5]));   
		        		 getSubgraph().getSubgraphValue().InEdges.put(Sink, new HashMap<Long,EdgeAttr>());
		        		 getSubgraph().getSubgraphValue().InEdges.get(Sink).put(Source, attr);
		        	  }
				}
					
				
				
					
			}
			if(!gcCalled){
			System.gc();
			System.runFinalization();
			}
			LOG.info("TIME ACCUMULATING INEDGES....Starting loading of heuristics");
//			hueristics=HueristicsLoad.getInstance();
			LOG.info("Heuristic Loaded");

			if(!gcCalled){
	                        System.gc();
	                        System.runFinalization();
	                        gcCalled=true;
	                }
			
		}
		}
		
		// RUNTIME FUNCTIONALITITES 
		{
	
			// COMPUTE-LOAD-INIT
			if(getSuperstep()==3){
			        if(!queryStart){
			        queryStart=true;  
				LOG.info("Starting Query Execution");
				 queryEnd=false;
			        }
				// COMPUTE HUERISTIC BASED QUERY COST
//				{
//					// TODO: implementation for calc cost from middle of query ( for each position calc cost forward and backward cost and add them)
//					
//					for (int pos = 0;pos < getSubgraph().getSubgraphValue().path.size() ; pos+=2 ){
//						Double joinCost = new Double(0);
//						//forward cost
//						{	
//							Double totalCost = new Double(0);
//							Double prevScanCost = hueristics.numVertices;
//							Double resultSetNumber = hueristics.numVertices;
//							ListIterator<Step> It = getSubgraph().getSubgraphValue().path.listIterator(pos);
//							//Iterator<Step> It = path.iterator();
//							Step currentStep = It.next();
//							
//							while(It.hasNext()){
//								//cost calc
//								// TODO: make cost not count in probability when no predicate on edge/vertex
//								{
//									Double probability = null;
//									
//									if ( currentStep.property == null )
//										probability = new Double(1);
//									else {
//										if ( hueristics.vertexPredicateMap.get(currentStep.property).containsKey(currentStep.value.toString()) ){
//											probability = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).probability;
//											//System.out.println("Vertex Probability:" + probability);
//										}	
//										else {
//											totalCost = new Double(-1);
//											break;
//										}
//									}
//									resultSetNumber *= probability;
//									Double avgDeg = new Double(0);
//									Double avgRemoteDeg = new Double(0);
//									Step nextStep = It.next();
//									if(nextStep.direction == Direction.OUT){
//										if ( currentStep.property == null) {
//											avgDeg = hueristics.numEdges/hueristics.numVertices;
//											avgRemoteDeg = hueristics.numRemoteVertices/(hueristics.numVertices+hueristics.numRemoteVertices) * avgDeg;
//											//System.out.println("AVGDEG:" +avgDeg + "REMOTEAVGDEG:" + avgRemoteDeg);
//										}	
//										else { 
//											avgDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgOutDegree; 
//											avgRemoteDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgRemoteOutDegree;
//											//System.out.println("AVGDEG:" +avgDeg + "REMOTEAVGDEG:" + avgRemoteDeg);
//										}	
//									}else if(nextStep.direction == Direction.IN){
//										if ( currentStep.property == null) {
//											avgDeg = hueristics.numEdges/hueristics.numVertices;
//											avgRemoteDeg = hueristics.numRemoteVertices/(hueristics.numVertices+hueristics.numRemoteVertices) * avgDeg;
//											//System.out.println("AVGDEG:" +avgDeg + "REMOTEAVGDEG:" + avgRemoteDeg);
//										}	
//										else { 
//											avgDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgInDegree;
//											avgRemoteDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgRemoteInDegree;
//											//System.out.println("AVGDEG:" +avgDeg + "REMOTEAVGDEG:" + avgRemoteDeg);
//										}		
//									}
//									resultSetNumber *= (avgDeg+avgRemoteDeg); 
//									Double eScanCost = prevScanCost * probability * avgDeg;
//									Double networkCost = new Double(0);
//									Double vScanCost = new Double(0);
//									if(nextStep.property == null)
//										vScanCost = eScanCost;
//									else {
//										//output(partition.getId(), subgraph.getId(),nextStep.property);
//										//output(partition.getId(), subgraph.getId(),nextStep.value.toString());
//										//output(partition.getId(), subgraph.getId(),String.valueOf(hueristics.edgePredicateMap.size()));
//										//output(partition.getId(), subgraph.getId(),String.valueOf(pos));
//										//output(partition.getId(), subgraph.getId(),hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability.toString());
//										//System.out.println(nextStep.property+":"+nextStep.value);
//										if ( hueristics.edgePredicateMap.get(nextStep.property).containsKey(nextStep.value.toString()) ) {
//											vScanCost = eScanCost * hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
//											networkCost = getSubgraph().getSubgraphValue().networkCoeff * prevScanCost * probability * avgRemoteDeg * hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
//											resultSetNumber *= hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
//											//System.out.println("Edge:" + hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability);
//										}
//										else {
//											totalCost = new Double(-1);
//											break;
//										}
//									}
//									totalCost += (eScanCost+vScanCost+networkCost);
//									prevScanCost = vScanCost;
//									currentStep = It.next();
//								}	
//												
//							}
//							joinCost += resultSetNumber;
//							getSubgraph().getSubgraphValue().queryCostHolder[pos] = totalCost;
//							
////							System.out.println(pos+":"+"for:"+String.valueOf(totalCost));
//						}
//						//reverse cost
//						{
//							Double totalCost = new Double(0);
//							Double prevScanCost = hueristics.numVertices;
//							Double resultSetNumber = hueristics.numVertices;
//
//							ListIterator<Step> revIt = getSubgraph().getSubgraphValue().path.listIterator(pos+1);
//							Step currentStep = revIt.previous();
//							while(revIt.hasPrevious()){
//								// TODO: make cost not count in probability when no predicate on edge/vertex
//								{
//									Double probability = null;
//									if ( currentStep.property == null )
//										probability = new Double(1);
//									else {
//										if ( hueristics.vertexPredicateMap.get(currentStep.property).containsKey(currentStep.value.toString()) )
//											probability = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).probability;
//										else {
//											totalCost = new Double(-1);
//											break;
//										}
//									}
//									resultSetNumber *= probability;
//									Double avgDeg = new Double(0);
//									Double avgRemoteDeg = new Double(0);
//									Step nextStep = revIt.previous();
//									if(nextStep.direction == Direction.OUT){
//										if ( currentStep.property == null) {
//											avgDeg = hueristics.numEdges/hueristics.numVertices;
//											avgRemoteDeg = hueristics.numRemoteVertices/(hueristics.numVertices+hueristics.numRemoteVertices) * avgDeg;
//										}
//										else {
//											avgDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgInDegree; 
//											avgRemoteDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgRemoteInDegree;
//										}	
//									}else if(nextStep.direction == Direction.IN){
//										if ( currentStep.property == null) {
//											avgDeg = hueristics.numEdges/hueristics.numVertices;
//											avgRemoteDeg = hueristics.numRemoteVertices/(hueristics.numVertices+hueristics.numRemoteVertices) * avgDeg;
//										}
//										else { 
//											avgDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgOutDegree;
//											avgRemoteDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgRemoteOutDegree;
//										}	
//									}
//									resultSetNumber *= (avgDeg+avgRemoteDeg);
//									Double eScanCost = prevScanCost * probability * avgDeg;
//									Double vScanCost = new Double(0);
//									Double networkCost = new Double(0);
//									if(nextStep.property == null)
//										vScanCost = eScanCost;
//									else {
//										if ( hueristics.edgePredicateMap.get(nextStep.property).containsKey(nextStep.value.toString()) ) {
//											vScanCost = eScanCost * hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
//											networkCost = getSubgraph().getSubgraphValue().networkCoeff * prevScanCost * probability * avgRemoteDeg * hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
//											resultSetNumber *= hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
//										}
//										else {
//											totalCost = new Double(-1);
//											break;
//										}
//									}
//									totalCost += (eScanCost+vScanCost);
//									prevScanCost = vScanCost;
//									currentStep = revIt.previous();
//								}
//							}
//							joinCost *= resultSetNumber;
//							if ( getSubgraph().getSubgraphValue().queryCostHolder[pos] != -1 && totalCost != -1) {
//								getSubgraph().getSubgraphValue().queryCostHolder[pos] += totalCost;
//								if (pos!=0 && pos!= getSubgraph().getSubgraphValue().path.size()-1)
//									getSubgraph().getSubgraphValue().queryCostHolder[pos] += joinCost;
//							}
//							else
//								getSubgraph().getSubgraphValue().queryCostHolder[pos] = new Double(-1);
//							
//						}
//						/* add that extra cost of initial scan*/
//						//TODO: Add 1 when indexed
//						if ( getSubgraph().getSubgraphValue().queryCostHolder[pos] != -1 )
//						{
//							if(!initDone)
//								getSubgraph().getSubgraphValue().queryCostHolder[pos] += hueristics.numVertices;
//							else
//								getSubgraph().getSubgraphValue().queryCostHolder[pos] +=1;
//								
//						}
////						System.out.println(pos+":Total:"+String.valueOf(getSubgraph().getSubgraphValue().queryCostHolder[pos]));
//					}
//					 
//				}
				
				
				
				
				// LOAD START VERTICES
				{
				       
//					Double minCost = getSubgraph().getSubgraphValue().queryCostHolder[getSubgraph().getSubgraphValue().startPos];
//					boolean queryPossible = true;
//					for (int i = 0; i < getSubgraph().getSubgraphValue().queryCostHolder.length ; i++) {
//						if ( getSubgraph().getSubgraphValue().queryCostHolder[i]!=0 && getSubgraph().getSubgraphValue().queryCostHolder[i]!=-1 && getSubgraph().getSubgraphValue().queryCostHolder[i] < minCost ){
//							minCost=getSubgraph().getSubgraphValue().queryCostHolder[i];
//							getSubgraph().getSubgraphValue().startPos = i;
//						}
//						if( getSubgraph().getSubgraphValue().queryCostHolder[i]==-1 )
//							queryPossible = false;
//					}
					
					String currentProperty = null;
					Object currentValue = null;
					getSubgraph().getSubgraphValue().startPos=fixedPos;//used for debugging
					currentProperty = getSubgraph().getSubgraphValue().path.get(getSubgraph().getSubgraphValue().startPos).property; 
					currentValue = getSubgraph().getSubgraphValue().path.get(getSubgraph().getSubgraphValue().startPos).value;
					
					// TODO: check if the property is indexed** uncomment this if using indexes
					long QueryId=getQueryId();
					try{
						synchronized(queryLock){
							if(!queryMade){
								LOG.info("Querying Lucene Index");
								makeQuery(currentProperty,currentValue);
								LOG.info("Querying Lucene Done");
							}
						}
						
//					System.out.println("Starting Position:" + getSubgraph().getSubgraphValue().startPos +"  Query min Cost:" + minCost + "   Path Size:" + getSubgraph().getSubgraphValue().path.size());	
//					System.out.println("*******Querying done********:"+hits.length);
					
						if(hits.length>0){
							LOG.info("Processing Initial Vertices Returned");
							for (int i=0;i<hits.length;i++){
								Document doc = indexSearcher.doc(hits[i].doc);
								if ( Long.valueOf(doc.get("subgraphid")) == getSubgraph().getSubgraphId().get() ){
									Long _vertexId = Long.valueOf(doc.get("id"));
									String _message = "V:"+String.valueOf(_vertexId);
//									System.out.println("STARTING VERTEX:" + _message);
									if ( getSubgraph().getSubgraphValue().startPos == 0)
									  getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(QueryId,_vertexId,_message, getSubgraph().getSubgraphValue().startPos, _vertexId,getSubgraph().getSubgraphValue().startPos, getSubgraph().getSubgraphId().get(), 0) );
									else
									if( getSubgraph().getSubgraphValue().startPos == (getSubgraph().getSubgraphValue().path.size()-1))
									  getSubgraph().getSubgraphValue().revLocalVertexList.add( new VertexMessageSteps(QueryId,_vertexId,_message, getSubgraph().getSubgraphValue().startPos , _vertexId,getSubgraph().getSubgraphValue().startPos, getSubgraph().getSubgraphId().get(), 0) );
									else{
									  getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(QueryId,_vertexId,_message, getSubgraph().getSubgraphValue().startPos, _vertexId,getSubgraph().getSubgraphValue().startPos, getSubgraph().getSubgraphId().get(), 0) );
									  getSubgraph().getSubgraphValue().revLocalVertexList.add( new VertexMessageSteps(QueryId,_vertexId,_message, getSubgraph().getSubgraphValue().startPos , _vertexId,getSubgraph().getSubgraphValue().startPos, getSubgraph().getSubgraphId().get(), 0) );
									}
										
//									getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(_vertexId,_message,0) );
								}
							}
							LOG.info("Processing Initial Vertices Done");
						}
						
					}catch(Exception e){e.printStackTrace();}
					
		
					// TODO : else iteratively check for satisfying vertices
//					if ( queryPossible == true )
//					for(IVertex<MapWritable, MapWritable, LongWritable, LongWritable> vertex: getSubgraph().getLocalVertices()) {
//						if ( vertex.isRemote() ) continue;
//						
//						if ( compareValuesUtil(vertex.getValue().get(new Text(currentProperty)).toString(), currentValue.toString()) ) {
//							String _message = "V:"+String.valueOf(vertex.getVertexId().get());
//							System.out.println("Vertex id:" + vertex.getVertexId().get() + "Property:"+currentProperty +" Value:" + vertex.getValue().get(new Text(currentProperty)).toString());
//							if ( getSubgraph().getSubgraphValue().startPos == 0)
//								getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(QueryId,vertex.getVertexId().get(),_message, getSubgraph().getSubgraphValue().startPos, vertex.getVertexId().get(),getSubgraph().getSubgraphValue().startPos, getSubgraph().getSubgraphId().get(), 0) );
//							else
//							if( getSubgraph().getSubgraphValue().startPos == (getSubgraph().getSubgraphValue().path.size()-1))
//								getSubgraph().getSubgraphValue().revLocalVertexList.add( new VertexMessageSteps(QueryId,vertex.getVertexId().get(),_message, getSubgraph().getSubgraphValue().startPos , vertex.getVertexId().get(),getSubgraph().getSubgraphValue().startPos, getSubgraph().getSubgraphId().get(), 0) );
//							else{
//								getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(QueryId,vertex.getVertexId().get(),_message, getSubgraph().getSubgraphValue().startPos, vertex.getVertexId().get(),getSubgraph().getSubgraphValue().startPos, getSubgraph().getSubgraphId().get(), 0) );
//								getSubgraph().getSubgraphValue().revLocalVertexList.add( new VertexMessageSteps(QueryId,vertex.getVertexId().get(),_message, getSubgraph().getSubgraphValue().startPos , vertex.getVertexId().get(),getSubgraph().getSubgraphValue().startPos, getSubgraph().getSubgraphId().get(), 0) );
//							}
//							//output(partition.getId(), subgraph.getId(), subgraphProperties.getValue(currentProperty).toString());
//						}
//					}
					
					
					Iterator msgIter=messageList.iterator();
					while(msgIter.hasNext()){
					  msgIter.remove();
					}
				}
				
			}
			
		LOG.info("Traversal Started");
			// CHECK MSSG-PROCESS FORWARD-PROCESS BACKWARD
			if(getSuperstep()>=3) {
				LOG.info("Initial memory" + " Free Memory: " + Runtime.getRuntime().freeMemory() + " Total Memory:" + Runtime.getRuntime().totalMemory());
				// CHECK INCOMING MESSAGE, ADD VERTEX TO APPRT LIST
				// this is for the partially executed paths, which have been 
				// forwarded from a different machine
			        Iterator msgIter=messageList.iterator();
				if(msgIter.hasNext()){
					for (IMessage<LongWritable, Text> message: messageList){
						
						if(message.getMessage().toString().contains("output();") ) {
							if ( Long.parseLong(message.getMessage().toString().split(Pattern.quote(";"))[3] ) == getSubgraph().getSubgraphId().get() ){
								//System.out.println("OUTPUTMESSAGE:" + new String(message.getData()));
								join(message);
							
							}
							continue;
						}
						
						if ( Long.parseLong(new String( message.getMessage().toString() ).split(Pattern.quote(";"))[6] ) == getSubgraph().getSubgraphId().get() ){
							VertexMessageSteps v=processMessage(message) ;
							if(v!=null){
								if(new String( message.getMessage().toString() ).contains("rev()") )
									getSubgraph().getSubgraphValue().revLocalVertexList.add( v );
								else
									getSubgraph().getSubgraphValue().forwardLocalVertexList.add( v ); 
							}
						
						}
						
					}
	
				}
			
			//output(partition.getId(), subgraph.getId(), getSuperstep()+":"+forwardLocalVertexList.size()+":"+revLocalVertexList.size());
			// PROCESS FORWARD LIST
			//System.out.println("FORWARD LIST:"+forwardLocalVertexList.isEmpty() +" REV LIST:"+revLocalVertexList.isEmpty() + "SGID:" + subgraph.getId() + " PID:" + partition.getId());
				long[] hopTime=new long[getSubgraph().getSubgraphValue().path.size()];
				long[] count=new long[getSubgraph().getSubgraphValue().path.size()];//assuming default is zero
				String countStr1="";
			
//				LOG.info("SGID:" + getSubgraph().getSubgraphId()+" Traversal Memory" + " Free Memory: " + Runtime.getRuntime().freeMemory() + " Total Memory:" + Runtime.getRuntime().totalMemory() + " TraversalSteps:" + countStr1 + ":" +getSubgraph().getSubgraphValue().forwardLocalVertexList.size()+"," + getSuperstep());
				LinkedList<VertexMessageSteps> nextStepForwardLocalVertexList = new LinkedList<VertexMessageSteps>();
			while(!getSubgraph().getSubgraphValue().forwardLocalVertexList.isEmpty()) {
				VertexMessageSteps vertexMessageStep = getSubgraph().getSubgraphValue().forwardLocalVertexList.poll();
				if(getSubgraph().getSubgraphValue().forwardLocalVertexList.isEmpty()) {
					count[vertexMessageStep.stepsTraversed]++;
					getSubgraph().getSubgraphValue().forwardLocalVertexList=nextStepForwardLocalVertexList;
					String countStr="";
					for(long c:count) {
						countStr+=","+c;
					}
//				LOG.info("SGID:" + getSubgraph().getSubgraphId() +" Traversal Memory" + " Free Memory: " + Runtime.getRuntime().freeMemory() + " Total Memory:" + Runtime.getRuntime().totalMemory() + " TraversalSteps:" + countStr + ":" +getSubgraph().getSubgraphValue().forwardLocalVertexList.size()+"," + getSuperstep());
				}
				//output(partition.getId(), subgraph.getId(), "FORWARD-LIST");
				/* if last step,end that iteration*/
				//System.out.println("Reached:" + vertexMessageStep.startVertexId + " Path Size:" + vertexMessageStep.stepsTraversed + "/" + (path.size()-1));
				if ( vertexMessageStep.stepsTraversed == getSubgraph().getSubgraphValue().path.size()-1 ){
					// TODO :gather instead of output 
					//output(partition.getId(), subgraph.getId(), vertexMessageStep.message);
					// send this as a reduceMessage
					//if (vertexMessageStep.previousSubgraphId == subgraph.getId()) {
					//	if ( !resultsMap.containsKey(vertexMessageStep.startVertexId) )
					//		resultsMap.put(vertexMessageStep.startVertexId, new ResultSet());
					//	resultsMap.get(vertexMessageStep.startVertexId).forwardResultSet.add(vertexMessageStep.message);
					//	
					//}	
//					else {
//						forwardOutputToSubgraph(1,vertexMessageStep);
//						output(partition.getId(), subgraph.getId(), "output();for();"+vertexMessageStep.message);
//					}
					
//					if (!recursivePaths.containsKey(vertexMessageStep.vertexId))
//					{
//						ArrayList<RecursivePathMaintained> tempList = new ArrayList<RecursivePathMaintained>();
//						tempList.add(new RecursivePathMaintained(vertexMessageStep.startVertexId, vertexMessageStep.message,0));
//						recursivePaths.put( vertexMessageStep.vertexId, tempList);
//					}
//					else{
//						recursivePaths.get(vertexMessageStep.vertexId).add(new RecursivePathMaintained(vertexMessageStep.startVertexId, vertexMessageStep.message,0));
//					}
//					System.out.println("Querying Output Path:" + vertexMessageStep.queryId+","+vertexMessageStep.startStep+","+true+","+vertexMessageStep.startVertexId );
					if(getSubgraph().getSubgraphValue().outputPathMaintainance.containsKey(new OutputPathKey(vertexMessageStep.queryId,vertexMessageStep.startStep,true,vertexMessageStep.startVertexId))){
						forwardOutputToSubgraph(1,vertexMessageStep);
					}
					else{
					    time=System.currentTimeMillis();
						if ( !getSubgraph().getSubgraphValue().resultsMap.containsKey(vertexMessageStep.startVertexId) )
							getSubgraph().getSubgraphValue().resultsMap.put(vertexMessageStep.startVertexId, new ResultSet());
						//System.out.println("MESSAGE ADDED TO FORWARDRESULTSET:" + vertexMessageStep.message);
						getSubgraph().getSubgraphValue().resultsMap.get(vertexMessageStep.startVertexId).forwardResultSet.add(vertexMessageStep.message);
						getSubgraph().getSubgraphValue().resultCollectionTime+=(System.currentTimeMillis()-time);
					}
						
					continue;
				}
				long startTime=System.nanoTime();
				Step nextStep = getSubgraph().getSubgraphValue().path.get(vertexMessageStep.stepsTraversed+1);
				count[vertexMessageStep.stepsTraversed]++;
				
				
				IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex = getSubgraph().getVertexById(new LongWritable(vertexMessageStep.vertexId));
				
				if( nextStep.type == Type.EDGE ) {
					
					if ( nextStep.direction == Direction.OUT ) {
						/* null predicate handling*/
						//int count=0;
						boolean flag=false;
						boolean addFlag=false;
						if ( nextStep.property == null && nextStep.value == null ) {
							for( IEdge<MapValue, LongWritable, LongWritable> edge: currentVertex.getOutEdges()) {
								//count++;
//								System.out.println("Traversing edges");
								IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex = getSubgraph().getVertexById(edge.getSinkVertexId());
								StringBuilder _modifiedMessage = new StringBuilder("");
								_modifiedMessage.append(vertexMessageStep.message).append("-->E:").append(String.valueOf(edge.getEdgeId().get())).append("-->V:").append(String.valueOf(otherVertex.getVertexId().get()));
								if ( !otherVertex.isRemote() ) {
//									System.out.println("Path Till Now:" + _modifiedMessage.toString());
									nextStepForwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
								}
								else {
									
									
									if(!flag){
									addFlag=StoreRecursive(vertexMessageStep,_modifiedMessage.toString(),true);	
									
									flag=true;
									}
									
									if(addFlag){
										getSubgraph().getSubgraphValue().forwardRemoteVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1,vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed+1, vertexMessageStep.previousSubgraphId,vertexMessageStep.previousPartitionId));
									}
									
								}
									
							}
						}
						/* filtered edge*/
						else {
							for( IEdge<MapValue, LongWritable, LongWritable> edge: currentVertex.getOutEdges() ) {
								
								//System.out.println("COMPARING:" + subgraphProperties.getValue(nextStep.property));
								//output(partition.getId(), subgraph.getId(), currentVertex.getId()+":"+subgraphProperties.getValue("relation"));
								if ( compareValuesUtil(edge.getValue().get(nextStep.property.toString()).toString(), nextStep.value.toString()) ) {
									IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex = getSubgraph().getVertexById(edge.getSinkVertexId());
									StringBuilder _modifiedMessage = new StringBuilder("");
									_modifiedMessage.append(vertexMessageStep.message).append("-->E:").append(String.valueOf(edge.getEdgeId().get())).append("-->V:").append(String.valueOf(otherVertex.getVertexId().get()));
									if ( !otherVertex.isRemote() ) {
										/* TODO :add the correct value to list*/
										nextStepForwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
									}
									else {
										/* TODO :add vertex to forwardRemoteVertexList*/
										
										
										if(!flag){
										addFlag=StoreRecursive(vertexMessageStep,_modifiedMessage.toString(),true);	
										
										flag=true;
										}
										
										if(addFlag){
											getSubgraph().getSubgraphValue().forwardRemoteVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1,vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed+1, vertexMessageStep.previousSubgraphId,vertexMessageStep.previousPartitionId));
										}
									}
								}
							}
						}
					}
					else if ( nextStep.direction == Direction.IN ) {

						/* null predicate handling*/
						boolean flag=false;
						boolean addFlag=false;
						if ( nextStep.property == null && nextStep.value == null ) {
							if(getSubgraph().getSubgraphValue().InEdges.containsKey(currentVertex.getVertexId().get()))
							for(Map.Entry<Long, EdgeAttr> edgeMap: getSubgraph().getSubgraphValue().InEdges.get(currentVertex.getVertexId().get()).entrySet()) {
								long otherVertexId = edgeMap.getKey();
								StringBuilder _modifiedMessage = new StringBuilder("");
								_modifiedMessage.append(vertexMessageStep.message).append("<--E:").append(String.valueOf(edgeMap.getValue().EdgeId)).append("<--V:").append(String.valueOf(otherVertexId));
								if ( !edgeMap.getValue().isRemote ) {
									/* TODO :add the correct value to list*/
									nextStepForwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
								}
								else{
								/* TODO :add vertex to forwardRemoteVertexList*/
									
									if(!flag){
									addFlag=StoreRecursive(vertexMessageStep,_modifiedMessage.toString(),true);	
									
									flag=true;
									}
									
									if(addFlag){
										getSubgraph().getSubgraphValue().forwardRemoteVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed+1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
									}
									
								}
							
							}
						}
						/* filtered edge*/
						else {
							if(getSubgraph().getSubgraphValue().InEdges.containsKey(currentVertex.getVertexId().get()))
							for( Map.Entry<Long, EdgeAttr> edgeMap: getSubgraph().getSubgraphValue().InEdges.get(currentVertex.getVertexId().get()).entrySet() ) {
								//ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
								//output(partition.getId(), subgraph.getId(), currentVertex.getId()+":"+subgraphProperties.getValue("relation"));
								if ( compareValuesUtil(edgeMap.getValue().Value.toString(), nextStep.value.toString()) ) {
									long otherVertexId = edgeMap.getKey();
									StringBuilder _modifiedMessage = new StringBuilder("");
									_modifiedMessage.append(vertexMessageStep.message).append("<--E:").append(String.valueOf(edgeMap.getValue().EdgeId)).append("<--V:").append(String.valueOf(otherVertexId));
									if ( !edgeMap.getValue().isRemote ) {
										/* TODO :add the correct value to list*/
										nextStepForwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
									}
									else{
							
										if(!flag){
										addFlag=StoreRecursive(vertexMessageStep,_modifiedMessage.toString(),true);	
										
										flag=true;
										}
										
										if(addFlag){
											getSubgraph().getSubgraphValue().forwardRemoteVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed+1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
										}
									}
									
									
								}
							
							}
						}
					}
					
				}
				else if ( nextStep.type == Type.VERTEX ) {
					
					/* null predicate*/
					if( nextStep.property == null && nextStep.value == null ) {
						/* add appropriate value later*/
						nextStepForwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
						//forwardLocalVertexList.add(vertexMessageStep);
					}
					/* filtered vertex*/
					else {
//						ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForVertex(currentVertex.getId());
						if ( compareValuesUtil(currentVertex.getValue().get(nextStep.property).toString(), nextStep.value.toString()) ) {
							/* add appropriate value later*/
							nextStepForwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
							//forwardLocalVertexList.add(vertexMessageStep);
						}
					}
					
				}
				long endTime=System.nanoTime();
				hopTime[vertexMessageStep.stepsTraversed]+=(endTime-startTime);
						
				
			}
			
			
			// PROCESS REVERSE LIST
			while(!getSubgraph().getSubgraphValue().revLocalVertexList.isEmpty()) {
				VertexMessageSteps vertexMessageStep = getSubgraph().getSubgraphValue().revLocalVertexList.poll();
				
				/* if last step,end that iteration, while traversing in reverse direction last step is first step which is zero */
				if ( vertexMessageStep.stepsTraversed == 0 ){
					//if current subgraph is not source subgraph then start recursive aggregation of partial results
//					System.out.println("Querying Output Path:" + vertexMessageStep.queryId+","+vertexMessageStep.startStep+","+false+","+vertexMessageStep.startVertexId );
					if(getSubgraph().getSubgraphValue().outputPathMaintainance.containsKey(new OutputPathKey(vertexMessageStep.queryId,vertexMessageStep.startStep,false,vertexMessageStep.startVertexId))){
						forwardOutputToSubgraph(0,vertexMessageStep);
					}
					else{//else if current subgraph is source subgraph then store the results
					  time=System.currentTimeMillis();
						if ( !getSubgraph().getSubgraphValue().resultsMap.containsKey(vertexMessageStep.startVertexId) )
							getSubgraph().getSubgraphValue().resultsMap.put(vertexMessageStep.startVertexId, new ResultSet());
						
						getSubgraph().getSubgraphValue().resultsMap.get(vertexMessageStep.startVertexId).revResultSet.add(vertexMessageStep.message);
						getSubgraph().getSubgraphValue().resultCollectionTime+=(System.currentTimeMillis() - time);
					}
										
					continue;
				}
				
				Step prevStep = getSubgraph().getSubgraphValue().path.get(vertexMessageStep.stepsTraversed-1);
				IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex = getSubgraph().getVertexById(new LongWritable(vertexMessageStep.vertexId));
			
				
				
				if( prevStep.type == Type.EDGE ) {
					
					if ( prevStep.direction == Direction.OUT ) {
						/* null predicate handling*/
						boolean flag=false;
						boolean addFlag=false;
						if ( prevStep.property == null && prevStep.value == null ) {
							if(getSubgraph().getSubgraphValue().InEdges.containsKey(currentVertex.getVertexId().get()))
							for( Map.Entry<Long, EdgeAttr> edgeMap: getSubgraph().getSubgraphValue().InEdges.get(currentVertex.getVertexId().get()).entrySet()) {
								long otherVertexId = edgeMap.getKey();
								StringBuilder _modifiedMessage = new StringBuilder("");
								_modifiedMessage.append("V:").append(String.valueOf(otherVertexId)).append("-->E:").append(String.valueOf(edgeMap.getValue().EdgeId)).append("-->").append(vertexMessageStep.message);
								if ( !edgeMap.getValue().isRemote ) {
									/* TODO :add the correct value to list*/
									getSubgraph().getSubgraphValue().revLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
								}
								else{
									
									if(!flag){
									addFlag=StoreRecursive(vertexMessageStep,_modifiedMessage.toString(),false);
									
									flag=true;
									}
									
									if(addFlag){
										getSubgraph().getSubgraphValue().revRemoteVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed-1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
									}
									
								}
							
							}
						}
						/* filtered edge*/
						else {
							if(getSubgraph().getSubgraphValue().InEdges.containsKey(currentVertex.getVertexId().get()))
							for( Map.Entry<Long, EdgeAttr> edgeMap: getSubgraph().getSubgraphValue().InEdges.get(currentVertex.getVertexId().get()).entrySet() ) {
								//ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
								//output(partition.getId(), subgraph.getId(), currentVertex.getId()+":"+subgraphProperties.getValue("relation"));
								if ( compareValuesUtil(edgeMap.getValue().Value, prevStep.value) ) {
									long otherVertexId = edgeMap.getKey();
									//output(partition.getId(), subgraph.getId(), String.valueOf(otherVertex.getId()));
									StringBuilder _modifiedMessage = new StringBuilder("");
									_modifiedMessage.append("V:").append(String.valueOf(otherVertexId)).append("-->E:").append(String.valueOf(edgeMap.getValue().EdgeId)).append("-->").append(vertexMessageStep.message);
									if ( !edgeMap.getValue().isRemote) {
										/* TODO :add the correct value to list*/
										getSubgraph().getSubgraphValue().revLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
									}
									else{
										
										
										if(!flag){
										addFlag=StoreRecursive(vertexMessageStep,_modifiedMessage.toString(),false);
										
										flag=true;
										}
										
										if(addFlag){
											getSubgraph().getSubgraphValue().revRemoteVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed-1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
										}
										
									}
									
								}
							
							}
						}
					}
					else if ( prevStep.direction == Direction.IN ) {

						/* null predicate handling*/
						boolean flag=false;
						boolean addFlag=false;
						if ( prevStep.property == null && prevStep.value == null ) {
							for(IEdge<MapValue, LongWritable, LongWritable> edge: currentVertex.getOutEdges()) {
								IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex = getSubgraph().getVertexById(edge.getSinkVertexId());
								StringBuilder _modifiedMessage = new StringBuilder("");
								_modifiedMessage.append("V:").append(String.valueOf(otherVertex.getVertexId().get())).append("<--E:").append(String.valueOf(edge.getEdgeId().get())).append("<--").append(vertexMessageStep.message);
								if ( !otherVertex.isRemote() ) {
									/* add the correct value to list*/
									getSubgraph().getSubgraphValue().revLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
								}
								/* TODO : clarify with Ravi about InEdge having remote source( not possible?)*/
								else {
									/* TODO :add vertex to revRemoteVertexList*/
									
									if(!flag){
									addFlag=StoreRecursive(vertexMessageStep,_modifiedMessage.toString(),false);
									
									flag=true;
									}
									
									if(addFlag){
										getSubgraph().getSubgraphValue().revRemoteVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.vertexId, vertexMessageStep.stepsTraversed-1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
									}
									
								}
									
							}
							
						}
						/* filtered edge*/
						else {
							for( IEdge<MapValue, LongWritable, LongWritable> edge: currentVertex.getOutEdges() ) {
//								ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
//								output(partition.getId(), subgraph.getId(), currentVertex.getId()+":"+subgraphProperties.getValue("relation"));
							  //CHANGE it when moving to hashmaps
								if ( compareValuesUtil(edge.getValue().get(prevStep.property.toString()).toString(), prevStep.value.toString()) ) {
									IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex = getSubgraph().getVertexById(edge.getSinkVertexId());
									StringBuilder _modifiedMessage = new StringBuilder("");
									_modifiedMessage.append("V:").append(String.valueOf(otherVertex.getVertexId().get())).append("<--E:").append(String.valueOf(edge.getEdgeId().get())).append("<--").append(vertexMessageStep.message);
									if ( !otherVertex.isRemote() ) {
										/* TODO :add the correct value to list*/
										getSubgraph().getSubgraphValue().revLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
									}
									/* TODO : clarify with Ravi about InEdge having remote source( not possible?)*/
									else {
										/* TODO :add vertex to revRemoteVertexList*/
										
										if(!flag){
										addFlag=StoreRecursive(vertexMessageStep,_modifiedMessage.toString(),false);
										
										flag=true;
										}
										
										if(addFlag){
											getSubgraph().getSubgraphValue().revRemoteVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed-1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
										}
										
									}
								}
							}
						}
					}
					
				}
				else if ( prevStep.type == Type.VERTEX ) {
					
					/* null predicate*/
					if( prevStep.property == null && prevStep.value == null ) {
						/* add appropriate value later*/
						getSubgraph().getSubgraphValue().revLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
						//revLocalVertexList.add(vertexMessageStep);
					}
					/* filtered vertex*/
					else {
					        LOG.info("PROP:"+prevStep.property.toString() + " VALUE:" + currentVertex.getValue().get(prevStep.property.toString()));
//						ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForVertex(currentVertex.getId());
						if ( compareValuesUtil(currentVertex.getValue().get(prevStep.property.toString()).toString(), prevStep.value.toString()) ) {
							/* add appropriate value later*/
						  
							getSubgraph().getSubgraphValue().revLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep,vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
							//revLocalVertexList.add(vertexMessaf.vertexIdgeStep);
							//output(partition.getId(), subgraph.getId(), vertexMessageStep.message);
						}
					}
					
				}
			
				
			}

			LOG.info("Sending Messages");
			// TODO: send the messages in Remote vertex list
			for(VertexMessageSteps stuff: getSubgraph().getSubgraphValue().forwardRemoteVertexList){
				// send message to all the remote vertices
				
				IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)getSubgraph().getVertexById(new LongWritable(stuff.vertexId));
				StringBuilder remoteMessage = new StringBuilder("for();");
				//remoteMessage.append(String.valueOf(stuff.vertexId.longValue())).append(";").append(stuff.message).append(";").append(stuff.stepsTraversed) ;
				if(remoteVertex!=null){
					remoteMessage.append(String.valueOf(stuff.startVertexId)).append(";").append(String.valueOf(stuff.previousSubgraphId)).append(";").append(stuff.previousPartitionId).append(";").append(stuff.vertexId).append(";").append(stuff.stepsTraversed).append(";").append(remoteVertex.getSubgraphId().get());
					}
					else{
						remoteMessage.append(String.valueOf(stuff.startVertexId)).append(";").append(String.valueOf(stuff.previousSubgraphId)).append(";").append(stuff.previousPartitionId).append(";").append(stuff.vertexId).append(";").append(stuff.stepsTraversed).append(";").append(getSubgraph().getSubgraphValue().InEdges.get(stuff.startVertexId).get(stuff.vertexId).sinkSubgraphId);
					}
				remoteMessage.append(";").append(stuff.queryId);
				Text remoteM = new Text(remoteMessage.toString());
				
				if(remoteVertex!=null){
					sendMessage(remoteVertex.getSubgraphId(),remoteM);
				}
					
				else{
					
					sendMessage(new LongWritable(getSubgraph().getSubgraphValue().InEdges.get(stuff.startVertexId).get(stuff.vertexId).sinkSubgraphId),remoteM);
				}
					
			}
			getSubgraph().getSubgraphValue().forwardRemoteVertexList.clear();
			
			for(VertexMessageSteps stuff: getSubgraph().getSubgraphValue().revRemoteVertexList){
				// send message to all the remote vertices
				IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)getSubgraph().getVertexById(new LongWritable(stuff.vertexId));
				StringBuilder remoteMessage = new StringBuilder("rev();");
				//remoteMessage.append(String.valueOf(stuff.vertexId.longValue())).append(";").append(stuff.message).append(";").append(stuff.stepsTraversed) ;
				if(remoteVertex!=null){
				remoteMessage.append(String.valueOf(stuff.startVertexId)).append(";").append(String.valueOf(stuff.previousSubgraphId)).append(";").append(stuff.previousPartitionId).append(";").append(stuff.vertexId).append(";").append(stuff.stepsTraversed).append(";").append(remoteVertex.getSubgraphId().get());
				}
				else{
					remoteMessage.append(String.valueOf(stuff.startVertexId)).append(";").append(String.valueOf(stuff.previousSubgraphId)).append(";").append(stuff.previousPartitionId).append(";").append(stuff.vertexId).append(";").append(stuff.stepsTraversed).append(";").append(getSubgraph().getSubgraphValue().InEdges.get(stuff.startVertexId).get(stuff.vertexId).sinkSubgraphId);
				}
				remoteMessage.append(";").append(stuff.queryId);	
				Text remoteM = new Text(remoteMessage.toString());
				
				if(remoteVertex!=null){
					sendMessage(remoteVertex.getSubgraphId(),remoteM);
				}
					
				else{
					
					sendMessage(new LongWritable(getSubgraph().getSubgraphValue().InEdges.get(stuff.startVertexId).get(stuff.vertexId).sinkSubgraphId),remoteM);
				}
			}
			getSubgraph().getSubgraphValue().revRemoteVertexList.clear();
			
			// TODO: Send back the partial result lists
			for(OutputMessageSteps stuff: getSubgraph().getSubgraphValue().outputList){
				sendMessage(stuff.targetSubgraphId,stuff.message);
			}
			
			getSubgraph().getSubgraphValue().outputList.clear();
			
			
			//printing traversal data for analysis

			String countStr="";
			for(long c:count) {
				countStr+=","+c;
			}
		LOG.info("SGID:" + getSubgraph().getSubgraphId() +" Traversal Memory" + " Free Memory: " + Runtime.getRuntime().freeMemory() + " Total Memory:" + Runtime.getRuntime().totalMemory() + " TraversalSteps:" + countStr + ":" +getSubgraph().getSubgraphValue().forwardLocalVertexList.size()+"," + getSuperstep());
		
		String timeStr="";
		for(long t:hopTime) {
			timeStr+=","+t;
		}
//		System.gc();
//	LOG.info("SGID:" + getSubgraph().getSubgraphId() +" Traversal Memory" + " Free Memory: " + Runtime.getRuntime().freeMemory() + " Total Memory:" + Runtime.getRuntime().totalMemory() + " TraversalTimeSteps:" + timeStr + ":" + getSuperstep());
	
		}
		}
		
		LOG.info("Traversal Ends");
		if(getSuperstep()>=3)
			voteToHalt();
	}
	
	
	

	/**
	 * This function takes current vertex message step and stores the partial path, done before sending message to remote subgraph 
	 * 
	 */
	private boolean StoreRecursive(VertexMessageSteps vertexMessageStep,String _modifiedMessage,boolean _direction) {
	
		boolean flag=true;
		
		int dir=0;
		if(_direction==true){
			dir=1;
		}
		
		if(!getSubgraph().getSubgraphValue().recursivePaths.containsKey(new RecursivePathKey(vertexMessageStep.queryId,vertexMessageStep.stepsTraversed,_direction,vertexMessageStep.vertexId))){
			
			ArrayList<RecursivePathMaintained> tempList = new ArrayList<RecursivePathMaintained>();
			tempList.add(new RecursivePathMaintained(vertexMessageStep.startVertexId,vertexMessageStep.startStep, _modifiedMessage.toString(),dir));
			getSubgraph().getSubgraphValue().recursivePaths.put(new RecursivePathKey(vertexMessageStep.queryId,vertexMessageStep.stepsTraversed,_direction,vertexMessageStep.vertexId), tempList);
//			System.out.println(vertexMessageStep.queryId+" Storing Recursive path:"+vertexMessageStep.startVertexId+":" + vertexMessageStep.vertexId +":"+_modifiedMessage+ ":" + vertexMessageStep.startStep + ":" + vertexMessageStep.stepsTraversed + ":" + _direction);
//			vertexMessageStep.startVertexId = vertexMessageStep.vertexId;
		}
		else{
			if(!getSubgraph().getSubgraphValue().recursivePaths.get(new RecursivePathKey(vertexMessageStep.queryId,vertexMessageStep.stepsTraversed,_direction,vertexMessageStep.vertexId)).contains(new RecursivePathMaintained(vertexMessageStep.startVertexId,vertexMessageStep.startStep, _modifiedMessage.toString(),dir))){
				
				getSubgraph().getSubgraphValue().recursivePaths.get(new RecursivePathKey(vertexMessageStep.queryId,vertexMessageStep.stepsTraversed,_direction,vertexMessageStep.vertexId)).add(new RecursivePathMaintained(vertexMessageStep.startVertexId,vertexMessageStep.startStep, _modifiedMessage.toString(),dir));
//				System.out.println(vertexMessageStep.queryId+" Adding Recursive path:"+vertexMessageStep.startVertexId+":" + vertexMessageStep.vertexId +":"+_modifiedMessage+ ":" + vertexMessageStep.startStep + ":" + vertexMessageStep.stepsTraversed + ":" + _direction  + ":" + !getSubgraph().getSubgraphValue().recursivePaths.get(new RecursivePathKey(vertexMessageStep.queryId,vertexMessageStep.stepsTraversed,_direction,vertexMessageStep.vertexId)).contains(new RecursivePathMaintained(vertexMessageStep.startVertexId,vertexMessageStep.startStep, _modifiedMessage.toString(),dir)) );
				
				//Checking partialResultCache if any partialResult present prior to this and sending result back
//				
//				if(partialResultCache.containsKey(new RecursivePathKey(vertexMessageStep.queryId, vertexMessageStep.stepsTraversed,_direction,vertexMessageStep.vertexId))){
//					
//					for(String partialResult:partialResultCache.get(new RecursivePathKey(vertexMessageStep.queryId, vertexMessageStep.stepsTraversed,_direction,vertexMessageStep.vertexId))){
//						
//						StringBuilder result =new StringBuilder(partialResult);
//						if (_direction)
//							result.insert(0, vertexMessageStep.message);
//						else
//							result.append(vertexMessageStep.message);
//						
//						System.out.println("END:" + vertexMessageStep.vertexId + "PATH:"+vertexMessageStep.message + "Merged path:" + result);
//						Integer recursiveStartStep=vertexMessageStep.startStep;
//						boolean recursion=false;
//						System.out.println("partialJoinQuery:" + vertexMessageStep.queryId +","+ recursiveStartStep+","+ _direction+","+ vertexMessageStep.startVertexId);
//						
//						if ( outputPathMaintainance.containsKey(new OutputPathKey(vertexMessageStep.queryId, recursiveStartStep, _direction, vertexMessageStep.startVertexId)))
//						{
//							
//							for ( Pair entry: outputPathMaintainance.get(new OutputPathKey(vertexMessageStep.queryId,recursiveStartStep,_direction,vertexMessageStep.startVertexId))){
//								StringBuilder remoteMessage = new StringBuilder("output();");
//								if(_direction)
//									remoteMessage.append("for();");
//								else
//									remoteMessage.append("rev();");
//								remoteMessage.append(entry.endVertex.toString()).append(";");
//								remoteMessage.append(entry.prevSubgraphId.toString()).append(";");
//								remoteMessage.append(result).append(";").append(vertexMessageStep.queryId).append(";").append(recursiveStartStep);
//								SubGraphMessage remoteM = new SubGraphMessage(remoteMessage.toString().getBytes());
//								
//								System.out.println("Sending partialJOIN Message:" + remoteMessage.toString());
//								outputList.add(new OutputMessageSteps(remoteM, entry.prevPartitionId));
//							}
//							recursion=true;
//						}
//									
//						System.out.println("Adding result");
//						if(!recursion){
//							if ( !resultsMap.containsKey(vertexMessageStep.startVertexId) )
//								resultsMap.put(vertexMessageStep.startVertexId, new ResultSet());
//							if ( _direction ) 
//								resultsMap.get(vertexMessageStep.startVertexId).forwardResultSet.add(result.toString());
//							else
//								resultsMap.get(vertexMessageStep.startVertexId).revResultSet.add(result.toString());
//						}
//						
//						
//					}
//				}
				
				//********************END*************************
				
				
			}
			
			
			
			flag=false;
		}
		//*******************************END*******************************
		
		return flag;
	}








	
	
	
	
	
	
	
	/**
	 * Process an incoming remote message, creates appropriate entry in 'outputPathMaintainance',
	 * which may be required for recursive aggregation of partial results.
	 * 
	 */
	private VertexMessageSteps processMessage(IMessage<LongWritable, Text> _message){
		
		
		//TODO:add queryid to vertex message step
		String message = _message.getMessage().toString();
//		System.out.println("RECVD REMOTE MSG:" + message);
		String[] split = message.split(Pattern.quote(";"));
		boolean _dir=false;
		if(split[0].equals("for()")){
			_dir=true;
		}
		Long _endVertexId = Long.parseLong( split[1] );
		Long _previousSubgraphId = Long.parseLong( split[2] );
		Integer _previousPartitionId = Integer.parseInt( split[3] );
		Long _vertexId = Long.parseLong( split[4] );
		Integer _steps = Integer.parseInt( split[5] );
		//Recent change....Reminder:DONE
		//_steps=_dir?_steps+1:_steps-1;
		Long _queryId=Long.parseLong(split[7]);
		//adding to the recursive path maintenance
		boolean pathAlreadyExists=false;
//			System.out.println("Storing PATH:" + _queryId + "," + _steps + "," + _dir + "," + _vertexId+"," + _endVertexId);
			
			if(getSubgraph().getSubgraphValue().outputPathMaintainance.containsKey(new OutputPathKey(_queryId,_steps,_dir,_vertexId))){
				getSubgraph().getSubgraphValue().outputPathMaintainance.get(new OutputPathKey(_queryId,_steps,_dir,_vertexId)).add( new Pair(_endVertexId,_previousSubgraphId,_previousPartitionId));
				pathAlreadyExists=true;
		
		}else{
			
			List<Pair> tempList=new ArrayList<Pair>();
			tempList.add(new Pair(_endVertexId,_previousSubgraphId,_previousPartitionId));
			
			getSubgraph().getSubgraphValue().outputPathMaintainance.put(new OutputPathKey(_queryId,_steps,_dir,_vertexId),tempList);
		}
		
//			//Debug code
//			VertexMessageSteps v=new VertexMessageSteps(_queryId,_vertexId,"V:" + _vertexId, _steps, _vertexId,_steps, subgraph.getId() , partition.getId());
//			if(v.startVertexId==5461450)
//				System.out.println("SToring VertexMessageStep:" + v.queryId+"," +v.startStep +"," +_dir+","+ v.startVertexId);
//			//end of Debug code
			
			
			//FIX: new code to remove duplicates, to be tested
//		if(pathAlreadyExists)
//			return null;
		
			return new VertexMessageSteps(_queryId,_vertexId,"V:" + _vertexId, _steps, _vertexId,_steps, getSubgraph().getSubgraphId().get() , 0);
		
	}
	
	
	
	//Getting identifier for a query
	//TODO:change this when implementing concurrent queries
	private long getQueryId() {
		
		return 1;
	}
	
	
	//(*********** UTILITY FUNCTIONS**********
	
	
	/**
	 * Utility function to compare two values
	 * 
	 */
	private boolean compareValuesUtil(Object o,Object currentValue){
		if( o.getClass().getName() != currentValue.getClass().getName()){return false;}
		if (o instanceof Float){
			return ((Float)o).equals(currentValue);
		}
		else if (o instanceof Double){
			return ((Double)o).equals(currentValue);
		}
		else if (o instanceof Integer){
			return ((Integer)o).equals(currentValue);
		}
		else{
			return ((String)o).equals(currentValue);
		}
		
	}
	






// Aggregating results
	@Override
	public void wrapup() { 
	  if(!queryEnd){
	    queryEnd=true;
	  LOG.info("Ending Query Execution");
	  }
		for(Map.Entry<Long, ResultSet> entry: getSubgraph().getSubgraphValue().resultsMap.entrySet()) {
			if (!entry.getValue().revResultSet.isEmpty())
				for(String partialRevPath: entry.getValue().revResultSet) {
					if (!entry.getValue().forwardResultSet.isEmpty())
						for(String partialForwardPath: entry.getValue().forwardResultSet) {
							LOG.info("ResultSetBothNotEmpty:" +partialRevPath+partialForwardPath);
							//output(partition.getId(), subgraph.getId(), partialRevPath+partialForwardPath); 
						}
					else{
						LOG.info("ResultSetForwardEmpty:" +partialRevPath);
						//output(partition.getId(), subgraph.getId(), partialRevPath);
					}
				}
			else
				for(String partialForwardPath: entry.getValue().forwardResultSet) {
					LOG.info("ResultSetReverseEmpty:" +partialForwardPath);
					//output(partition.getId(), subgraph.getId(), partialForwardPath); 
				}
		}
	LOG.info("Cumulative Result Collection:" +  getSubgraph().getSubgraphValue().resultCollectionTime);	
		clear();
	}

	public void clear(){
	  getSubgraph().getSubgraphValue().Arguments=null;
	  getSubgraph().getSubgraphValue().forwardLocalVertexList.clear();
	  getSubgraph().getSubgraphValue().forwardRemoteVertexList.clear();
	  getSubgraph().getSubgraphValue().revLocalVertexList.clear();
	  getSubgraph().getSubgraphValue().revRemoteVertexList.clear();
	  getSubgraph().getSubgraphValue().MessagePacking.clear();
	  getSubgraph().getSubgraphValue().noOfSteps=0;
	  getSubgraph().getSubgraphValue().outputList.clear();
	  getSubgraph().getSubgraphValue().outputPathMaintainance.clear();
	  getSubgraph().getSubgraphValue().partialResultCache.clear();
	  getSubgraph().getSubgraphValue().recursivePaths.clear();
	  getSubgraph().getSubgraphValue().resultsMap.clear();
	  getSubgraph().getSubgraphValue().path.clear();
	  getSubgraph().getSubgraphValue().queryCostHolder=null;
	  getSubgraph().getSubgraphValue().startPos=0;
	  queryMade=false;
	  queryStart=false;
	
	}




	
	
    
    
}

