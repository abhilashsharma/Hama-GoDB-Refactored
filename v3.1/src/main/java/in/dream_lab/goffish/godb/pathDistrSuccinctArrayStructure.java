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
import org.apache.hama.commons.math.Tuple;
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

import edu.berkeley.cs.succinct.buffers.SuccinctBuffer;
import in.dream_lab.goffish.api.AbstractSubgraphComputation;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraphWrapup;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.Step.Direction;
import in.dream_lab.goffish.godb.Step.Type;
import in.dream_lab.goffish.godb.pathDistrSuccinctIndex.VertexMessageSteps;
import in.dream_lab.goffish.hama.succinctstructure.SuccinctArraySubgraph;
import in.dream_lab.goffish.hama.succinctstructure.SuccinctArrayVertex;
import in.dream_lab.goffish.hama.succinctstructure.SuccinctSubgraph;
import in.dream_lab.goffish.hama.succinctstructure.SuccinctVertex;



public class pathDistrSuccinctArrayStructure extends
AbstractSubgraphComputation<pathDistrSubgraphSuccinctArrayStructureState, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable> 
implements ISubgraphWrapup{
	
	public pathDistrSuccinctArrayStructure(String initMsg) {
		// TODO Auto-generated constructor stub
		Arguments=initMsg;
	}
	
	public static final Log LOG = LogFactory.getLog(pathDistrSuccinctArrayStructure.class);
	
	long selectivity=0;
	public ArrayList<String> delimArray= new ArrayList<>();
	public ArrayList<String> propArray= new ArrayList<>();
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
	//for succinct
	HashMap<String,Integer> propToIndex= new HashMap<String,Integer>();
	public  List<Long> hitList;
	//Local subgraph to succint buffer mapping
	static HashMap<Long,SuccinctBuffer> subgraphToBuffer;
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
		//CitPatent Schema
//		propToIndex.put("patid", 0);
//    	propToIndex.put("country", 1);
//    	propToIndex.put("nclass", 2);
		
		//RGraph schema
		propToIndex.put("vid", 0);
		propToIndex.put("lang", 1);
    	propToIndex.put("ind", 2);
    	propToIndex.put("contr", 3);
    	propToIndex.put("ispublic", 4);
		propToIndex.put("follow", 5);
		
    	//gplus Schema
//    	propToIndex.put("vid", 0);
//    	propToIndex.put("employer", 1);
//    	propToIndex.put("school", 2);
//    	propToIndex.put("major", 3);
//    	propToIndex.put("places_lived", 4);
		
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
		
		//add properties and delimiter list
		
		//for RGraph
		propArray.add("lang");
		propArray.add("ind");
		propArray.add("contr");
		propArray.add("ispublic");
		propArray.add("follow");//added for Rgraph...remove for gplus
		
		delimArray.add("@");
		delimArray.add("$");
		delimArray.add("*");
		delimArray.add("^");
		delimArray.add("%");
		delimArray.add("|");
		
		
		//for gplus
//		propArray.add("employer");
//		propArray.add("school");
//		propArray.add("major");
//		propArray.add("places_lived");
//		
//		delimArray.add("@");
//		delimArray.add("$");
//		delimArray.add("*");
//		delimArray.add("^");
//		delimArray.add("|");
		
		
		 SuccinctArraySubgraph sg=(SuccinctArraySubgraph)getSubgraph();
		 
		 sg.setDelimArray(delimArray);
		 sg.setPropArray(propArray);

		
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
//		 LOG.info("Joining:"+ queryId+","+step+","+direction+","+endVertexId+"," + getSubgraph().getSubgraphId().get());
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

	
	
	void initNull() {
		initDone=true;
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
		SuccinctArraySubgraph sg=(SuccinctArraySubgraph)getSubgraph();
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
							      initNull();
						}
					}catch(Exception e){e.printStackTrace();}
					
				}
			}
			
	

		}//STATIC PROCESS ENDED
		
		// RUNTIME FUNCTIONALITITES 
		{
			// COMPUTE-LOAD-INIT
			if(getSuperstep()==1){
			        if(!queryStart){
			        queryStart=true;  
				LOG.info("Starting Query Execution");
				 queryEnd=false;
			        }

				
				
				
				
				// LOAD START VERTICES
				{
				       
					
					
					String currentProperty = null;
					Object currentValue = null;
//					startPos=0;//used for debugging
					currentProperty = getSubgraph().getSubgraphValue().path.get(0).property; 
					currentValue = getSubgraph().getSubgraphValue().path.get(0).value;
					
					// TODO: check if the property is indexed** uncomment this if using indexes
					long QueryId=getQueryId();
					
					try{
						{
							
								queryMade=true;
//								LOG.info("Querying start");
								hitList=sg.getVertexByProp(currentProperty, (String)currentValue, '@');
//								LOG.info("Querying end");
							
						}
						
					System.out.println("Starting Vertices:" +hitList.size());	
					
					
						if(hitList.size()>0){
							LOG.info("Index Querying Processing");
							for (int i=0;i< hitList.size();i++){

								long vid= hitList.get(i);
//								if ( getSubgraph().getSubgraphId().get() ==hitList.get(i)){
//									System.out.println("GOT:"+ vid);
									Long _vertexId = vid;
									String _message = "V:"+String.valueOf(_vertexId);
									
									
									  getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(QueryId,_vertexId,_message, getSubgraph().getSubgraphValue().startPos, _vertexId,getSubgraph().getSubgraphValue().startPos, getSubgraph().getSubgraphId().get(), 0) );
									
										
//									getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(_vertexId,_message,0) );
//								}//subgraph checking
							}
							LOG.info("Index Querying Processing Done");
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
			
			
			// CHECK MSSG-PROCESS FORWARD-PROCESS BACKWARD
			if(getSuperstep()>=1) {
			
				LOG.info("Started Query Traversal");
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
			while(!getSubgraph().getSubgraphValue().forwardLocalVertexList.isEmpty()) {
				VertexMessageSteps vertexMessageStep = getSubgraph().getSubgraphValue().forwardLocalVertexList.poll();
				selectivity++;
				//output(partition.getId(), subgraph.getId(), "FORWARD-LIST");
				/* if last step,end that iteration*/
				//System.out.println("Reached:" + vertexMessageStep.startVertexId + " Path Size:" + vertexMessageStep.stepsTraversed + "/" + (path.size()-1));
				if ( vertexMessageStep.stepsTraversed == getSubgraph().getSubgraphValue().path.size()-1 ){
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
				
				Step nextStep = getSubgraph().getSubgraphValue().path.get(vertexMessageStep.stepsTraversed+1);
				
				
				SuccinctArrayVertex<MapValue,MapValue,LongWritable,LongWritable> currentVertex = new SuccinctArrayVertex(new LongWritable(vertexMessageStep.vertexId),sg.getVertexBufferList(),sg.getEdgeBufferList(),'|');
				
				if( nextStep.type == Type.EDGE ) {
					
					if ( nextStep.direction == Direction.OUT ) {
						/* null predicate handling*/
						//int count=0;
						boolean flag=false;
						boolean addFlag=false;
						if ( nextStep.property == null && nextStep.value == null ) {
							Tuple<List<Long>,List<Long>> edges= currentVertex.getEdges();
							
							//iterating over local sinks
							for( long edge: edges.getFirst()) {
								//count++;
//								System.out.println("Traversing edges");
								Long otherVertex = edge;
								StringBuilder _modifiedMessage = new StringBuilder("");
								_modifiedMessage.append(vertexMessageStep.message).append("-->E:").append("-->V:").append(otherVertex);
							
//									System.out.println("Path Till Now:" + _modifiedMessage.toString());
									getSubgraph().getSubgraphValue().forwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
									
							}//localoutedges traversal end
							
							
							
							//iterating over remote sinks
							for( long edge: edges.getSecond()) {
								
								
								Long otherVertex = edge;
								StringBuilder _modifiedMessage = new StringBuilder("");
								_modifiedMessage.append(vertexMessageStep.message).append("-->E:").append("-->V:").append(otherVertex);
							
								if(!flag){
								addFlag=StoreRecursive(vertexMessageStep,_modifiedMessage.toString(),true);	
								
								flag=true;
								}
								
								if(addFlag){
									getSubgraph().getSubgraphValue().forwardRemoteVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1,vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed+1, vertexMessageStep.previousSubgraphId,vertexMessageStep.previousPartitionId));
								}
								
							}//remoteoutedges traversal end
							
							
						}
					
					}

					
				}
				else if ( nextStep.type == Type.VERTEX ) {
					
					/* null predicate*/
					if( nextStep.property == null && nextStep.value == null ) {
						/* add appropriate value later*/
						getSubgraph().getSubgraphValue().forwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
						//forwardLocalVertexList.add(vertexMessageStep);
					}
					/* filtered vertex*/
					else {
//						System.out.println("CurrentVertex:" + currentVertex  + "," + propToIndex.get(nextStep.property));
						if ( compareValuesUtil(String.valueOf(currentVertex.getPropforVertex(propToIndex.get(nextStep.property))), nextStep.value.toString()) ) {
							/* add appropriate value later*/
							getSubgraph().getSubgraphValue().forwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
							//forwardLocalVertexList.add(vertexMessageStep);
						}
					}
					
				}
				
			}
			
			LOG.info("Sending Remote Messages:" + getSubgraph().getSubgraphValue().forwardRemoteVertexList.size() + "," + getSubgraph().getSubgraphValue().revRemoteVertexList.size());
			// TODO: send the messages in Remote vertex list
			for(VertexMessageSteps stuff: getSubgraph().getSubgraphValue().forwardRemoteVertexList){
				// send message to all the remote vertices
				
//				IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)getSubgraph().getVertexById(new LongWritable(stuff.vertexId));
				StringBuilder remoteMessage = new StringBuilder("for();");
				//remoteMessage.append(String.valueOf(stuff.vertexId.longValue())).append(";").append(stuff.message).append(";").append(stuff.stepsTraversed) ;
				
				remoteMessage.append(String.valueOf(stuff.startVertexId)).append(";").append(String.valueOf(stuff.previousSubgraphId)).append(";").append(stuff.previousPartitionId).append(";").append(stuff.vertexId).append(";").append(stuff.stepsTraversed).append(";").append(sg.getRemoteMap().get(stuff.vertexId).toString());
					
				remoteMessage.append(";").append(stuff.queryId);
				Text remoteM = new Text(remoteMessage.toString());
//				LOG.info("RemoteMap:" + (long)sg.getRemoteMap().get(stuff.vertexId) + "," +stuff.vertexId);
				sendMessage(new LongWritable((long) sg.getRemoteMap().get(stuff.vertexId)),remoteM);

					
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
			LOG.info("Remote Messages Sent");
		}
		}
		
		LOG.info("Ending Query Traversal");
		LOG.info("Compute Ends");
		if(getSuperstep()>=1)
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
//		LOG.info("Storing Recursive:" + vertexMessageStep.queryId+","+vertexMessageStep.stepsTraversed+","+_direction+","+vertexMessageStep.vertexId);
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
	  long resultSetSize=0;
		for(Map.Entry<Long, ResultSet> entry: getSubgraph().getSubgraphValue().resultsMap.entrySet()) {
			if (!entry.getValue().revResultSet.isEmpty())
				for(String partialRevPath: entry.getValue().revResultSet) {
					if (!entry.getValue().forwardResultSet.isEmpty())
						for(String partialForwardPath: entry.getValue().forwardResultSet) {
							LOG.info("ResultSetBothNotEmpty:" +partialRevPath+partialForwardPath);
							resultSetSize++;
							//output(partition.getId(), subgraph.getId(), partialRevPath+partialForwardPath); 
						}
					else{
						LOG.info("ResultSetForwardEmpty:" +partialRevPath);
						resultSetSize++;
						//output(partition.getId(), subgraph.getId(), partialRevPath);
					}
				}
			else
				for(String partialForwardPath: entry.getValue().forwardResultSet) {
					LOG.info("ResultSetReverseEmpty:" +partialForwardPath);
					resultSetSize++;
					//output(partition.getId(), subgraph.getId(), partialForwardPath); 
				}
		}
		 if(resultSetSize!=0){
	          LOG.info(Arguments+"$ResultSetSize:" + resultSetSize);
	          }

	          if(selectivity>0){
	          	LOG.info("SELECTIVITY:" + selectivity);
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

