package in.dream_lab.goffish.godb.path;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
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
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.ISubgraphWrapup;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.ConfigFile;
import in.dream_lab.goffish.godb.EdgeAttr;
import in.dream_lab.goffish.godb.Hueristics;
import in.dream_lab.goffish.godb.HueristicsLoad;
import in.dream_lab.goffish.godb.MapValue;
import in.dream_lab.goffish.godb.Path;
import in.dream_lab.goffish.godb.PathWithDir;
import in.dream_lab.goffish.godb.ResultSet;
import in.dream_lab.goffish.godb.Step;
import in.dream_lab.goffish.godb.Step.Direction;
import in.dream_lab.goffish.godb.Step.Type;
import in.dream_lab.goffish.godb.reach.ReachMessage;
import in.dream_lab.goffish.godb.path.DoPath.OutputPathKey;
import in.dream_lab.goffish.godb.path.DoPath.Pair;
import in.dream_lab.goffish.godb.path.DoPath.RecursivePathKey;
import in.dream_lab.goffish.godb.path.DoPath.RecursivePathMaintained;
import in.dream_lab.goffish.godb.path.PathMessage.InEdgesWriter;
import in.dream_lab.goffish.godb.path.PathMessage.ResultsReader;
import in.dream_lab.goffish.godb.path.PathMessage.ResultsWriter;
import in.dream_lab.goffish.godb.path.PathMessage.RevisitTraversalReader;
import in.dream_lab.goffish.godb.path.PathMessage.RevisitTraversalWriter;
import in.dream_lab.goffish.godb.path.PathMessage.ResultsReader.OutputReader;
import in.dream_lab.goffish.godb.path.PathMessage.RevisitTraversalReader.TraversalMsg;
import in.dream_lab.goffish.godb.path.TraversalStep.TraversalWithState;


public class DoTest extends
        AbstractSubgraphComputation<PathStateTest, MapValue, MapValue, PathMessage, LongWritable, LongWritable, LongWritable>
        implements ISubgraphWrapup {

	public static final Log LOG = LogFactory.getLog(DoTest.class);
	//Clear this at the end of the query
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
        Map<Long, ResultsWriter> remoteResultsMap = new HashMap<>();
        long time;
        class Pair{
                Long endVertex;
        Long prevSubgraphId;
//        Integer prevPartitionId;    
        public Pair(Long _endVertex,Long _prevSubgraphId) {
                this.endVertex=_endVertex;
            this.prevSubgraphId = _prevSubgraphId;
//            this.prevPartitionId = _prevPartitionId;
        }
    }
                
                class RecursivePathMaintained{
                        Long startVertex;
                        Integer startStep;
                        PathWithDir path;
                        int direction=0;
                        
                        public int hashCode(){
                                return (int)(startStep+direction+startVertex + path.hashCode());
                        }
                        
                        public boolean equals(Object obj){
                                RecursivePathMaintained other=(RecursivePathMaintained)obj;
                                return (this.direction==other.direction && this.startStep.intValue()==other.startStep.intValue() && this.startVertex.longValue()==other.startVertex.longValue() && this.path.equals(other.path));
                        }
                        
                        
                        public RecursivePathMaintained(Long _startVertex,Integer _startStep, PathWithDir _path, int _direction){
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
        
        
        
	// FIXME: We're copying this to the subgraph state in sstep 0. Is that fine?
	String queryParam;
	Double networkCoeff=49.57;
	Hueristics hueristics=null;
	/**
	 * Initialize BFS query with query string
	 * 
	 * @param initMsg
	 */
	public DoTest(String initMsg) {
		queryParam=initMsg;
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
         * Initialize the class variables
         * This method is called in first superstep, it parses the query passed.
         * It also reads the Graph statistics(Called as Heuristics) from disk
         */
        private void init(){
                PathStateTest state=getSubgraph().getSubgraphValue();
                String arguments = queryParam;
                state.Arguments=queryParam;
          
        
                
            state.path = new ArrayList<Step>();
                Type previousStepType = Type.EDGE;
                for(String _string : arguments.split(Pattern.quote("//"))[0].split(Pattern.quote("@")) ){
                        if(_string.contains("?")){
                                if(previousStepType == Type.EDGE)
                                        state.path.add(new Step(Type.VERTEX,null, null, null));
                                previousStepType = Type.EDGE;
                                String[] _contents = _string.split(Pattern.quote("?")); 
                                String p = null ;
                                Object v = null ;
                                Direction d = (_contents[0].equals("out") ) ? Direction.OUT : Direction.IN;
                                if ( _contents.length > 1 )     {
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
                                state.path.add(new Step(Type.EDGE, d, p, v));
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

                                state.path.add(new Step(Type.VERTEX,null, p, v));
                        }
                        

                }
                if(previousStepType == Type.EDGE){
                        state.path.add(new Step(Type.VERTEX,null, null, null));
                }
                state.noOfSteps = state.path.size();
                state.queryCostHolder = new Double[state.noOfSteps];
                for (int i = 0; i < state.queryCostHolder.length; i++) {
                        state.queryCostHolder[i] = new Double(0);
                        
                }
                state.forwardLocalVertexList = new LinkedList<TraversalWithState>();
                state.revLocalVertexList = new LinkedList<TraversalWithState>();
//              inVerticesMap = new HashMap<Long, HashMap<String,LinkedList<Long>>>();
//              remoteSubgraphMap = new HashMap<Long, Long>();
//              hueristics=HueristicsLoad.getInstance();//loading this at a different place

                
        }




	////////////////////////////////////////////////////////////////
	// SUPERSTEP 0
	//
	////////////////////////////////////////////////////////////////
	/**
	 * Parse the query
	 * Initialize the state data structures
	 * Initialize the Lucene index
	 * Load Heuristics
	 * Create Inedges
	 */
	private void doSuperstep0() {
		ISubgraph<PathStateTest, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
		PathStateTest state = subgraph.getSubgraphValue();

//		state.rootQuerier = rootQuerier;

		if (queryParam == null) {
			throw new RuntimeException("Invalid input query. Found NULL");
		}

		// Parse and load queries
		// TODO: Should this be part of application superstep to include its timing?
		if (LOG.isInfoEnabled()) LOG.info("***************ARGUMENTS************** :" + queryParam);
//		state.query = new ReachQuery(queryParam);

		init();
                // load index
                try{
                        synchronized (initLock) {
                                if ( !initDone )
                                      initInMemoryLucene();
                        }
                }catch(Exception e){e.printStackTrace();}
		
		//Load Heuristics
		hueristics=HueristicsLoad.getInstance();
		
		//create InEdges
		if(state.InEdges==null){
		  createInEdges();
		}
	}
	
	/**
	 * create InEdges
	 */
	private void createInEdges(){
	  //Logic to Accumulate inedges
          getSubgraph().getSubgraphValue().InEdges=new HashMap<Long,HashMap<Long,EdgeAttr>>();  
    

        
        String m="";
        
        for(IVertex<MapValue, MapValue, LongWritable, LongWritable> sourceVertex:getSubgraph().getLocalVertices())
        for(IEdge<MapValue, LongWritable, LongWritable> edge : sourceVertex.getOutEdges()) {
                
                IVertex<MapValue, MapValue, LongWritable, LongWritable> sinkVertex=getSubgraph().getVertexById(edge.getSinkVertexId());
//              LOG.info("VERTEX:" + sinkVertex.getVertexId().get());
        //if sink vertex is not remote then add inedge to appropriate data structure, otherwise send source value to remote partition
        if(!sinkVertex.isRemote())
        {
 
                if(getSubgraph().getSubgraphValue().InEdges.containsKey(sinkVertex.getVertexId().get()))
                {
                        if(!getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).containsKey(sourceVertex.getVertexId().get()))
                        {
                                
                                
//                            ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
                                EdgeAttr attr= new EdgeAttr("relation","null" /*subgraphProperties.getValue("relation").toString()*/,edge.getEdgeId().get(),false,null);
                                getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).put(sourceVertex.getVertexId().get(), attr);
                                //System.out.println("Accumulation inedge for edge "+ edge.getId() + " Value " + subgraphProperties.getValue("relation").toString() );
                        }
                        
                }
                else
                {
//                    ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
                        EdgeAttr attr= new EdgeAttr("relation", "null"/*subgraphProperties.getValue("relation").toString()*/,edge.getEdgeId().get(),false,null);                                
                        getSubgraph().getSubgraphValue().InEdges.put(sinkVertex.getVertexId().get(), new HashMap<Long,EdgeAttr>());
                        getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).put(sourceVertex.getVertexId().get(), attr);
                        //System.out.println("Accumulation inedge for edge "+ edge.getId() + " Value " + subgraphProperties.getValue("relation").toString() );
                        
                }
                
                //System.out.println(edge.getSource().getId() + " -->" + edge.getSink().getId());
        }
        else
        { //send message to remote partition
        
        //TODO: generalize this for all attributes
                IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)sinkVertex;
                remoteVertex.getSubgraphId().get();
        if(!getSubgraph().getSubgraphValue().MessagePacking.containsKey(remoteVertex.getSubgraphId().get()))
                getSubgraph().getSubgraphValue().MessagePacking.put(remoteVertex.getSubgraphId().get(),new StringBuilder("#|" + sourceVertex.getVertexId().get()  + "|" + sinkVertex.getVertexId().get() + "|" + "relation" + ":"  +"null" /*subgraphProperties.getValue("relation").toString()*/+"|" + edge.getEdgeId().get()+"|" + getSubgraph().getSubgraphId().get() + "|" +0));
        else{
                getSubgraph().getSubgraphValue().MessagePacking.get(remoteVertex.getSubgraphId().get()).append("$").append("#|").append(sourceVertex.getVertexId().get()).
                                                        append("|").append(sinkVertex.getVertexId().get()).
                                                        append("|").append("relation").append(":").append("null" /*subgraphProperties.getValue("relation").toString()*/).
                                                        append("|").append(edge.getEdgeId().get()).
                                                        append("|").append(getSubgraph().getSubgraphId().get()).
                                                        append("|").append(0);
                
        }
        
        
        
        }
        
        
        
        
        
}

//Sending packed messages by iterating through MessagePacking Hashmap
for(Map.Entry<Long,StringBuilder> remoteSubgraphMessage: getSubgraph().getSubgraphValue().MessagePacking.entrySet()){
        
        InEdgesWriter in= new InEdgesWriter(remoteSubgraphMessage.getValue().toString().getBytes());
        PathMessage msg = new PathMessage(in);

        sendMessage(new LongWritable(remoteSubgraphMessage.getKey()),msg);
}
	  
	}
	
	/**
	 * Accumulate Inedges
	 */
	private void doSuperstep1(Iterable<IMessage<LongWritable, PathMessage>> messageList) {
	 
    for (IMessage<LongWritable, PathMessage> _message: messageList){
            
            String message = new String(_message.getMessage().getInEdgesReader().getInEdgesMessage());

            
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
	}
	
	
	/**
         * When there is match for a path and parent subgraph is not the current subgraph then this method is used to send partial results back to parent subgraph. 
         * @throws IOException 
         * 
         */
        private void forwardOutputToSubgraph(int direction,TraversalWithState step) throws IOException {
                PathStateTest state=getSubgraph().getSubgraphValue();
                boolean d=false;
                if(direction==1){
                        d=true;
                }
                long Time=System.currentTimeMillis();
                
//              System.out.println("OUTPUT SIZE:"+step.startVertexId+":"+step.message+":"+state.outputPathMaintainance.get(new OutputPathKey(step.queryId,step.startStep,d,step.startVertexId)).size());
                
                for (Pair entry: state.outputPathMaintainance.get(new OutputPathKey(step.queryId,step.depth,d,step.startVertex) )){
                        
                         ResultsWriter remoteResults = remoteResultsMap.get(step.previousSubgraph);
                         if (remoteResults == null) {
                                 remoteResults = new ResultsWriter();
                                 remoteResultsMap.put(step.previousSubgraph, remoteResults);
                         }
                         
                         remoteResults.addResult(step.queryId, step.startDepth, entry.endVertex, d,step.path);

                }
                
                Time=System.currentTimeMillis()-Time;
                state.resultCollectionTime+=Time;
                
        }
	
	
	/**
         * When getting a message that contains partial output, this method merges the partial output from the message and partial path stored currently. 
         * The Merged output may be partial output that needs to be sent back further to its parent subgraph, that is checked by checking  'outputPathMaintainance' if there is an entry.
         * @throws IOException 
         */
        private void join(long queryId,OutputReader o) throws IOException {
                PathStateTest state=getSubgraph().getSubgraphValue();
                long Time=System.currentTimeMillis();
                PathWithDir partialPath = o.path;
//              System.out.println("RECEIVED JOIN MESSAGE:" +message);
//              String[] split = message.split(Pattern.quote(";"));
                boolean direction =  o.dir;
                Long endVertexId = o.previousVertex;
                
                Integer step=o.startDepth;
                //Recently added line...Reminder
                step=direction?step-1:step+1;
                for (RecursivePathMaintained stuff : state.recursivePaths.get(new RecursivePathKey(queryId, step, direction,endVertexId))){
                        PathWithDir result = new PathWithDir(partialPath.startVertex,partialPath.path);//partial result
                        //*****Adding partial Result to partialResultCache********
                        
//                      if(!partialResultCache.containsKey(new RecursivePathKey(queryId, step, direction,endVertexId))){
//                              List<String> pathList=new ArrayList<String>();
//                              pathList.add(split[4]);
//                              partialResultCache.put(new RecursivePathKey(queryId, step, direction,endVertexId),pathList);
//                      }
//                      else{
//                              partialResultCache.get(new RecursivePathKey(queryId, step, direction,endVertexId)).add(split[4]);
//                      }
                        
                        //***************************END***************************
                        
                        if (direction)
                                result.insert(stuff.path);
                        else
                                result.append(stuff.path);
                        
//                      System.out.println("END:" + endVertexId + "PATH:"+stuff.path + "Merged path:" + result);
                        Integer recursiveStartStep=stuff.startStep;
                        boolean recursion=false;
//                      System.out.println("JoinQuery:" + queryId+","+ recursiveStartStep+","+ direction+","+ stuff.startVertex);
                        
                        if ( state.outputPathMaintainance.containsKey(new OutputPathKey(queryId, recursiveStartStep, direction, stuff.startVertex)))
                        {
                                
                                for ( Pair entry: state.outputPathMaintainance.get(new OutputPathKey(queryId,recursiveStartStep,direction,stuff.startVertex))){
                                        
                                        ResultsWriter remoteResults = remoteResultsMap.get(entry.prevSubgraphId);
                                         if (remoteResults == null) {
                                                 remoteResults = new ResultsWriter();
                                                 remoteResultsMap.put(entry.prevSubgraphId, remoteResults);
                                         }
                                         
                                         remoteResults.addResult(queryId, recursiveStartStep, entry.endVertex, o.dir,result);
                                }
                                recursion=true;
                        }
                                                
                        if(!recursion){
                                if ( !state.resultsMap.containsKey(stuff.startVertex) )
                                        state.resultsMap.put(stuff.startVertex, new ResultSet());
                                if ( direction ) 
                                        state.resultsMap.get(stuff.startVertex).forwardResultSet.add(result.toString());
                                else
                                        state.resultsMap.get(stuff.startVertex).revResultSet.add(result.toString());
                        }
                        
                }
                                                
                Time=System.currentTimeMillis()-Time;
                state.resultCollectionTime+=Time;
                                                
        }



        /**
         * Process an incoming remote message, creates appropriate entry in 'outputPathMaintainance',
         * which may be required for recursive aggregation of partial results.
         * 
         */
        private TraversalWithState processMessage(TraversalMsg message){
                
                PathStateTest state=getSubgraph().getSubgraphValue();
                //TODO:add queryid to vertex message step
//              String message = _message.getMessage().toString();
//              System.out.println("RECVD REMOTE MSG:" + message);
//              String[] split = message.split(Pattern.quote(";"));
                boolean _dir=message.dir;
                
                Long _endVertexId = message.previousVertex;
                Long _previousSubgraphId = message.previousSubgraph;
//              Integer _previousPartitionId = Integer.parseInt( split[3] );
                Long _vertexId =message.targetVID;
                Integer _steps = message.depth;
                //Recent change....Reminder:DONE
                //_steps=_dir?_steps+1:_steps-1;
                Long _queryId=message.queryId;
                //adding to the recursive path maintenance
                boolean pathAlreadyExists=false;
//                      System.out.println("Storing PATH:" + _queryId + "," + _steps + "," + _dir + "," + _vertexId+"," + _endVertexId);
                        
                        if(state.outputPathMaintainance.containsKey(new OutputPathKey(_queryId,_steps,_dir,_vertexId))){
                                state.outputPathMaintainance.get(new OutputPathKey(_queryId,_steps,_dir,_vertexId)).add( new Pair(_endVertexId,_previousSubgraphId));
                                pathAlreadyExists=true;
                
                }else{
                        
                        List<Pair> tempList=new ArrayList<Pair>();
                        tempList.add(new Pair(_endVertexId,_previousSubgraphId));
                        
                        state.outputPathMaintainance.put(new OutputPathKey(_queryId,_steps,_dir,_vertexId),tempList);
                }
                
//                      //Debug code
//                      TraversalWithState v=new TraversalWithState(_queryId,_vertexId,"V:" + _vertexId, _steps, _vertexId,_steps, subgraph.getId() , partition.getId());
//                      if(v.startVertexId==5461450)
//                              System.out.println("SToring VertexMessageStep:" + v.queryId+"," +v.startStep +"," +_dir+","+ v.startVertexId);
//                      //end of Debug code
                        
                        
                        //FIX: new code to remove duplicates, to be tested
                if(pathAlreadyExists)
                        return null;
                
                        return new TraversalWithState(_queryId, message.rooSubgraph, message.rootVertex, message.targetVID, message.previousSubgraph, message.depth, message.targetVID, message.depth, new PathWithDir(message.targetVID));
                
        }
	
	
        /**
         * This function takes current vertex message step and stores the partial path, done before sending message to remote subgraph 
         * 
         */
        private boolean StoreRecursive(TraversalWithState vertexMessageStep,PathWithDir _modifiedPath,boolean _direction) {
                PathStateTest state=getSubgraph().getSubgraphValue();
                boolean flag=true;
                
                int dir=0;
                if(_direction==true){
                        dir=1;
                }
                
                if(!state.recursivePaths.containsKey(new RecursivePathKey(vertexMessageStep.queryId,vertexMessageStep.depth,_direction,vertexMessageStep.targetVertex))){
                        
                        ArrayList<RecursivePathMaintained> tempList = new ArrayList<RecursivePathMaintained>();
                        tempList.add(new RecursivePathMaintained(vertexMessageStep.startVertex,vertexMessageStep.startDepth, _modifiedPath,dir));
                        state.recursivePaths.put(new RecursivePathKey(vertexMessageStep.queryId,vertexMessageStep.depth,_direction,vertexMessageStep.targetVertex), tempList);
//                      System.out.println(vertexMessageStep.queryId+" Storing Recursive path:"+vertexMessageStep.startVertexId+":" + vertexMessageStep.vertexId +":"+_modifiedMessage+ ":" + vertexMessageStep.startStep + ":" + vertexMessageStep.stepsTraversed + ":" + _direction);
//                      vertexMessageStep.startVertexId = vertexMessageStep.vertexId;
                }
                else{
                        if(!state.recursivePaths.get(new RecursivePathKey(vertexMessageStep.queryId,vertexMessageStep.depth,_direction,vertexMessageStep.targetVertex)).contains(new RecursivePathMaintained(vertexMessageStep.startVertex,vertexMessageStep.startDepth, _modifiedPath,dir))){
                                
                                state.recursivePaths.get(new RecursivePathKey(vertexMessageStep.queryId,vertexMessageStep.depth,_direction,vertexMessageStep.targetVertex)).add(new RecursivePathMaintained(vertexMessageStep.startVertex,vertexMessageStep.startDepth, _modifiedPath,dir));
//                              System.out.println(vertexMessageStep.queryId+" Adding Recursive path:"+vertexMessageStep.startVertexId+":" + vertexMessageStep.vertexId +":"+_modifiedMessage+ ":" + vertexMessageStep.startStep + ":" + vertexMessageStep.stepsTraversed + ":" + _direction  + ":" + !state.recursivePaths.get(new RecursivePathKey(vertexMessageStep.queryId,vertexMessageStep.stepsTraversed,_direction,vertexMessageStep.vertexId)).contains(new RecursivePathMaintained(vertexMessageStep.startVertexId,vertexMessageStep.startStep, _modifiedMessage.toString(),dir)) );
                                
                                //Checking partialResultCache if any partialResult present prior to this and sending result back
//                              
//                              if(partialResultCache.containsKey(new RecursivePathKey(vertexMessageStep.queryId, vertexMessageStep.stepsTraversed,_direction,vertexMessageStep.vertexId))){
//                                      
//                                      for(String partialResult:partialResultCache.get(new RecursivePathKey(vertexMessageStep.queryId, vertexMessageStep.stepsTraversed,_direction,vertexMessageStep.vertexId))){
//                                              
//                                              StringBuilder result =new StringBuilder(partialResult);
//                                              if (_direction)
//                                                      result.insert(0, vertexMessageStep.message);
//                                              else
//                                                      result.append(vertexMessageStep.message);
//                                              
//                                              System.out.println("END:" + vertexMessageStep.vertexId + "PATH:"+vertexMessageStep.message + "Merged path:" + result);
//                                              Integer recursiveStartStep=vertexMessageStep.startStep;
//                                              boolean recursion=false;
//                                              System.out.println("partialJoinQuery:" + vertexMessageStep.queryId +","+ recursiveStartStep+","+ _direction+","+ vertexMessageStep.startVertexId);
//                                              
//                                              if ( outputPathMaintainance.containsKey(new OutputPathKey(vertexMessageStep.queryId, recursiveStartStep, _direction, vertexMessageStep.startVertexId)))
//                                              {
//                                                      
//                                                      for ( Pair entry: outputPathMaintainance.get(new OutputPathKey(vertexMessageStep.queryId,recursiveStartStep,_direction,vertexMessageStep.startVertexId))){
//                                                              StringBuilder remoteMessage = new StringBuilder("output();");
//                                                              if(_direction)
//                                                                      remoteMessage.append("for();");
//                                                              else
//                                                                      remoteMessage.append("rev();");
//                                                              remoteMessage.append(entry.endVertex.toString()).append(";");
//                                                              remoteMessage.append(entry.prevSubgraphId.toString()).append(";");
//                                                              remoteMessage.append(result).append(";").append(vertexMessageStep.queryId).append(";").append(recursiveStartStep);
//                                                              SubGraphMessage remoteM = new SubGraphMessage(remoteMessage.toString().getBytes());
//                                                              
//                                                              System.out.println("Sending partialJOIN Message:" + remoteMessage.toString());
//                                                              outputList.add(new OutputMessageSteps(remoteM, entry.prevPartitionId));
//                                                      }
//                                                      recursion=true;
//                                              }
//                                                                      
//                                              System.out.println("Adding result");
//                                              if(!recursion){
//                                                      if ( !resultsMap.containsKey(vertexMessageStep.startVertexId) )
//                                                              resultsMap.put(vertexMessageStep.startVertexId, new ResultSet());
//                                                      if ( _direction ) 
//                                                              resultsMap.get(vertexMessageStep.startVertexId).forwardResultSet.add(result.toString());
//                                                      else
//                                                              resultsMap.get(vertexMessageStep.startVertexId).revResultSet.add(result.toString());
//                                              }
//                                              
//                                              
//                                      }
//                              }
                                
                                //********************END*************************
                                
                                
                        }
                        
                        
                        
                        flag=false;
                }
                //*******************************END*******************************
                
                return flag;
        }



        
        
        

////////////////////////////////////////////////////////////////
// SUPERSTEP N
//
////////////////////////////////////////////////////////////////
  @Override
	public void compute(Iterable<IMessage<LongWritable, PathMessage>> messages) throws IOException {

		ISubgraph<PathStateTest, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
		long sgid = subgraph.getSubgraphId().get();
		PathStateTest state = subgraph.getSubgraphValue();

		////////////////////////////////////////////
		// SUPERSTEP 0: LOAD QUERY AND INITIALIZE LUCENE
		////////////////////////////////////////////

		if (getSuperstep() == 0) {

			doSuperstep0();
			return;
		} // Done with sstep 0. Finishing compute. Do NOT vote to halt.

                ////////////////////////////////////////////
                // SUPERSTEP 1: ACCUMULATE REMOTE INEDGES
                ////////////////////////////////////////////
		if(getSuperstep()==1){
		        doSuperstep1(messages);
		        return;
		}
		
		
	              // RUNTIME FUNCTIONALITITES 
                {
                  
                        // COMPUTE-LOAD-INIT
                        if(getSuperstep()==2){
                                if(!queryStart){
                                queryStart=true;  
                                LOG.info("Starting Query Execution");
                                 queryEnd=false;
                                }
                                // COMPUTE HUERISTIC BASED QUERY COST
                                {
                                        // TODO: implementation for calc cost from middle of query ( for each position calc cost forward and backward cost and add them)
                                        
                                        for (int pos = 0;pos < state.path.size() ; pos+=2 ){
                                                Double joinCost = new Double(0);
                                                //forward cost
                                                {       
                                                        Double totalCost = new Double(0);
                                                        Double prevScanCost = hueristics.numVertices;
                                                        Double resultSetNumber = hueristics.numVertices;
                                                        ListIterator<Step> It = state.path.listIterator(pos);
                                                        //Iterator<Step> It = path.iterator();
                                                        Step currentStep = It.next();
                                                        
                                                        while(It.hasNext()){
                                                                //cost calc
                                                                // TODO: make cost not count in probability when no predicate on edge/vertex
                                                                {
                                                                        Double probability = null;
                                                                        
                                                                        if ( currentStep.property == null )
                                                                                probability = new Double(1);
                                                                        else {
                                                                                if ( hueristics.vertexPredicateMap.get(currentStep.property).containsKey(currentStep.value.toString()) ){
                                                                                        probability = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).probability;
                                                                                        //System.out.println("Vertex Probability:" + probability);
                                                                                }       
                                                                                else {
                                                                                        totalCost = new Double(-1);
                                                                                        break;
                                                                                }
                                                                        }
                                                                        resultSetNumber *= probability;
                                                                        Double avgDeg = new Double(0);
                                                                        Double avgRemoteDeg = new Double(0);
                                                                        Step nextStep = It.next();
                                                                        if(nextStep.direction == Direction.OUT){
                                                                                if ( currentStep.property == null) {
                                                                                        avgDeg = hueristics.numEdges/hueristics.numVertices;
                                                                                        avgRemoteDeg = hueristics.numRemoteVertices/(hueristics.numVertices+hueristics.numRemoteVertices) * avgDeg;
                                                                                        //System.out.println("AVGDEG:" +avgDeg + "REMOTEAVGDEG:" + avgRemoteDeg);
                                                                                }       
                                                                                else { 
                                                                                        avgDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgOutDegree; 
                                                                                        avgRemoteDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgRemoteOutDegree;
                                                                                        //System.out.println("AVGDEG:" +avgDeg + "REMOTEAVGDEG:" + avgRemoteDeg);
                                                                                }       
                                                                        }else if(nextStep.direction == Direction.IN){
                                                                                if ( currentStep.property == null) {
                                                                                        avgDeg = hueristics.numEdges/hueristics.numVertices;
                                                                                        avgRemoteDeg = hueristics.numRemoteVertices/(hueristics.numVertices+hueristics.numRemoteVertices) * avgDeg;
                                                                                        //System.out.println("AVGDEG:" +avgDeg + "REMOTEAVGDEG:" + avgRemoteDeg);
                                                                                }       
                                                                                else { 
                                                                                        avgDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgInDegree;
                                                                                        avgRemoteDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgRemoteInDegree;
                                                                                        //System.out.println("AVGDEG:" +avgDeg + "REMOTEAVGDEG:" + avgRemoteDeg);
                                                                                }               
                                                                        }
                                                                        resultSetNumber *= (avgDeg+avgRemoteDeg); 
                                                                        Double eScanCost = prevScanCost * probability * avgDeg;
                                                                        Double networkCost = new Double(0);
                                                                        Double vScanCost = new Double(0);
                                                                        if(nextStep.property == null)
                                                                                vScanCost = eScanCost;
                                                                        else {
                                                                                //output(partition.getId(), subgraph.getId(),nextStep.property);
                                                                                //output(partition.getId(), subgraph.getId(),nextStep.value.toString());
                                                                                //output(partition.getId(), subgraph.getId(),String.valueOf(hueristics.edgePredicateMap.size()));
                                                                                //output(partition.getId(), subgraph.getId(),String.valueOf(pos));
                                                                                //output(partition.getId(), subgraph.getId(),hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability.toString());
                                                                                //System.out.println(nextStep.property+":"+nextStep.value);
                                                                                if ( hueristics.edgePredicateMap.get(nextStep.property).containsKey(nextStep.value.toString()) ) {
                                                                                        vScanCost = eScanCost * hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
                                                                                        networkCost = state.networkCoeff * prevScanCost * probability * avgRemoteDeg * hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
                                                                                        resultSetNumber *= hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
                                                                                        //System.out.println("Edge:" + hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability);
                                                                                }
                                                                                else {
                                                                                        totalCost = new Double(-1);
                                                                                        break;
                                                                                }
                                                                        }
                                                                        totalCost += (eScanCost+vScanCost+networkCost);
                                                                        prevScanCost = vScanCost;
                                                                        currentStep = It.next();
                                                                }       
                                                                                                
                                                        }
                                                        joinCost += resultSetNumber;
                                                        state.queryCostHolder[pos] = totalCost;
                                                        
//                                                      System.out.println(pos+":"+"for:"+String.valueOf(totalCost));
                                                }
                                                //reverse cost
                                                {
                                                        Double totalCost = new Double(0);
                                                        Double prevScanCost = hueristics.numVertices;
                                                        Double resultSetNumber = hueristics.numVertices;

                                                        ListIterator<Step> revIt = state.path.listIterator(pos+1);
                                                        Step currentStep = revIt.previous();
                                                        while(revIt.hasPrevious()){
                                                                // TODO: make cost not count in probability when no predicate on edge/vertex
                                                                {
                                                                        Double probability = null;
                                                                        if ( currentStep.property == null )
                                                                                probability = new Double(1);
                                                                        else {
                                                                                if ( hueristics.vertexPredicateMap.get(currentStep.property).containsKey(currentStep.value.toString()) )
                                                                                        probability = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).probability;
                                                                                else {
                                                                                        totalCost = new Double(-1);
                                                                                        break;
                                                                                }
                                                                        }
                                                                        resultSetNumber *= probability;
                                                                        Double avgDeg = new Double(0);
                                                                        Double avgRemoteDeg = new Double(0);
                                                                        Step nextStep = revIt.previous();
                                                                        if(nextStep.direction == Direction.OUT){
                                                                                if ( currentStep.property == null) {
                                                                                        avgDeg = hueristics.numEdges/hueristics.numVertices;
                                                                                        avgRemoteDeg = hueristics.numRemoteVertices/(hueristics.numVertices+hueristics.numRemoteVertices) * avgDeg;
                                                                                }
                                                                                else {
                                                                                        avgDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgInDegree; 
                                                                                        avgRemoteDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgRemoteInDegree;
                                                                                }       
                                                                        }else if(nextStep.direction == Direction.IN){
                                                                                if ( currentStep.property == null) {
                                                                                        avgDeg = hueristics.numEdges/hueristics.numVertices;
                                                                                        avgRemoteDeg = hueristics.numRemoteVertices/(hueristics.numVertices+hueristics.numRemoteVertices) * avgDeg;
                                                                                }
                                                                                else { 
                                                                                        avgDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgOutDegree;
                                                                                        avgRemoteDeg = hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgRemoteOutDegree;
                                                                                }       
                                                                        }
                                                                        resultSetNumber *= (avgDeg+avgRemoteDeg);
                                                                        Double eScanCost = prevScanCost * probability * avgDeg;
                                                                        Double vScanCost = new Double(0);
                                                                        Double networkCost = new Double(0);
                                                                        if(nextStep.property == null)
                                                                                vScanCost = eScanCost;
                                                                        else {
                                                                                if ( hueristics.edgePredicateMap.get(nextStep.property).containsKey(nextStep.value.toString()) ) {
                                                                                        vScanCost = eScanCost * hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
                                                                                        networkCost = state.networkCoeff * prevScanCost * probability * avgRemoteDeg * hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
                                                                                        resultSetNumber *= hueristics.edgePredicateMap.get(nextStep.property).get(nextStep.value.toString()).probability;
                                                                                }
                                                                                else {
                                                                                        totalCost = new Double(-1);
                                                                                        break;
                                                                                }
                                                                        }
                                                                        totalCost += (eScanCost+vScanCost);
                                                                        prevScanCost = vScanCost;
                                                                        currentStep = revIt.previous();
                                                                }
                                                        }
                                                        joinCost *= resultSetNumber;
                                                        if ( state.queryCostHolder[pos] != -1 && totalCost != -1) {
                                                                state.queryCostHolder[pos] += totalCost;
                                                                if (pos!=0 && pos!= state.path.size()-1)
                                                                        state.queryCostHolder[pos] += joinCost;
                                                        }
                                                        else
                                                                state.queryCostHolder[pos] = new Double(-1);
                                                        
                                                }
                                                /* add that extra cost of initial scan*/
                                                //TODO: Add 1 when indexed
                                                if ( state.queryCostHolder[pos] != -1 )
                                                {
                                                        if(!initDone)
                                                                state.queryCostHolder[pos] += hueristics.numVertices;
                                                        else
                                                                state.queryCostHolder[pos] +=1;
                                                                
                                                }
//                                              System.out.println(pos+":Total:"+String.valueOf(state.queryCostHolder[pos]));
                                        }
                                         
                                }
                                
                                
                                
                                
                                // LOAD START VERTICES
                                {
                                       
                                        Double minCost = state.queryCostHolder[state.startPos];
                                        boolean queryPossible = true;
                                        for (int i = 0; i < state.queryCostHolder.length ; i++) {
                                                if ( state.queryCostHolder[i]!=0 && state.queryCostHolder[i]!=-1 && state.queryCostHolder[i] < minCost ){
                                                        minCost=state.queryCostHolder[i];
                                                        state.startPos = i;
                                                }
                                                if( state.queryCostHolder[i]==-1 )
                                                        queryPossible = false;
                                        }
                                        
                                        String currentProperty = null;
                                        Object currentValue = null;
//                                      startPos=0;//used for debugging
                                        currentProperty = state.path.get(state.startPos).property; 
                                        currentValue = state.path.get(state.startPos).value;
                                        
                                        // TODO: check if the property is indexed** uncomment this if using indexes
                                        long QueryId=getQueryId();
                                        try{
                                                synchronized(queryLock){
                                                        if(!queryMade){
                                                                makeQuery(currentProperty,currentValue);
                                                        }
                                                }
                                                
//                                      System.out.println("Starting Position:" + state.startPos +"  Query min Cost:" + minCost + "   Path Size:" + state.path.size()); 
//                                      System.out.println("*******Querying done********:"+hits.length);
                                        
                                                if(hits.length>0){
                                                        for (int i=0;i<hits.length;i++){
                                                                Document doc = indexSearcher.doc(hits[i].doc);
                                                                if ( Long.valueOf(doc.get("subgraphid")) == getSubgraph().getSubgraphId().get() ){
                                                                        Long _vertexId = Long.valueOf(doc.get("id"));
                                                                        String _message = "V:"+String.valueOf(_vertexId);
//                                                                      System.out.println("STARTING VERTEX:" + _message);
                                                                        //(long sg_, long r_, long t_,long pg_,int sd_,long sv_,  int d_, Path p_)
                                                                        PathWithDir p = new PathWithDir(_vertexId);
                                                                        long qid=getQueryId();
                                                                        if ( state.startPos == 0)
                                                                          state.forwardLocalVertexList.add( new TraversalWithState(qid,getSubgraph().getSubgraphId().get(),_vertexId,_vertexId, getSubgraph().getSubgraphId().get(), state.startPos,_vertexId, state.startPos, p) );
                                                                        else
                                                                        if( state.startPos == (state.path.size()-1))
                                                                          state.revLocalVertexList.add( new TraversalWithState(qid,getSubgraph().getSubgraphId().get(),_vertexId,_vertexId, getSubgraph().getSubgraphId().get(), state.startPos,_vertexId, state.startPos, p) );
                                                                        else{
                                                                          state.forwardLocalVertexList.add( new TraversalWithState(qid,getSubgraph().getSubgraphId().get(),_vertexId,_vertexId, getSubgraph().getSubgraphId().get(), state.startPos,_vertexId, state.startPos, p) );
                                                                          state.revLocalVertexList.add( new TraversalWithState(qid,getSubgraph().getSubgraphId().get(),_vertexId,_vertexId, getSubgraph().getSubgraphId().get(), state.startPos,_vertexId, state.startPos, p) );
                                                                        }
                                                                                
//                                                                      state.forwardLocalVertexList.add( new TraversalWithState(_vertexId,_message,0) );
                                                                }
                                                        }
                                                }
                                                
                                        }catch(Exception e){e.printStackTrace();}
                                        
                
                                        // TODO : else iteratively check for satisfying vertices
//                                      if ( queryPossible == true )
//                                      for(IVertex<MapWritable, MapWritable, LongWritable, LongWritable> vertex: getSubgraph().getLocalVertices()) {
//                                              if ( vertex.isRemote() ) continue;
//                                              
//                                              if ( compareValuesUtil(vertex.getValue().get(new Text(currentProperty)).toString(), currentValue.toString()) ) {
//                                                      String _message = "V:"+String.valueOf(vertex.getVertexId().get());
//                                                      System.out.println("Vertex id:" + vertex.getVertexId().get() + "Property:"+currentProperty +" Value:" + vertex.getValue().get(new Text(currentProperty)).toString());
//                                                      if ( state.startPos == 0)
//                                                              state.forwardLocalVertexList.add( new TraversalWithState(QueryId,vertex.getVertexId().get(),_message, state.startPos, vertex.getVertexId().get(),state.startPos, getSubgraph().getSubgraphId().get(), 0) );
//                                                      else
//                                                      if( state.startPos == (state.path.size()-1))
//                                                              state.revLocalVertexList.add( new TraversalWithState(QueryId,vertex.getVertexId().get(),_message, state.startPos , vertex.getVertexId().get(),state.startPos, getSubgraph().getSubgraphId().get(), 0) );
//                                                      else{
//                                                              state.forwardLocalVertexList.add( new TraversalWithState(QueryId,vertex.getVertexId().get(),_message, state.startPos, vertex.getVertexId().get(),state.startPos, getSubgraph().getSubgraphId().get(), 0) );
//                                                              state.revLocalVertexList.add( new TraversalWithState(QueryId,vertex.getVertexId().get(),_message, state.startPos , vertex.getVertexId().get(),state.startPos, getSubgraph().getSubgraphId().get(), 0) );
//                                                      }
//                                                      //output(partition.getId(), subgraph.getId(), subgraphProperties.getValue(currentProperty).toString());
//                                              }
//                                      }
                                        
                                        
                                        Iterator msgIter=messages.iterator();
                                        while(msgIter.hasNext()){
                                          msgIter.remove();
                                        }
                                }
                                
                        }
                        
                        
                        // CHECK MSSG-PROCESS FORWARD-PROCESS BACKWARD
                        if(getSuperstep()>=2) {
                        
                                // CHECK INCOMING MESSAGE, ADD VERTEX TO APPRT LIST
                                // this is for the partially executed paths, which have been 
                                // forwarded from a different machine
                                
                                        for (IMessage<LongWritable, PathMessage> message: messages){
                                          PathMessage msg = message.getMessage();
                                          if (msg.getMessageType() == PathMessage.RESULTS_READER) {
                                            ResultsReader resultsReader = msg.getResultsReader();
                                            Map<Long, ArrayList<OutputReader>> newResults = resultsReader.getResults();
                                            
                                            for (Entry<Long, ArrayList<OutputReader>> entry : newResults.entrySet()) {
                                              
                                              for(OutputReader o: entry.getValue()){
                                                  //joining each path
                                                  join(entry.getKey(),o);
                                              }
                                            }
                                            
                                          }
                                          else if (msg.getMessageType() == PathMessage.REVISIT_TRAVERSAL_READER) {
                                                RevisitTraversalReader resultsReader = msg.getRevisitTraversalReader();
                                                List<TraversalMsg> stepList = resultsReader.getRevisitTraversals();
                                                
                                                for(TraversalMsg traversal:stepList){
                                                  TraversalWithState v=processMessage(traversal) ;
                                                  if(v!=null){
                                                          if(!traversal.dir)
                                                                  state.revLocalVertexList.add( v );
                                                          else
                                                                  state.forwardLocalVertexList.add( v ); 
                                                  }
                                                }
                                            
                                          }
                                                
                                                
                                        }
        
                                
                        
                        //output(partition.getId(), subgraph.getId(), getSuperstep()+":"+forwardLocalVertexList.size()+":"+revLocalVertexList.size());
                        // PROCESS FORWARD LIST
                        //System.out.println("FORWARD LIST:"+forwardLocalVertexList.isEmpty() +" REV LIST:"+revLocalVertexList.isEmpty() + "SGID:" + subgraph.getId() + " PID:" + partition.getId());
                        while(!state.forwardLocalVertexList.isEmpty()) {
                                TraversalWithState vertexMessageStep = state.forwardLocalVertexList.poll();
                                //output(partition.getId(), subgraph.getId(), "FORWARD-LIST");
                                /* if last step,end that iteration*/
                                //System.out.println("Reached:" + vertexMessageStep.startVertexId + " Path Size:" + vertexMessageStep.stepsTraversed + "/" + (path.size()-1));
                                if ( vertexMessageStep.depth == state.path.size()-1 ){
                                        // TODO :gather instead of output 
                                        //output(partition.getId(), subgraph.getId(), vertexMessageStep.message);
                                        // send this as a reduceMessage
                                        //if (vertexMessageStep.previousSubgraphId == subgraph.getId()) {
                                        //      if ( !resultsMap.containsKey(vertexMessageStep.startVertexId) )
                                        //              resultsMap.put(vertexMessageStep.startVertexId, new ResultSet());
                                        //      resultsMap.get(vertexMessageStep.startVertexId).forwardResultSet.add(vertexMessageStep.message);
                                        //      
                                        //}     
//                                      else {
//                                              forwardOutputToSubgraph(1,vertexMessageStep);
//                                              output(partition.getId(), subgraph.getId(), "output();for();"+vertexMessageStep.message);
//                                      }
                                        
//                                      if (!recursivePaths.containsKey(vertexMessageStep.vertexId))
//                                      {
//                                              ArrayList<RecursivePathMaintained> tempList = new ArrayList<RecursivePathMaintained>();
//                                              tempList.add(new RecursivePathMaintained(vertexMessageStep.startVertexId, vertexMessageStep.message,0));
//                                              recursivePaths.put( vertexMessageStep.vertexId, tempList);
//                                      }
//                                      else{
//                                              recursivePaths.get(vertexMessageStep.vertexId).add(new RecursivePathMaintained(vertexMessageStep.startVertexId, vertexMessageStep.message,0));
//                                      }
//                                      System.out.println("Querying Output Path:" + vertexMessageStep.queryId+","+vertexMessageStep.startStep+","+true+","+vertexMessageStep.startVertexId );
                                        if(state.outputPathMaintainance.containsKey(new OutputPathKey(vertexMessageStep.queryId,vertexMessageStep.depth,true,vertexMessageStep.startVertex))){
                                                forwardOutputToSubgraph(1,vertexMessageStep);
                                        }
                                        else{
                                            time=System.currentTimeMillis();
                                                if ( !state.resultsMap.containsKey(vertexMessageStep.startVertex) )
                                                        state.resultsMap.put(vertexMessageStep.startVertex, new ResultSet());
                                                //System.out.println("MESSAGE ADDED TO FORWARDRESULTSET:" + vertexMessageStep.message);
                                                state.resultsMap.get(vertexMessageStep.startVertex).forwardResultSet.add(vertexMessageStep.path.toString());
                                                state.resultCollectionTime+=(System.currentTimeMillis()-time);
                                        }
                                                
                                        continue;
                                }
                                
                                Step nextStep = state.path.get(vertexMessageStep.depth+1);
                                
                                
                                IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex = getSubgraph().getVertexById(new LongWritable(vertexMessageStep.targetVertex));
                                
                                if( nextStep.type == Type.EDGE ) {
                                        
                                        if ( nextStep.direction == Direction.OUT ) {
                                                /* null predicate handling*/
                                                //int count=0;
                                                boolean flag=false;
                                                boolean addFlag=false;
                                                if ( nextStep.property == null && nextStep.value == null ) {
                                                        for( IEdge<MapValue, LongWritable, LongWritable> edge: currentVertex.getOutEdges()) {
                                                                //count++;
//                                                              System.out.println("Traversing edges");
                                                                IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex = getSubgraph().getVertexById(edge.getSinkVertexId());
                                                                long currentVertexId=otherVertex.getVertexId().get();
                                                                long currentEdgeId=edge.getEdgeId().get();
                                                                PathWithDir modifiedPath = vertexMessageStep.path.getCopy();
                                                                 modifiedPath.addEV(currentEdgeId, currentVertexId,true);
                                                                if ( !otherVertex.isRemote() ) {
//                                                                      System.out.println("Path Till Now:" + _modifiedMessage.toString());
//                                                                      state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                        state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,currentVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth+1,modifiedPath));
                                                                }
                                                                else {
                                                                        
                                                                        
                                                                        if(!flag){
                                                                        addFlag=StoreRecursive(vertexMessageStep,modifiedPath,true);    
                                                                        
                                                                        flag=true;
                                                                        }
                                                                        
                                                                        if(addFlag){
//                                                                              state.forwardRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1,vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed+1, vertexMessageStep.previousSubgraphId,vertexMessageStep.previousPartitionId));
                                                                          state.forwardRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,currentVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.depth+1,vertexMessageStep.targetVertex,vertexMessageStep.depth+1,modifiedPath));
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
                                                                        long currentVertexId=otherVertex.getVertexId().get();
                                                                        long currentEdgeId=edge.getEdgeId().get();
                                                                        PathWithDir modifiedPath = vertexMessageStep.path.getCopy();
                                                                         modifiedPath.addEV(currentEdgeId, currentVertexId,true);
                                                                        if ( !otherVertex.isRemote() ) {
                                                                                /* TODO :add the correct value to list*/
//                                                                              state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                          state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,currentVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth+1,modifiedPath));
                                                                        }
                                                                        else {
                                                                                /* TODO :add vertex to forwardRemoteVertexList*/
                                                                                
                                                                                
                                                                                if(!flag){
                                                                                addFlag=StoreRecursive(vertexMessageStep,modifiedPath,true);    
                                                                                
                                                                                flag=true;
                                                                                }
                                                                                
                                                                                if(addFlag){
//                                                                                      state.forwardRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1,vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed+1, vertexMessageStep.previousSubgraphId,vertexMessageStep.previousPartitionId));
                                                                                        state.forwardRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,currentVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.depth+1,vertexMessageStep.targetVertex,vertexMessageStep.depth+1,modifiedPath));
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
                                                        if(state.InEdges.containsKey(currentVertex.getVertexId().get()))
                                                        for(Map.Entry<Long, EdgeAttr> edgeMap: state.InEdges.get(currentVertex.getVertexId().get()).entrySet()) {
                                                                long otherVertexId = edgeMap.getKey();
                                                                PathWithDir modifiedPath = vertexMessageStep.path.getCopy();
                                                                modifiedPath.addEV(edgeMap.getValue().getEdgeId(), otherVertexId,false);
                                                                if ( !edgeMap.getValue().isRemote() ) {
                                                                        /* TODO :add the correct value to list*/
//                                                                      state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                  state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,otherVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth+1,modifiedPath));
                                                                }
                                                                else{
                                                                /* TODO :add vertex to forwardRemoteVertexList*/
                                                                        
                                                                        if(!flag){
                                                                        addFlag=StoreRecursive(vertexMessageStep,modifiedPath,true);    
                                                                        
                                                                        flag=true;
                                                                        }
                                                                        
                                                                        if(addFlag){
//                                                                              state.forwardRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed+1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                          state.forwardRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,otherVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.depth+1,vertexMessageStep.targetVertex,vertexMessageStep.depth+1,modifiedPath));
                                                                        }
                                                                        
                                                                }
                                                        
                                                        }
                                                }
                                                /* filtered edge*/
                                                else {
                                                        if(state.InEdges.containsKey(currentVertex.getVertexId().get()))
                                                        for( Map.Entry<Long, EdgeAttr> edgeMap: state.InEdges.get(currentVertex.getVertexId().get()).entrySet() ) {
                                                                //ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
                                                                //output(partition.getId(), subgraph.getId(), currentVertex.getId()+":"+subgraphProperties.getValue("relation"));
                                                                if ( compareValuesUtil(edgeMap.getValue().getValue().toString(), nextStep.value.toString()) ) {
                                                                        long otherVertexId = edgeMap.getKey();
                                                                        PathWithDir modifiedPath = vertexMessageStep.path.getCopy();
                                                                        modifiedPath.addEV(edgeMap.getValue().getEdgeId(), otherVertexId,false);
                                                                        if ( !edgeMap.getValue().isRemote() ) {
                                                                                /* TODO :add the correct value to list*/
//                                                                              state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                          state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,otherVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth+1,modifiedPath));
                                                                        }
                                                                        else{
                                                        
                                                                                if(!flag){
                                                                                addFlag=StoreRecursive(vertexMessageStep,modifiedPath,true);    
                                                                                
                                                                                flag=true;
                                                                                }
                                                                                
                                                                                if(addFlag){
//                                                                                      state.forwardRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed+1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                                  state.forwardRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,otherVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.depth+1,vertexMessageStep.targetVertex,vertexMessageStep.depth+1,modifiedPath));
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
//                                              state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                          state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,vertexMessageStep.targetVertex,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth+1,vertexMessageStep.path));
                                                
                                        }
                                        /* filtered vertex*/
                                        else {
//                                              ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForVertex(currentVertex.getId());
                                                if ( compareValuesUtil(currentVertex.getValue().get(nextStep.property).toString(), nextStep.value.toString()) ) {
                                                        /* add appropriate value later*/
//                                                      state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                  state.forwardLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,vertexMessageStep.targetVertex,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth+1,vertexMessageStep.path));
                                                
                                                }
                                        }
                                        
                                }
                                
                        }
                        
                        
                        // PROCESS REVERSE LIST
                        while(!state.revLocalVertexList.isEmpty()) {
                                TraversalWithState vertexMessageStep = state.revLocalVertexList.poll();
                                
                                /* if last step,end that iteration, while traversing in reverse direction last step is first step which is zero */
                                if ( vertexMessageStep.depth == 0 ){
                                        //if current subgraph is not source subgraph then start recursive aggregation of partial results
//                                      System.out.println("Querying Output Path:" + vertexMessageStep.queryId+","+vertexMessageStep.startStep+","+false+","+vertexMessageStep.startVertexId );
                                        if(state.outputPathMaintainance.containsKey(new OutputPathKey(vertexMessageStep.queryId,vertexMessageStep.startDepth,false,vertexMessageStep.startVertex))){
                                                forwardOutputToSubgraph(0,vertexMessageStep);
                                        }
                                        else{//else if current subgraph is source subgraph then store the results
                                          time=System.currentTimeMillis();
                                                if ( !state.resultsMap.containsKey(vertexMessageStep.startVertex) )
                                                        state.resultsMap.put(vertexMessageStep.startVertex, new ResultSet());
                                                
                                                state.resultsMap.get(vertexMessageStep.startVertex).revResultSet.add(vertexMessageStep.path.toString());
                                                state.resultCollectionTime+=(System.currentTimeMillis() - time);
                                        }
                                                                                
                                        continue;
                                }
                                
                                Step prevStep = state.path.get(vertexMessageStep.depth-1);
                                IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex = getSubgraph().getVertexById(new LongWritable(vertexMessageStep.targetVertex));
                        
                                
                                
                                if( prevStep.type == Type.EDGE ) {
                                        
                                        if ( prevStep.direction == Direction.OUT ) {
                                                /* null predicate handling*/
                                                boolean flag=false;
                                                boolean addFlag=false;
                                                if ( prevStep.property == null && prevStep.value == null ) {
                                                        if(state.InEdges.containsKey(currentVertex.getVertexId().get()))
                                                        for( Map.Entry<Long, EdgeAttr> edgeMap: state.InEdges.get(currentVertex.getVertexId().get()).entrySet()) {
                                                                long otherVertexId = edgeMap.getKey();
                                                                PathWithDir modifiedPath = vertexMessageStep.path.getCopy();
                                                                modifiedPath.addEV(edgeMap.getValue().getEdgeId(), otherVertexId,false);
                                                                if ( !edgeMap.getValue().isRemote() ) {
                                                                        /* TODO :add the correct value to list*/
//                                                                      state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                  state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,otherVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth-1,modifiedPath));
                                                                }
                                                                else{
                                                                        
                                                                        if(!flag){
                                                                        addFlag=StoreRecursive(vertexMessageStep,modifiedPath,false);
                                                                        
                                                                        flag=true;
                                                                        }
                                                                        
                                                                        if(addFlag){
//                                                                              state.revRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed-1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                          state.revRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,otherVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.depth-1,vertexMessageStep.targetVertex,vertexMessageStep.depth-1,modifiedPath));
                                                                        }
                                                                        
                                                                }
                                                        
                                                        }
                                                }
                                                /* filtered edge*/
                                                else {
                                                        if(state.InEdges.containsKey(currentVertex.getVertexId().get()))
                                                        for( Map.Entry<Long, EdgeAttr> edgeMap: state.InEdges.get(currentVertex.getVertexId().get()).entrySet() ) {
                                                                //ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
                                                                //output(partition.getId(), subgraph.getId(), currentVertex.getId()+":"+subgraphProperties.getValue("relation"));
                                                                if ( compareValuesUtil(edgeMap.getValue().getValue(), prevStep.value) ) {
                                                                        long otherVertexId = edgeMap.getKey();
                                                                        //output(partition.getId(), subgraph.getId(), String.valueOf(otherVertex.getId()));
                                                                        PathWithDir modifiedPath = vertexMessageStep.path.getCopy();
                                                                        modifiedPath.addEV(edgeMap.getValue().getEdgeId(), otherVertexId,false);
                                                                        if ( !edgeMap.getValue().isRemote()) {
                                                                                /* TODO :add the correct value to list*/
//                                                                              state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                                state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,otherVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth-1,modifiedPath));
                                                                                
                                                                        }
                                                                        else{
                                                                                
                                                                                
                                                                                if(!flag){
                                                                                addFlag=StoreRecursive(vertexMessageStep,modifiedPath,false);
                                                                                
                                                                                flag=true;
                                                                                }
                                                                                
                                                                                if(addFlag){
//                                                                                      state.revRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed-1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                                        state.revRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,otherVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.depth-1,vertexMessageStep.targetVertex,vertexMessageStep.depth-1,modifiedPath));
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
                                                                long currentVertexId=otherVertex.getVertexId().get();
                                                                long currentEdgeId=edge.getEdgeId().get();
                                                                PathWithDir modifiedPath = vertexMessageStep.path.getCopy();
                                                                 modifiedPath.addEV(currentEdgeId, currentVertexId,true);
                                                                if ( !otherVertex.isRemote() ) {
                                                                        /* add the correct value to list*/
//                                                                      state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                  state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,currentVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth-1,modifiedPath));
                                                                }
                                                                /* TODO : clarify with Ravi about InEdge having remote source( not possible?)*/
                                                                else {
                                                                        /* TODO :add vertex to revRemoteVertexList*/
                                                                        
                                                                        if(!flag){
                                                                        addFlag=StoreRecursive(vertexMessageStep,modifiedPath,false);
                                                                        
                                                                        flag=true;
                                                                        }
                                                                        
                                                                        if(addFlag){
//                                                                              state.revRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.vertexId, vertexMessageStep.stepsTraversed-1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                          state.revRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,currentVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.depth-1,vertexMessageStep.targetVertex,vertexMessageStep.depth-1,modifiedPath));
                                                                        }
                                                                        
                                                                }
                                                                        
                                                        }
                                                        
                                                }
                                                /* filtered edge*/
                                                else {
                                                        for( IEdge<MapValue, LongWritable, LongWritable> edge: currentVertex.getOutEdges() ) {
//                                                              ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
//                                                              output(partition.getId(), subgraph.getId(), currentVertex.getId()+":"+subgraphProperties.getValue("relation"));
                                                          //CHANGE it when moving to hashmaps
                                                                if ( compareValuesUtil(edge.getValue().get(prevStep.property.toString()).toString(), prevStep.value.toString()) ) {
                                                                        IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex = getSubgraph().getVertexById(edge.getSinkVertexId());
                                                                        long currentVertexId=otherVertex.getVertexId().get();
                                                                        long currentEdgeId=edge.getEdgeId().get();
                                                                        PathWithDir modifiedPath = vertexMessageStep.path.getCopy();
                                                                         modifiedPath.addEV(currentEdgeId, currentVertexId,true);
                                                                        if ( !otherVertex.isRemote() ) {
                                                                                /* TODO :add the correct value to list*/
//                                                                              state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                          state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,currentVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth-1,modifiedPath));
                                                                        }
                                                                        /* TODO : clarify with Ravi about InEdge having remote source( not possible?)*/
                                                                        else {
                                                                                /* TODO :add vertex to revRemoteVertexList*/
                                                                                
                                                                                if(!flag){
                                                                                addFlag=StoreRecursive(vertexMessageStep,modifiedPath,false);
                                                                                
                                                                                flag=true;
                                                                                }
                                                                                
                                                                                if(addFlag){
//                                                                                      state.revRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed-1, vertexMessageStep.vertexId,vertexMessageStep.stepsTraversed-1, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                                                  state.revRemoteVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,currentVertexId,vertexMessageStep.previousSubgraph,vertexMessageStep.depth-1,vertexMessageStep.targetVertex,vertexMessageStep.depth-1,modifiedPath));
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
//                                              state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                          state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,vertexMessageStep.targetVertex,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth-1,vertexMessageStep.path));
                                            
                                        }
                                        /* filtered vertex*/
                                        else {
//                                              LOG.info("PROP:"+prevStep.property.toString() + " VALUE:" + currentVertex.getValue().get(prevStep.property.toString()));
                                                if ( compareValuesUtil(currentVertex.getValue().get(prevStep.property.toString()).toString(), prevStep.value.toString()) ) {
                                                        /* add appropriate value later*/
                                                  
//                                                      state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed-1, vertexMessageStep.startVertexId,vertexMessageStep.startStep,vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
                                                  state.revLocalVertexList.add(new TraversalWithState(vertexMessageStep.queryId,vertexMessageStep.rootSubgraph,vertexMessageStep.rootVertex,vertexMessageStep.targetVertex,vertexMessageStep.previousSubgraph,vertexMessageStep.startDepth,vertexMessageStep.startVertex,vertexMessageStep.depth-1,vertexMessageStep.path));
                                                }
                                        }
                                        
                                }
                                
                        }
                        //MAP from remoteSGID to list of traversals
                        Map<Long, RevisitTraversalWriter> remoteTraversalMap = new HashMap<>();
                        // TODO: send the messages in Remote vertex list
                        for(TraversalWithState stuff: state.forwardRemoteVertexList){
                                // send message to all the remote vertices
                                
                                IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)getSubgraph().getVertexById(new LongWritable(stuff.targetVertex));
                                PathMessage remoteMessage;
                                //remoteMessage.append(String.valueOf(stuff.vertexId.longValue())).append(";").append(stuff.message).append(";").append(stuff.stepsTraversed) ;
                                long remoteSGID;
                                if(remoteVertex!=null){
//                                              remoteMessage.append(String.valueOf(stuff.startVertexId)).append(";").append(String.valueOf(stuff.previousSubgraphId)).append(";").append(stuff.previousPartitionId).append(";").append(stuff.vertexId).append(";").append(stuff.stepsTraversed).append(";").append(remoteVertex.getSubgraphId().get());
//                                              remoteMessage=new PathMessage(stuff.queryId,stuff.startVertex,stuff.previousSubgraph,stuff.targetVertex,stuff.depth,true);
                                                remoteSGID=remoteVertex.getSubgraphId().get();
                                        }
                                        else{
//                                              remoteMessage.append(String.valueOf(stuff.startVertexId)).append(";").append(String.valueOf(stuff.previousSubgraphId)).append(";").append(stuff.previousPartitionId).append(";").append(stuff.vertexId).append(";").append(stuff.stepsTraversed).append(";").append(state.InEdges.get(stuff.startVertexId).get(stuff.vertexId).getSinkSubgraphId());
//                                        remoteMessage=new PathMessage(stuff.queryId,stuff.startVertex,stuff.previousSubgraph,stuff.targetVertex,stuff.depth,true);
                                          remoteSGID=state.InEdges.get(stuff.startVertex).get(stuff.targetVertex).getSinkSubgraphId();
                                        }
//                              
                                RevisitTraversalWriter traversalMessage = remoteTraversalMap.get(remoteSGID);
                                if (traversalMessage == null) {
                                        traversalMessage = new RevisitTraversalWriter();
                                        remoteTraversalMap.put(remoteSGID, traversalMessage);
                                }
                                traversalMessage.addTraversal(stuff.queryId,stuff.rootSubgraph,stuff.rootVertex, stuff.previousSubgraph,stuff.startVertex,stuff.targetVertex,stuff.depth,true);
                                
                                        
                        }
                        state.forwardRemoteVertexList.clear();
                        
                        
                        
                        
                        for(TraversalWithState stuff: state.revRemoteVertexList){
                                // send message to all the remote vertices
                                IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)getSubgraph().getVertexById(new LongWritable(stuff.targetVertex));
                                PathMessage remoteMessage ;
                                //remoteMessage.append(String.valueOf(stuff.vertexId.longValue())).append(";").append(stuff.message).append(";").append(stuff.stepsTraversed) ;
                                long remoteSGID;
                                if(remoteVertex!=null){
//                                              remoteMessage.append(String.valueOf(stuff.startVertexId)).append(";").append(String.valueOf(stuff.previousSubgraphId)).append(";").append(stuff.previousPartitionId).append(";").append(stuff.vertexId).append(";").append(stuff.stepsTraversed).append(";").append(remoteVertex.getSubgraphId().get());
                                                remoteSGID=remoteVertex.getSubgraphId().get();
                                        }
                                        else{
//                                              remoteMessage.append(String.valueOf(stuff.startVertexId)).append(";").append(String.valueOf(stuff.previousSubgraphId)).append(";").append(stuff.previousPartitionId).append(";").append(stuff.vertexId).append(";").append(stuff.stepsTraversed).append(";").append(state.InEdges.get(stuff.startVertexId).get(stuff.vertexId).getSinkSubgraphId());
                                          remoteSGID=state.InEdges.get(stuff.startVertex).get(stuff.targetVertex).getSinkSubgraphId();
                                        }
//                              
                                RevisitTraversalWriter traversalMessage = remoteTraversalMap.get(remoteSGID);
                                if (traversalMessage == null) {
                                        traversalMessage = new RevisitTraversalWriter();
                                        remoteTraversalMap.put(remoteSGID, traversalMessage);
                                }
                                traversalMessage.addTraversal(stuff.queryId,stuff.rootSubgraph,stuff.rootVertex, stuff.previousSubgraph,stuff.startVertex,stuff.targetVertex,stuff.depth,false);
                                
                        }
                        state.revRemoteVertexList.clear();
                        //Sending traversal, one per Remote subgraph
                        for (Entry<Long, RevisitTraversalWriter> entry : remoteTraversalMap.entrySet()) {
                                sendMessage(new LongWritable(entry.getKey()), new PathMessage(entry.getValue()));
                        }
                        
                        
        
                        //sending partial results back
                        for (Entry<Long, ResultsWriter> entry : remoteResultsMap.entrySet()) {
//                            LOG.info("Sending Results Back:" + entry.getValue().toString());
                                sendMessage(new LongWritable(entry.getKey()), new PathMessage(entry.getValue()));
                        }
                        remoteResultsMap.clear();
                        
                }
                }

		voteToHalt();

	}





  @Override
	public void wrapup() throws IOException {
    if(!queryEnd){
      queryEnd=true;
    LOG.info("Ending Query Execution");
    }
    PathStateTest state=getSubgraph().getSubgraphValue();
          for(Map.Entry<Long, ResultSet> entry: state.resultsMap.entrySet()) {
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
  LOG.info("Cumulative Result Collection:" +  state.resultCollectionTime);        
	}


  private void clear() {
    // TODO Auto-generated method stub
    PathStateTest state=getSubgraph().getSubgraphValue();
    state.Arguments=null;
    state.forwardLocalVertexList.clear();
    state.forwardRemoteVertexList.clear();
    state.revLocalVertexList.clear();
    state.revRemoteVertexList.clear();
    state.MessagePacking.clear();
    state.noOfSteps=0;
    state.outputPathMaintainance.clear();
    state.partialResultCache.clear();
    state.recursivePaths.clear();
    state.resultsMap.clear();
    state.path.clear();
    state.queryCostHolder=null;
    state.startPos=0;
    queryMade=false;
    queryStart=false;
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
  
  
  
}

