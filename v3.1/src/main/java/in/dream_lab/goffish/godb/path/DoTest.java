package in.dream_lab.goffish.godb.path;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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
import in.dream_lab.goffish.godb.path.PathMessage.ResultsWriter;
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
	Hueristics heuristics=null;
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
		heuristics=HueristicsLoad.getInstance();
		
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
		

		voteToHalt();

	}





  @Override
	public void wrapup() throws IOException {
		// Writing results
	        LOG.info("Ending Query Execution");
		PathStateTest state = getSubgraph().getSubgraphValue();
		
		// clearing Subgraph Value for next query
		// for cleaning up the subgraph value so that Results could be cleared while
		// Inedges won't be cleared so that it could be reused.
		clear();
		
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

