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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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
import org.apache.lucene.util.Version;


import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraphWrapup;
import in.dream_lab.goffish.api.IVertex;


public class reachabilityDistrNonIndexed extends
AbstractSubgraphComputation<reachabilityDistrNonIndexedSubgraphState, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable>
implements ISubgraphWrapup{
        
        
        public reachabilityDistrNonIndexed(String initMsg) {
                // TODO Auto-generated constructor stub
                Arguments=initMsg;
        }
        public static final Log LOG = LogFactory.getLog(reachabilityDistrIn.class);
        long time;
        String Arguments=null;
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
        private static final Object statsLock = new Object();
        static boolean statsLoaded=true;
        private static boolean queryStart=false;//later lock this when multithreaded
        private static boolean queryEnd=false;//later lock this when multithreaded
        private static boolean gcCalled=false;//later lock this when multithreaded
        /**
         * Representative class to keep tab of next vertex to be processed,  this is different for 
         * path queries, hence defined separately
         */
         class VertexMessageSteps{
                public Long vertexId;
                public String message;
                public Integer stepsTraversed;
                public Long subgraphId;
                public Long startSubgraphId;
                VertexMessageSteps(Long _vertexId,String _message,Integer _stepTraversed,Long _subgraphId,Long _startSubgraphId){
                        this.vertexId = _vertexId;
                        this.message = _message;
                        this.stepsTraversed= _stepTraversed;
                        this.subgraphId= _subgraphId;
                        this.startSubgraphId = _startSubgraphId;
                }
        } 
        
        
        
        
        
        
        /**
         * Initialize the class variables
         * 
         */
        private void init(Iterable<IMessage<LongWritable,Text>> messageList){
                //noOfSteps@attrName:type[value]@attrName:type[value]//startInstance//endInstance
                
                String arguments = Arguments; 
                getSubgraph().getSubgraphValue().Arguments=arguments;
                Long minSubgraphId=Long.MAX_VALUE;
                //FIXME:find an alternative for this
                
//              if(getSubgraph().getSubgraphId().get()==minSubgraphId)//printing arguments only once
//                      System.out.println("*************ARGUMENTS:***************"+arguments);
                
                
                String[] _string = arguments.split(Pattern.quote("//"))[0].split(Pattern.quote("@"));
                getSubgraph().getSubgraphValue().noOfSteps = Integer.valueOf(_string[0]);
                getSubgraph().getSubgraphValue().stopTraversalLength = getSubgraph().getSubgraphValue().noOfSteps;
                getSubgraph().getSubgraphValue().path = new ArrayList<Step>();
                for (int i=1;i<3;i++)
                {
                                String p = _string[i].split(Pattern.quote(":"))[0];
                                String typeAndValue = _string[i].split(Pattern.quote(":"))[1];
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
                                
                                if (i==1)
                                        getSubgraph().getSubgraphValue().startVertex = new Step(p, v);
                                if (i==2)
                                        getSubgraph().getSubgraphValue().endVertex = new Step(p, v);
                }
                        

                
                
                //noOfSteps = 2;
                getSubgraph().getSubgraphValue().queryCostHolder = new Double[2];
                for (int i = 0; i < getSubgraph().getSubgraphValue().queryCostHolder.length; i++) {
                        getSubgraph().getSubgraphValue().queryCostHolder[i] = new Double(0);
                }
                getSubgraph().getSubgraphValue().forwardLocalVertexList = new LinkedList<VertexMessageSteps>();
                getSubgraph().getSubgraphValue().revLocalVertexList = new LinkedList<VertexMessageSteps>();
                
                
//              getSubgraph().getSubgraphValue().hueristics=HueristicsLoad.getInstance();//loading this at a different place
        }

        

        
        
        private void initLucene() throws InterruptedException, IOException{
                
                {
                  long pseudoPid=getSubgraph().getSubgraphId().get() >> 32;
                        initDone = true;
                        vertexIndexDir = new File(ConfigFile.basePath+ "/index/Partition"+pseudoPid+"/vertexIndex"); //TODO: Change this
                        vertexDirectory = FSDirectory.open(vertexIndexDir);
                        analyzer = new StandardAnalyzer(Version.LATEST);
                        indexReader  = DirectoryReader.open(vertexDirectory);
                        indexSearcher = new IndexSearcher(indexReader);
                }
                
        }


        private void join(IMessage<LongWritable,Text> _message) {
                long Time= System.currentTimeMillis();
                String message = _message.getMessage().toString();
                String[] split = message.split(Pattern.quote(";"));
                //Long _startVertexId = Long.parseLong( split[2] );
                //resultsSet.put(_startVertexId, new ResultSet());
                if ( split[1].equals("for()") ) 
                        getSubgraph().getSubgraphValue().resultsSet.forwardResultSet.add(split[3]);
                else
                        getSubgraph().getSubgraphValue().resultsSet.revResultSet.add(split[3]);
                
                Time=System.currentTimeMillis()-Time;
                getSubgraph().getSubgraphValue().resultCollectionTime+=Time;
        }
        
        
        private void forwardOutputToSubgraph(int direction,VertexMessageSteps step) {
                long Time= System.currentTimeMillis();
                String dir="for()";
                if(direction==0)
                        dir="rev()";
                StringBuilder remoteMessage = new StringBuilder("output();"+dir+";");
                remoteMessage.append(step.startSubgraphId+";"+step.message) ;
                Text remoteM = new Text(remoteMessage.toString());
                sendMessage(new LongWritable(step.startSubgraphId), remoteM); 
                Time=System.currentTimeMillis()-Time;
                getSubgraph().getSubgraphValue().resultCollectionTime+=Time;
        }
        
//      private void makeQuery(String prop,String val) throws IOException{
//              {
//                      queryMade = true;
//                      query  = new BooleanQuery();
//                      query.add(new TermQuery(new Term(prop, val)), BooleanClause.Occur.MUST);
//                      hits =  indexSearcher.search(query,40000).scoreDocs;
//              }
//      }
        //for querying lucene for starting vertex
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
        
        private boolean compareValuesUtil(Object o,Object currentValue){
                if( o.getClass().getName() != currentValue.getClass().getName()){return false;}
                if (o instanceof Float){
                        return ((Float)o).equals((Float)currentValue);
                }
                else if (o instanceof Double){
                        return ((Double)o).equals((Double)currentValue);
                }
                else if (o instanceof Integer){
                        return ((Integer)o).equals((Integer)currentValue);
                }
                else{
                        return ((String)o).equals((String)currentValue);
                }
                
        }

        private void sendStopMessage(Integer _stepsTraversed){
                
                        StringBuilder remoteMessage = new StringBuilder("#:").append(_stepsTraversed);
                        Text remoteM = new Text(remoteMessage.toString().getBytes());
                        sendToAll(remoteM);     
                
        }

                
        @Override
        public void compute(Iterable<IMessage<LongWritable,Text>> _messageList) {
                
                Iterable<IMessage<LongWritable,Text>> messageList = _messageList;
                
                //###########################################STATIC ONE TIME PROCESSES##########################################
                {
                        // LOAD QUERY AND INITIALIZE LUCENE
                        if(getSuperstep() == 0){
        
        
                                if(Arguments == null ){
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
                                                                initLucene();
                                                }
                                        }catch(Exception e){e.printStackTrace();}
                                        
                                }
                        }
                        
                        
                        else if (getSuperstep()==1) {
                                // GATHER HEURISTICS FROM OTHER SUBGRAPHS
//                              System.out.println("******************Gathering Heuristics******************");
//                              gatherHueristics(messageList);
//                              System.out.println("***********DONE**************");
                                
                                HashMap<Long,HashMap<String,StringBuilder>> remoteMap = new HashMap<Long,HashMap<String,StringBuilder>>();
//                              LOG.info("ENTERING");
                                if(getSubgraph().getSubgraphValue().InEdges==null){
                                        //Logic to Accumulate inedges
                                          getSubgraph().getSubgraphValue().InEdges=new HashMap<Long,HashMap<Long,EdgeAttr>>();  
                                        time=System.currentTimeMillis();
                                
                                        
                                        String m="";
                                        
                                        for(IVertex<MapValue, MapValue, LongWritable, LongWritable> sourceVertex:getSubgraph().getLocalVertices())
                                        for(IEdge<MapValue, LongWritable, LongWritable> edge : sourceVertex.getOutEdges()) {
                                                
                                                IVertex<MapValue, MapValue, LongWritable, LongWritable> sinkVertex=getSubgraph().getVertexById(edge.getSinkVertexId());
//                                              LOG.info("VERTEX:" + sinkVertex.getVertexId().get());
                                        //if sink vertex is not remote then add inedge to appropriate data structure, otherwise send source value to remote partition
                                        if(!sinkVertex.isRemote())
                                        {
                                 
                                                if(getSubgraph().getSubgraphValue().InEdges.containsKey(sinkVertex.getVertexId().get()))
                                                {
                                                        if(!getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).containsKey(sourceVertex.getVertexId().get()))
                                                        {
                                                                
                                                                
//                                                            ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
                                                                EdgeAttr attr= new EdgeAttr("relation","null" /*subgraphProperties.getValue("relation").toString()*/,edge.getEdgeId().get(),false,null);
                                                                getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).put(sourceVertex.getVertexId().get(), attr);
                                                                //System.out.println("Accumulation inedge for edge "+ edge.getId() + " Value " + subgraphProperties.getValue("relation").toString() );
                                                        }
                                                        
                                                }
                                                else
                                                {
//                                                    ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
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
                                        Text msg = new Text(remoteSubgraphMessage.getValue().toString());
                                
                                sendMessage(new LongWritable(remoteSubgraphMessage.getKey()),msg);
                                }
                                
                        }
                        
                        
                        }
        
                        // "subgraphId/20:attr?21,12,23|attr?12,12"       This is an example message sent to Subgraphs of remote Sinks 
                        else if ( getSuperstep()==2 ) {
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
                                LOG.info("Loading Heuristics");
                                getSubgraph().getSubgraphValue().hueristics=HueristicsLoad.getInstance();
                                LOG.info("Loading Heuristics Complete");
                                if(!gcCalled){
                                        gcCalled=true;
                                        System.gc();
                                        System.runFinalization();
                                        }

                        }
                }
                //###########################################STATIC ONE TIME PROCESSES##########################################
                
                //###########################################RUNTIME FUNCTIONALITITES########################################### 
                {
                        //#######################################COMPUTE-LOAD-INIT##################################################
                        if(getSuperstep()==3){
                          if(!queryStart){
                                queryStart=true;
                                LOG.info("Starting Query Execution");
                                 queryEnd=false;
                          }
                                //###################################COMPUTE HUERISTIC BASED QUERY COST#################################
//                              {        
//                                      //###########################forward cost#######################################################
                                        {       
                                                Double totalCost = new Double(0);
                                                Double prevScanCost = getSubgraph().getSubgraphValue().hueristics.numVertices;
                                                {
                                                        
                                                        {
                                                                Double probability = null;
                                                                Step currentStep = getSubgraph().getSubgraphValue().startVertex;
                                                                if ( getSubgraph().getSubgraphValue().hueristics.vertexPredicateMap.get(currentStep.property).containsKey(currentStep.value.toString()) )
                                                                                probability = getSubgraph().getSubgraphValue().hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).probability;
                                                                else {
                                                                                totalCost = new Double(-1);
                                                                }
                                                                
                                                                Double avgDeg = null;
                                                                Double avgRemoteDeg = null;
                                                                avgDeg = getSubgraph().getSubgraphValue().hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgOutDegree; 
                                                                avgRemoteDeg = getSubgraph().getSubgraphValue().hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgRemoteOutDegree;   
                                                                if (totalCost!=-1)
                                                                        totalCost = prevScanCost * probability *  ( avgDeg + (1+getSubgraph().getSubgraphValue().networkCoeff) * avgRemoteDeg);
                                                        }       
                                                                                        
                                                }
                                                getSubgraph().getSubgraphValue().queryCostHolder[0] = totalCost;
                                        }
//                                      //###########################forward cost#######################################################
//                                      //###########################reverse cost#######################################################
                                        {
                                                Double totalCost = new Double(0);
                                                Double prevScanCost = getSubgraph().getSubgraphValue().hueristics.numVertices;
                                                {
                                                        
                                                        {
                                                                Double probability = null;
                                                                Step currentStep = getSubgraph().getSubgraphValue().endVertex;
                                                                if ( getSubgraph().getSubgraphValue().hueristics.vertexPredicateMap.get(currentStep.property).containsKey(currentStep.value.toString()) )
                                                                                probability = getSubgraph().getSubgraphValue().hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).probability;
                                                                else {
                                                                                totalCost = new Double(-1);
                                                                }
                                                                
                                                                Double avgDeg = null;
                                                                Double avgRemoteDeg = null;
//                                                              System.out.println("Property:" +currentStep.property);
//                                                              System.out.println("Value:" + currentStep.value);
                                                                avgDeg = getSubgraph().getSubgraphValue().hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgInDegree; 
                                                                avgRemoteDeg = getSubgraph().getSubgraphValue().hueristics.vertexPredicateMap.get(currentStep.property).get(currentStep.value.toString()).avgRemoteInDegree;    
                                                                if (totalCost!=-1)
                                                                        totalCost = prevScanCost * probability *  ( avgDeg + (1+getSubgraph().getSubgraphValue().networkCoeff) * avgRemoteDeg);
                                                        }       
                                                                                        
                                                }
                                                getSubgraph().getSubgraphValue().queryCostHolder[1] = totalCost;
                                        }
//                                      //###########################reverse cost#######################################################
//                              }               
//                              //###################################COMPUTE HUERISTIC BASED QUERY COST#################################        
                                
                                
                        
                                
                                
                                //##################################LOAD START VERTICES#################################################
                                {
                                        Integer startPos = -1;
                                        if (getSubgraph().getSubgraphValue().queryCostHolder[0]==-1 && getSubgraph().getSubgraphValue().queryCostHolder[1]==-1)
                                                startPos = -1;
                                        else
                                                if(getSubgraph().getSubgraphValue().queryCostHolder[0]==-1)
                                                        startPos = 1;
                                                else if(getSubgraph().getSubgraphValue().queryCostHolder[1]==-1)
                                                        startPos = 0;
                                                else
                                                        startPos = getSubgraph().getSubgraphValue().queryCostHolder[0]<getSubgraph().getSubgraphValue().queryCostHolder[1] ? 0 : 1;
//                                      startPos=0;//used for debugging
                                        String currentProperty = null;
                                        Object currentValue = null;
                                        if (startPos == 0) {
                                                currentProperty = getSubgraph().getSubgraphValue().startVertex.property; 
                                                currentValue = getSubgraph().getSubgraphValue().startVertex.value;
                                        }
                                        else if(startPos == 1) {
                                                currentProperty = getSubgraph().getSubgraphValue().endVertex.property; 
                                                currentValue = getSubgraph().getSubgraphValue().endVertex.value;
                                        }
                                        

//                                        try{
//                                                synchronized(queryLock){
//                                                        if(!queryMade){
//                                                                makeQuery(currentProperty,currentValue);
//                                                        }
//                                                }
//                                        }catch(Exception e){
//                                                e.printStackTrace();
//                                                
//                                        }
                                        
                                        //VertexMessageSteps(Long _vertexId,String _message,Integer _stepTraversed,Long _subgraphId,Integer _partitionId,Long _startSubgraphId)
                                        //partition.getId()
                                        // TODO : else iteratively check for satisfying vertices
                                        
                                        System.out.println("START_POS:" + startPos +"Query Cost:" + getSubgraph().getSubgraphValue().queryCostHolder[startPos]);
//                                      for(ITemplateVertex vertex: subgraph.vertices()) {
//                                              if ( vertex.isRemote() ) continue;
//                                              ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForVertex(vertex.getId());
//                                              if ( compareValuesUtil(subgraphProperties.getValue(currentProperty), currentValue) ) {
//                                                      String _message = "V:"+String.valueOf(vertex.getId());
//                                                      if ( startPos == 0  )
//                                                              forwardLocalVertexList.add( new VertexMessageSteps(vertex.getId(),_message,1, subgraph.getId(), partition.getId(), subgraph.getId(), partition.getId()) );
//                                                      if( startPos == 1 ){
////                                                            System.out.println("ERROR!!");
//                                                              revLocalVertexList.add( new VertexMessageSteps(vertex.getId(),_message,1, subgraph.getId(), partition.getId(), subgraph.getId(), partition.getId()) );
//                                                      }
//                                                      //output(partition.getId(), subgraph.getId(), subgraphProperties.getValue(currentProperty).toString());
//                                              }
//                                      }
                                        
//                                        System.out.println("*******Querying done********:"+hits.length);
                                        
                                        try
                                        {
                                        
                                        //FIXME: remove partitionid
//                                        if(hits.length>0){
//                                                for (int i=0;i<hits.length;i++){
//                                                        Document doc = indexSearcher.doc(hits[i].doc);
//                                                        if ( Long.valueOf(doc.get("subgraphid")) == getSubgraph().getSubgraphId().get() ){
//                                                                Long _vertexId = Long.valueOf(doc.get("id"));
//                                                                String _message = "V:"+String.valueOf(_vertexId);
//                                                                //System.out.println("STARTING VERTEX:" + _message);
//                                                                if ( startPos == 0  )
//                                                                getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(_vertexId,_message,1, getSubgraph().getSubgraphId().get(),getSubgraph().getSubgraphId().get()) );
//                                                        if( startPos == 1 ){
////                                                                System.out
////                                                                    .println("Adding to ReverseList");
////                                                              System.out.println("ERROR!!");
//                                                                getSubgraph().getSubgraphValue().revLocalVertexList.add( new VertexMessageSteps(_vertexId,_message,1, getSubgraph().getSubgraphId().get(), getSubgraph().getSubgraphId().get()) );
//                                                        }
//
//                                                                        
//
//                                                        }
//                                                }
//                                        }
                                              for(IVertex<MapValue, MapValue, LongWritable, LongWritable> vertex: getSubgraph().getLocalVertices()) {
                                                      if ( vertex.isRemote() ) continue;
                                                      //FIXME: change this for all type of attributes
                                                      
                                                      if ( compareValuesUtil(vertex.getValue().get(currentProperty).toString(), currentValue.toString()) ) {
//                                                              System.out.println("Vertex id:" + vertex.getVertexId().get() + "Property:"+currentProperty +" Value:" + vertex.getValue().get(new Text(currentProperty)).toString());
                                                              String _message = "V:"+String.valueOf(vertex.getVertexId().get());
                                                              if ( startPos == 0  )
                                                                      getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(vertex.getVertexId().get(),_message,1, getSubgraph().getSubgraphId().get(),getSubgraph().getSubgraphId().get()) );
                                                              if( startPos == 1 ){
//                                                                    System.out.println("ERROR!!");
                                                                      getSubgraph().getSubgraphValue().revLocalVertexList.add( new VertexMessageSteps(vertex.getVertexId().get(),_message,1, getSubgraph().getSubgraphId().get(), getSubgraph().getSubgraphId().get()) );
                                                              }
                                                              
                                                      }
                                              }
                                                
                                                
                                        }catch(Exception e){
                                                e.printStackTrace();
                                        }
                                        
                                        
                                        Iterator msgIter=messageList.iterator();
                                        while(msgIter.hasNext()){
                                          msgIter.remove();
                                        }
                                }
                                //##################################LOAD START VERTICES#################################################
                        }
                        //#######################################COMPUTE-LOAD-INIT##################################################
                        

                        //#######################################CHECK MSSG-PROCESS FORWARD-PROCESS BACKWARD########################
                        if(getSuperstep()>=3) {
                                
                                //###################################CHECK-INCOMING-MESSAGE,ADD-VERTEX-TO-LIST,OR-STOP-IF-INDICATED#####
                          Iterator msgIter=messageList.iterator();
                                if(msgIter.hasNext()){
                                        for (IMessage<LongWritable,Text> message: messageList){
                                                String messageString = message.getMessage().toString();
                                                if(messageString.contains("output();") ) {
                                                        if ( Long.parseLong(messageString.split(Pattern.quote(";"))[2])  == getSubgraph().getSubgraphId().get() ){
//                                                              System.out.println("RECIEVED OUTPUT!");
                                                                join(message);

                                                        }
                                                        continue;
                                                }
                                                if ( messageString.charAt(0) == '#' ){
//                                                      System.out.println("RECIEVED STOP MESSAGE!");
                                                        getSubgraph().getSubgraphValue().stopProcessing = true;
                                                        getSubgraph().getSubgraphValue().noOfSteps = Integer.parseInt(messageString.split(":")[1]) < getSubgraph().getSubgraphValue().noOfSteps ? Integer.parseInt(messageString.split(":")[1]):getSubgraph().getSubgraphValue().noOfSteps;
                                                        //voteToHalt();
                                                }
                                                else 
                                                        processMessage(message);
                                        }
                                }
                                //###################################CHECK-INCOMING-MESSAGE,ADD-VERTEX-TO-LIST,OR-STOP-IF-INDICATED#####
                        
                        
                                //###################################PROCESS FORWARD LIST###############################################
                                while(!getSubgraph().getSubgraphValue().forwardLocalVertexList.isEmpty()) {
                                        VertexMessageSteps vertexMessageStep = getSubgraph().getSubgraphValue().forwardLocalVertexList.poll();
                                        

                                        if( vertexMessageStep.stepsTraversed >= getSubgraph().getSubgraphValue().noOfSteps ){
//                                              System.out.println("VERTEX STUCK:" + vertexMessageStep.vertexId + ":" + vertexMessageStep.stepsTraversed + ":" + noOfSteps);
//                                              System.out.println("FOR:1");
                                                continue;
                                        }
                                        
                                        IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex = getSubgraph().getVertexById(new LongWritable(vertexMessageStep.vertexId));
                                                                                        
                                        for( IEdge<MapValue, LongWritable, LongWritable> edge: currentVertex.getOutEdges() ) {
//                                              System.out.println("Traversing Outedges");
                                                IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex = getSubgraph().getVertexById(edge.getSinkVertexId());
                                                StringBuilder _modifiedMessage = new StringBuilder("");
                                                _modifiedMessage.append(vertexMessageStep.message).append("-->E:").append(String.valueOf(edge.getEdgeId())).append("-->V:").append(String.valueOf(otherVertex.getVertexId()));
                                                
                                                if ( !otherVertex.isRemote() ) {
                                                        //FIXME: check if toString returns correct value
                                                        
//                                                      System.out.println("COMPARING:" + getSubgraph().getSubgraphValue().endVertex.value.toString() + " To " + otherVertex.getValue().get(new Text(getSubgraph().getSubgraphValue().endVertex.property)).toString());
                                                        if ( !(compareValuesUtil(otherVertex.getValue().get(getSubgraph().getSubgraphValue().endVertex.property).toString(), getSubgraph().getSubgraphValue().endVertex.value.toString())) ){
                                                                
                                                                if (vertexMessageStep.stepsTraversed<getSubgraph().getSubgraphValue().noOfSteps){
//                                                                      System.out.println("FOR:2");
//                                                                      System.out.println("Unmatched PATH:" + _modifiedMessage.toString());
                                                                        getSubgraph().getSubgraphValue().forwardLocalVertexList.add(new VertexMessageSteps(otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.subgraphId, vertexMessageStep.startSubgraphId));
                                                                }
                                                        }
                                                        else{
//                                                                      System.out.println("Matched");
                                                                if (vertexMessageStep.startSubgraphId == getSubgraph().getSubgraphId().get()) {

                                                                        getSubgraph().getSubgraphValue().resultsSet.forwardResultSet.add(_modifiedMessage.toString());
                                                                        getSubgraph().getSubgraphValue().noOfSteps = vertexMessageStep.stepsTraversed;
                                                                        sendStopMessage(vertexMessageStep.stepsTraversed);
                                                                }
                                                                else{
                                                                      time = System.currentTimeMillis();
                                                                        if (vertexMessageStep.stepsTraversed<=getSubgraph().getSubgraphValue().noOfSteps){

                                                                                forwardOutputToSubgraph(1,new VertexMessageSteps(otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startSubgraphId, vertexMessageStep.startSubgraphId));
                                                                        }
                                                                        getSubgraph().getSubgraphValue().noOfSteps = vertexMessageStep.stepsTraversed;
                                                                        sendStopMessage(vertexMessageStep.stepsTraversed);
                                                                        getSubgraph().getSubgraphValue().resultCollectionTime+=(System.currentTimeMillis()-time);
                                                                }                                                       
                                                        }
                                                }
                                                else {
//                                                      System.out.println("Remote Vertex found");
                                                        IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)otherVertex;
                                                        /* TODO :add vertex to forwardRemoteVertexList*/
                                                        if (vertexMessageStep.stepsTraversed<=getSubgraph().getSubgraphValue().noOfSteps-1){
//                                                              System.out.println("FOR:6");
                                                                getSubgraph().getSubgraphValue().forwardRemoteVertexList.add(new VertexMessageSteps(otherVertex.getVertexId().get(),_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, remoteVertex.getSubgraphId().get(), vertexMessageStep.startSubgraphId));
                                                        }
                                                }
                                                
                                        }
                                                
                                }
                                //###################################PROCESS FORWARD LIST###############################################
                                        

                                //###################################PROCESS REVERSE LIST###############################################
                                while(!getSubgraph().getSubgraphValue().revLocalVertexList.isEmpty()) {
                                      
                                        VertexMessageSteps vertexMessageStep = getSubgraph().getSubgraphValue().revLocalVertexList.poll();
                                        
                                        if( vertexMessageStep.stepsTraversed >= getSubgraph().getSubgraphValue().noOfSteps ){
                                                //System.out.println("REV:1");
                                                continue;       
                                        }

                                        IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex = getSubgraph().getVertexById(new LongWritable(vertexMessageStep.vertexId));
//                                      System.out.println("ANY Inedges +"+getSubgraph().getSubgraphValue().InEdges.containsKey(currentVertex.getVertexId().get()) + " Vertex:" + currentVertex.getVertexId().get());
                                        if(getSubgraph().getSubgraphValue().InEdges.containsKey(currentVertex.getVertexId().get()))
                                        for(Map.Entry<Long, EdgeAttr> edgeMap: getSubgraph().getSubgraphValue().InEdges.get(currentVertex.getVertexId().get()).entrySet()) {
//                                        System.out.println("Inside");
                                                Long otherVertexId = edgeMap.getKey();
                                                IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex = getSubgraph().getVertexById(new LongWritable(otherVertexId));
                                                //System.out.println("OTHER:" + otherVertexId);
                                                StringBuilder _modifiedMessage = new StringBuilder("");
                                                _modifiedMessage.append("V:").append(otherVertexId).append("<--E:").append(edgeMap.getValue().EdgeId).append("<--").append(vertexMessageStep.message);
                                                

                                                if ( !edgeMap.getValue().isRemote ) {
                                                        /* add the correct value to list*/
                                                        
                                                        if (!compareValuesUtil(otherVertex.getValue().get(getSubgraph().getSubgraphValue().startVertex.property).toString(), getSubgraph().getSubgraphValue().startVertex.value.toString()) ){
                                                                if (vertexMessageStep.stepsTraversed<getSubgraph().getSubgraphValue().noOfSteps){
                                                                        //System.out.println("REV:2");
                                                                        //System.out.println("ERROR!!!");
                                                                        getSubgraph().getSubgraphValue().revLocalVertexList.add(new VertexMessageSteps(otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.subgraphId, vertexMessageStep.startSubgraphId));
                                                                }
                                                        }       
                                                        else{
                                                                //System.out.println("REV:3");
                                                                if (vertexMessageStep.startSubgraphId == getSubgraph().getSubgraphId().get()) {
                                                                
                                                                        getSubgraph().getSubgraphValue().resultsSet.revResultSet.add(_modifiedMessage.toString());
                                                                        getSubgraph().getSubgraphValue().noOfSteps = vertexMessageStep.stepsTraversed;
                                                                    sendStopMessage(vertexMessageStep.stepsTraversed);
                                                                }
                                                                else{
                                                                        //System.out.println("REV:4");
                                                                        time=System.currentTimeMillis();
                                                                        if (vertexMessageStep.stepsTraversed<=getSubgraph().getSubgraphValue().noOfSteps){
                                                                                //System.out.println("REV:5");
                                                                                forwardOutputToSubgraph(0,new VertexMessageSteps(otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startSubgraphId,vertexMessageStep.startSubgraphId));
                                                                        }
                                                                        getSubgraph().getSubgraphValue().noOfSteps = vertexMessageStep.stepsTraversed;
                                                                        sendStopMessage(vertexMessageStep.stepsTraversed);
                                                                        getSubgraph().getSubgraphValue().resultCollectionTime+=(System.currentTimeMillis()-time);
                                                                }       
                                                        }
                                                }
                                                /* TODO : clarify with Ravi about InEdge having remote source( not possible?)*/
                                                else {
                                                        /* TODO :add vertex to forwardRemoteVertexList*/
                                                        if (vertexMessageStep.stepsTraversed<=getSubgraph().getSubgraphValue().noOfSteps-1){
                                                                //System.out.println("REV:6");
                                                                getSubgraph().getSubgraphValue().revRemoteVertexList.add(new VertexMessageSteps(otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, edgeMap.getValue().sinkSubgraphId, vertexMessageStep.startSubgraphId));
                                                        }
                                                }
                                        }
                                }
                                //###################################PROCESS REVERSE LIST###############################################

                                // TODO: send the messages in Remote vertex list
                                for(VertexMessageSteps stuff: getSubgraph().getSubgraphValue().forwardRemoteVertexList){
                                        // send message to all the remote vertices
//                                      ITemplateVertex remoteVertex = subgraph.getVertex(stuff.vertexId);
                                        IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)getSubgraph().getVertexById(new LongWritable(stuff.vertexId));
                                        StringBuilder remoteMessage = new StringBuilder("for();");
                                        remoteMessage.append(stuff.vertexId).append(";").append(stuff.message).append(";").append(stuff.stepsTraversed).append(";").append(stuff.subgraphId).append(";").append(stuff.startSubgraphId).append(";");
                                        Text remoteM = new Text(remoteMessage.toString());
                                        //remoteM.setTargetSubgraph(remoteVertex.getRemoteSubgraphId());
                                        sendMessage(remoteVertex.getSubgraphId(),remoteM);
                                }
                                getSubgraph().getSubgraphValue().forwardRemoteVertexList.clear();
                                for(VertexMessageSteps stuff: getSubgraph().getSubgraphValue().revRemoteVertexList){
                                        // send message to all the remote vertices
//                                      System.out.println("Remote Vertex:" + stuff.vertexId);
                                        IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)getSubgraph().getVertexById(new LongWritable(stuff.vertexId));
                                        StringBuilder remoteMessage = new StringBuilder("rev();");
                                        remoteMessage.append(stuff.vertexId).append(";").append(stuff.message).append(";").append(stuff.stepsTraversed).append(";").append(stuff.subgraphId).append(";").append(stuff.startSubgraphId).append(";");
                                        Text remoteM = new Text(remoteMessage.toString());
                                        //remoteM.setTargetSubgraph(remoteVertex.getRemoteSubgraphId());
                                        
                                        if(remoteVertex!=null){
                                                sendMessage(remoteVertex.getSubgraphId(),remoteM);
                                        }
                                                
                                        else{
                                                //getSubgraph().getSubgraphValue().InEdges.get(stuff.vertexId).get(stuff.vertexId).sinkSubgraphId
                                                sendMessage(new LongWritable(stuff.subgraphId),remoteM);
                                        }
                                        
//                                      sendMessage(remoteVertex.getSubgraphId(),remoteM);
                                }
                                getSubgraph().getSubgraphValue().revRemoteVertexList.clear();
                        }
                        //#######################################CHECK MSSG-PROCESS FORWARD-PROCESS BACKWARD########################
                
                }
                //###########################################RUNTIME FUNCTIONALITITES###########################################
                
                
                if(getSuperstep()>=3)
                        voteToHalt();
        }
        


        
        private void processMessage(IMessage<LongWritable,Text> _message){
                String message = _message.getMessage().toString();
                String[] split = message.split(Pattern.quote(";"));
                if (getSubgraph().getSubgraphId().get() == Long.parseLong(split[4])){
                        if (split[0].equals("for()"))
                                getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(Long.parseLong(split[1]), split[2] , Integer.parseInt(split[3]), Long.parseLong( split[4] ), Long.parseLong(split[5])) );
                        else{
                                //System.out.println("ERROR!!");
                                getSubgraph().getSubgraphValue().revLocalVertexList.add( new VertexMessageSteps(Long.parseLong(split[1]), split[2] , Integer.parseInt(split[3]), Long.parseLong( split[4] ), Long.parseLong(split[5])) );
                        }
                }       
        }
        







        @Override
        public void wrapup() {
        // TODO Auto-generated method stub
          if(!queryEnd){
            queryEnd=true;
          LOG.info("Ending Query Execution");
          }
          {
                  for(String partialForwardPath: getSubgraph().getSubgraphValue().resultsSet.forwardResultSet) {
                    LOG.info("ResultSetFORWARD : " + partialForwardPath);
                          //output(partition.getId(), subgraph.getId(), "FORWARD : " + partialForwardPath); 
                  }
                  for(String partialRevPath: getSubgraph().getSubgraphValue().resultsSet.revResultSet){
                          //output(partition.getId(), subgraph.getId(), "REVERSE : " + partialRevPath);
                    LOG.info("ResultSetREVERSE  : " + partialRevPath);
                  }
          }
          LOG.info("Cumulative Result Collection:" + getSubgraph().getSubgraphValue().resultCollectionTime);
                clear();
        }
        
        public void clear(){
          getSubgraph().getSubgraphValue().Arguments=null;
          getSubgraph().getSubgraphValue().forwardLocalVertexList.clear();
          getSubgraph().getSubgraphValue().forwardRemoteVertexList.clear();
          getSubgraph().getSubgraphValue().revLocalVertexList.clear();
          getSubgraph().getSubgraphValue().revRemoteVertexList.clear();
          getSubgraph().getSubgraphValue().MessagePacking.clear();
          getSubgraph().getSubgraphValue().resultsSet.forwardResultSet.clear();
          getSubgraph().getSubgraphValue().resultsSet.revResultSet.clear();
          getSubgraph().getSubgraphValue().noOfSteps=0;
          getSubgraph().getSubgraphValue().path.clear();
          getSubgraph().getSubgraphValue().queryCostHolder=null;
          getSubgraph().getSubgraphValue().stopTraversalLength=Integer.MAX_VALUE;
          getSubgraph().getSubgraphValue().stopProcessing=false;
          queryMade=false;
          queryStart=false;
        
          
        }




}
