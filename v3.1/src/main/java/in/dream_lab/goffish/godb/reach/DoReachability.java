package in.dream_lab.goffish.godb.reach;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.ISubgraphWrapup;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.EdgeAttr;
import in.dream_lab.goffish.godb.Hueristics;
import in.dream_lab.goffish.godb.HueristicsLoad;
import in.dream_lab.goffish.godb.MapValue;
import in.dream_lab.goffish.godb.reach.ReachMessage.InEdgesWriter;
import in.dream_lab.goffish.godb.reach.ReachMessage.ResultsReader;
import in.dream_lab.goffish.godb.reach.ReachMessage.ResultsWriter;
import in.dream_lab.goffish.godb.reach.ReachMessage.RevisitTraversalReader;
import in.dream_lab.goffish.godb.reach.ReachMessage.RevisitTraversalWriter;
import in.dream_lab.goffish.godb.reach.ReachMessage.StopReader;
import in.dream_lab.goffish.godb.reach.ReachMessage.StopWriter;
import in.dream_lab.goffish.godb.reach.TraversalStep.TraversalWithPath;
import in.dream_lab.goffish.godb.util.DataReader;
import in.dream_lab.goffish.godb.util.DataWriter;


public class DoReachability extends
        AbstractSubgraphComputation<ReachState, MapValue, MapValue, ReachMessage, LongWritable, LongWritable, LongWritable>
        implements ISubgraphWrapup {

	public static final Log LOG = LogFactory.getLog(DoReachability.class);
	//Clear this at the end of the query
	
	
	// FIXME: We're copying this to the subgraph state in sstep 0. Is that fine?
	String queryParam;
	IReachRootQuerier rootQuerier;
	Double networkCoeff=49.57;
	Hueristics heuristics=null;
	/**
	 * Initialize BFS query with query string
	 * 
	 * @param initMsg
	 */
	public DoReachability(String initMsg) {
		String[] tokens = initMsg.split(" ");
		if (tokens.length != 2) throw new IllegalArgumentException("Expect 2 parameters: 'BFS [-i|-ni] [bfsQuery]', "
		        + "where -i indicates indexed, and -ni indicates non-indexed query.\nFound:" + initMsg);

		if ("-i".equals(tokens[0]))
			rootQuerier = new IndexedQuerierTopN();
		else if ("-ni".equals(tokens[0]))
			rootQuerier = new NonIndexedQuerier();
		else
			throw new IllegalArgumentException(
			        "Expect -i or -ni for the first parameter: 'BFS [-i|-ni] [bfsQuery]'; Found" + tokens[0]);

		queryParam = tokens[1];
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
		ISubgraph<ReachState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
		ReachState state = subgraph.getSubgraphValue();

		state.rootQuerier = rootQuerier;

		if (queryParam == null) {
			throw new RuntimeException("Invalid input query. Found NULL");
		}

		// Parse and load queries
		// TODO: Should this be part of application superstep to include its timing?
		if (LOG.isInfoEnabled()) LOG.info("***************ARGUMENTS************** :" + queryParam);
		state.query = new ReachQuery(queryParam);

		// init data structures in state
		// results for traversed edges for local root vertices
		state.results = new HashMap<>();

		// Load index
		getSubgraph().getSubgraphId().get();
		state.rootQuerier.loadIndex(getSubgraph().getSubgraphId().get());
		
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
        ReachMessage msg = new ReachMessage(in);

        sendMessage(new LongWritable(remoteSubgraphMessage.getKey()),msg);
}
	  
	}
	
	/**
	 * Accumulate Inedges
	 */
	private void doSuperstep1(Iterable<IMessage<LongWritable, ReachMessage>> messageList) {
	 
    for (IMessage<LongWritable, ReachMessage> _message: messageList){
            
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


	////////////////////////////////////////////////////////////////
	// SUPERSTEP 1
	//
	////////////////////////////////////////////////////////////////
	/**
	 * Queries the root vertices that match, and adds them to the subgraph's
	 * traversal state. Initializes the bitsets for visited, and sets the visited
	 * for the root vertex to true.
	 * 
	 * Add the root vertices to an empty traversal queue, and returns it.
	 * RuntimeException
	 * @throws IOException
	 * 
	 */
	private Queue<TraversalWithPath> initRootVertices() throws IOException {

		ISubgraph<ReachState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
		long sgid = subgraph.getSubgraphId().get();
		ReachState state = subgraph.getSubgraphValue();
		
		int startPoint=new ReachabilityHeuristicsOptimizer(heuristics, networkCoeff).Optimize(state.query);
		startPoint=0;//for debugging
		if(startPoint==1){
		  state.forwardQueue=false;
		}
		// local root vertices
		List<Long> rootVertices = state.rootQuerier.queryRootVertices(getSubgraph(),startPoint);
		// visit each root vertex, set their bit index and value, and init their
		// results
		
		Queue<TraversalWithPath> traversalQueue = new ArrayDeque<>();
		if(rootVertices==null){
		  return traversalQueue;
		}
		for (long root : rootVertices) {
			// Add result container for the root vertex
			ArrayList<Path> result = new ArrayList<Path>();
			state.results.put(root, result);

			
//			System.out.println("ARG:" + queryParam + "  ROOT:" + root + " SubgraphId:" + subgraph.getSubgraphId());
			// add root vertex to traversal queue with depth 0
			// provided the depth is greater than 0
			Path p=new Path(root);
			if (state.query.getDepth() > 0) traversalQueue.add(new TraversalWithPath(sgid, root, root, 0,p));
		}

		return traversalQueue;
	}

	/**
	 * If reverse traversal is selected by the optimizer
	 * @param queue
	 * @param remoteTraversalMap
	 * @param remoteResultsMap
	 * @throws IOException
	 */
	void reverseTraversalBackup(Queue<TraversalWithPath> queue,Map<Long, RevisitTraversalWriter> remoteTraversalMap,Map<Long, ResultsWriter> remoteResultsMap) throws IOException{
	  ISubgraph<ReachState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
          long sgid = subgraph.getSubgraphId().get();
          ReachState state = subgraph.getSubgraphValue();
          TraversalWithPath step;
          String propertyName=state.query.getSourcePropertyName();
          String propertyValue=state.query.getSourcePropertyValue().toString();
          while ((step = queue.poll()) != null) {

                  // CONTINUE IF depth of the STEP is over depth of query
                  if (step.depth > state.query.getDepth()) {
                          continue;
                  }

                  // DO NEXT STEP OF LOCAL BFS FOR VERTEX
                  IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex =
                          subgraph.getVertexById(new LongWritable(step.targetVertex));

                  // iterate through out edges
                  if(getSubgraph().getSubgraphValue().InEdges.containsKey(currentVertex.getVertexId().get()))
                    for(Map.Entry<Long, EdgeAttr> edgeMap: getSubgraph().getSubgraphValue().InEdges.get(currentVertex.getVertexId().get()).entrySet()) {

                          IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex =
                                  getSubgraph().getVertexById(new LongWritable(edgeMap.getKey()));
                          
                          long otherVID = edgeMap.getKey();
                          
                          
                          Path modifiedPath = step.path.getCopy();
                          modifiedPath.addEV(edgeMap.getValue().getEdgeId(), otherVID);
                          // if endvertex is found then send results to rootSubgraph and send stop message to all partitions
                          // to reduce depth of reachability query
                          if(!edgeMap.getValue().isRemote()){
                            String vertexValue = otherVertex.getValue().get(propertyName).toString();
                            if (propertyValue.toString().equals(vertexValue)) {
                           // Root vertex is local. Add Resultant Path from the traversal step to results.
                              if (step.rootSubgraph == sgid) {
                                ArrayList<Path> results = state.results.get(step.rootVertex);
                                results.add(step.path);
                              } else {
                                      // Root vertex is remote. Add to result message for the remote root
                                      // subgraph/vertex.
                                      // NOTE: A double map is more space efficient for messages since we
                                      // do not duplicate the root vertex and can copy the writer directly
                                      // to the remote root's resultset, but may not be time
                                      ResultsWriter remoteResults = remoteResultsMap.get(step.rootSubgraph);
                                      if (remoteResults == null) {
                                              remoteResults = new ResultsWriter();
                                              remoteResultsMap.put(step.rootSubgraph, remoteResults);
                                      }

                                      remoteResults.addResult(step.rootVertex, step.targetVertex, edgeMap.getValue().getEdgeId(), otherVID,step.path);
                              }
                              
                              if(step.depth < state.query.getDepth())
                                  sendStopMessage(step.depth);
                            }
                           
                          }
                          

                          

                          // if depth has not been reached, add the vertex to the traversal list
                          // step.depth is the depth (edges) to currentVertex. So depth to
                          // otherVertex that has just been visited is that+1.
                          if ((step.depth + 1) < state.query.getDepth()) {
                                  
                                  if (!otherVertex.isRemote()){ // other vertex is local
                                          
                                          queue.add(new TraversalWithPath(step.rootSubgraph, step.rootVertex, otherVID, step.depth + 1,step.path));
                                          
                                  }
                                  else {
                                          // add it to a remote list, to be sent to remote partition where it continues execution
                                          @SuppressWarnings("unchecked")
                                          long remoteSGID =
                                                  ((IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>) otherVertex)
                                                          .getSubgraphId().get();
                                          RevisitTraversalWriter traversalMessage = remoteTraversalMap.get(remoteSGID);
                                          if (traversalMessage == null) {
                                                  traversalMessage = new RevisitTraversalWriter();
                                                  remoteTraversalMap.put(remoteSGID, traversalMessage);
                                          }
                                          traversalMessage.addTraversal(step.rootSubgraph, step.rootVertex, otherVID, step.depth + 1,step.path);
                                  }
                          }
                          // else, skip adding. This traversal path has terminated.
                  }
          } // done with one vertex traversal

	}

	
	       void reverseTraversal(Queue<TraversalWithPath> queue,Map<Long, RevisitTraversalWriter> remoteTraversalMap,Map<Long, ResultsWriter> remoteResultsMap) throws IOException{
	          ISubgraph<ReachState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
	          long sgid = subgraph.getSubgraphId().get();
	          ReachState state = subgraph.getSubgraphValue();
	          TraversalWithPath step;
	          String propertyName=state.query.getSourcePropertyName();
	          String propertyValue=state.query.getSourcePropertyValue().toString();
	          while ((step = queue.poll()) != null) {

	                  // CONTINUE IF depth of the STEP is over depth of query
	                  if (step.depth > state.query.getDepth()) {
	                          continue;
	                  }

	                  // DO NEXT STEP OF LOCAL BFS FOR VERTEX
	                  IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex =
	                          subgraph.getVertexById(new LongWritable(step.targetVertex));

	                  // iterate through inedges:change this after BiVertex is implemented 
	                  if(getSubgraph().getSubgraphValue().InEdges.containsKey(currentVertex.getVertexId().get()))
	                    for(Map.Entry<Long, EdgeAttr> edgeMap: getSubgraph().getSubgraphValue().InEdges.get(currentVertex.getVertexId().get()).entrySet()) {


	                          IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex =
	                                  getSubgraph().getVertexById(new LongWritable(edgeMap.getKey()));
	                          long otherVID = edgeMap.getKey();
	                          String vertexValue ="";
	                          
//	                          step.path.addEV(edge.getEdgeId().get(), otherVID);
	                          Path modifiedPath = step.path.getCopy();
	                          modifiedPath.addEV(edgeMap.getValue().getEdgeId(), otherVID);
//	                          System.out.println("ACTUALPATH:" +step.path.toString());
//	                          System.out.println("MODIFIEDPATH:" + modifiedPath.toString());
	                          
	                          // if endvertex is found then send results to rootSubgraph and send stop message to all partitions
	                          // to reduce depth of reachability query
	                          if(!edgeMap.getValue().isRemote()){
	                            vertexValue= otherVertex.getValue().get(propertyName).toString();
//	                            System.out.println("Comparing Vertex Value:" + vertexValue + " with Property Value:" + propertyValue);
	                            if (propertyValue.equals(vertexValue)) {
//	                              System.out.println("MATCH FOUND");
	                              // Root vertex is local. Add Resultant Path from the traversal step to results.
	                                 if (step.rootSubgraph == sgid) {
	                                   ArrayList<Path> results = state.results.get(step.rootVertex);
	                                   results.add(modifiedPath);
	                                 } else {
	                                         // Root vertex is remote. Add to result message for the remote root
	                                         // subgraph/vertex.
	                                         // NOTE: A double map is more space efficient for messages since we
	                                         // do not duplicate the root vertex and can copy the writer directly
	                                         // to the remote root's resultset, but may not be time
	                                         ResultsWriter remoteResults = remoteResultsMap.get(step.rootSubgraph);
	                                         if (remoteResults == null) {
	                                                 remoteResults = new ResultsWriter();
	                                                 remoteResultsMap.put(step.rootSubgraph, remoteResults);
	                                         }
	                                         
	                                         remoteResults.addResult(step.rootVertex, step.targetVertex, edgeMap.getValue().getEdgeId(), otherVID,modifiedPath);
	                                 }
	                                 
	                                 if(step.depth < state.query.getDepth())
	                                     state.query.setDepth(step.depth);
	                                     sendStopMessage(step.depth);
	                                     continue;
	                               }
	                          }
	                          
	                          // Visit vertex at depth step.depth+1 (even if visited before)

	                          

	                          // if depth has not been reached, add the vertex to the traversal list
	                          // step.depth is the depth (edges) to currentVertex. So depth to
	                          // otherVertex that has just been visited is that+1.
	                          if ((step.depth + 1) < state.query.getDepth()) {
	                                  
	                                  if (!edgeMap.getValue().isRemote()){ // other vertex is local
	                                          
	                                          queue.add(new TraversalWithPath(step.rootSubgraph, step.rootVertex, otherVID, step.depth + 1,modifiedPath));
	                                          
	                                  }
	                                  else {
	                                          // add it to a remote list, to be sent to remote partition where it continues execution
	                                          @SuppressWarnings("unchecked")
	                                          long remoteSGID = edgeMap.getValue().getSinkSubgraphId();
	                                          RevisitTraversalWriter traversalMessage = remoteTraversalMap.get(remoteSGID);
	                                          if (traversalMessage == null) {
	                                                  traversalMessage = new RevisitTraversalWriter();
	                                                  remoteTraversalMap.put(remoteSGID, traversalMessage);
	                                          }
	                                          traversalMessage.addTraversal(step.rootSubgraph, step.rootVertex, otherVID, step.depth + 1,modifiedPath);
	                                  }
	                          }
	                          // else, skip adding. This traversal path has terminated.
	                  }
	          } // done with one vertex traversal

	        }
	
	
	
	void forwardTraversal(Queue<TraversalWithPath> queue,Map<Long, RevisitTraversalWriter> remoteTraversalMap,Map<Long, ResultsWriter> remoteResultsMap) throws IOException{
          ISubgraph<ReachState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
          long sgid = subgraph.getSubgraphId().get();
          ReachState state = subgraph.getSubgraphValue();
          TraversalWithPath step;
          String propertyName=state.query.getSinkPropertyName();
          String propertyValue=state.query.getSinkPropertyValue().toString();
          while ((step = queue.poll()) != null) {

                  // CONTINUE IF depth of the STEP is over depth of query
                  if (step.depth > state.query.getDepth()) {
                          continue;
                  }

                  // DO NEXT STEP OF LOCAL BFS FOR VERTEX
                  IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex =
                          subgraph.getVertexById(new LongWritable(step.targetVertex));

                  // iterate through out edges
                  for (IEdge<MapValue, LongWritable, LongWritable> edge : currentVertex.getOutEdges()) {

                          IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex =
                                  getSubgraph().getVertexById(edge.getSinkVertexId());
                          long otherVID = otherVertex.getVertexId().get();
                          String vertexValue ="";
         
//                          step.path.addEV(edge.getEdgeId().get(), otherVID);
                          Path modifiedPath = step.path.getCopy();
                          modifiedPath.addEV(edge.getEdgeId().get(), otherVID);
                          System.out.println("ACTUALPATH:" +step.path.toString());
                          System.out.println("MODIFIEDPATH:" + modifiedPath.toString());
                          
                          // if endvertex is found then send results to rootSubgraph and send stop message to all partitions
                          // to reduce depth of reachability query
                          if(!otherVertex.isRemote()){
                            vertexValue= otherVertex.getValue().get(propertyName).toString();
                            if (propertyValue.toString().equals(vertexValue)) {
//                              System.out.println("MATCH FOUND");
                              // Root vertex is local. Add Resultant Path from the traversal step to results.
                                 if (step.rootSubgraph == sgid) {
                                   ArrayList<Path> results = state.results.get(step.rootVertex);
                                   results.add(modifiedPath);
                                 } else {
                                         // Root vertex is remote. Add to result message for the remote root
                                         // subgraph/vertex.
                                         // NOTE: A double map is more space efficient for messages since we
                                         // do not duplicate the root vertex and can copy the writer directly
                                         // to the remote root's resultset, but may not be time
                                         ResultsWriter remoteResults = remoteResultsMap.get(step.rootSubgraph);
                                         if (remoteResults == null) {
                                                 remoteResults = new ResultsWriter();
                                                 remoteResultsMap.put(step.rootSubgraph, remoteResults);
                                         }
                                         
                                         remoteResults.addResult(step.rootVertex, step.targetVertex, edge.getEdgeId().get(), otherVID,modifiedPath);
                                 }
                                 
                                 if(step.depth < state.query.getDepth())
                                     state.query.setDepth(step.depth);
                                     sendStopMessage(step.depth);
                                     continue;
                               }
                          }
                          
                          // Visit vertex at depth step.depth+1 (even if visited before)

                          

                          // if depth has not been reached, add the vertex to the traversal list
                          // step.depth is the depth (edges) to currentVertex. So depth to
                          // otherVertex that has just been visited is that+1.
                          if ((step.depth + 1) < state.query.getDepth()) {
                                  
                                  if (!otherVertex.isRemote()){ // other vertex is local
                                          
                                          queue.add(new TraversalWithPath(step.rootSubgraph, step.rootVertex, otherVID, step.depth + 1,modifiedPath));
                                          
                                  }
                                  else {
                                          System.out.println("REMOTE:" + modifiedPath.toString());
                                          // add it to a remote list, to be sent to remote partition where it continues execution
                                          @SuppressWarnings("unchecked")
                                          long remoteSGID =
                                                  ((IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>) otherVertex)
                                                          .getSubgraphId().get();
                                          RevisitTraversalWriter traversalMessage = remoteTraversalMap.get(remoteSGID);
                                          if (traversalMessage == null) {
                                                  traversalMessage = new RevisitTraversalWriter();
                                                  remoteTraversalMap.put(remoteSGID, traversalMessage);
                                          }
                                          traversalMessage.addTraversal(step.rootSubgraph, step.rootVertex, otherVID, step.depth + 1,modifiedPath);
                                  }
                          }
                          // else, skip adding. This traversal path has terminated.
                  }
          } // done with one vertex traversal

        }

	
	

	private void sendStopMessage(int depth) {
    // TODO Auto-generated method stub
	StopWriter sw=new StopWriter(depth);  
        ReachMessage msg=new ReachMessage(sw);
        sendToAll(msg);
        }

////////////////////////////////////////////////////////////////
// SUPERSTEP N
//
////////////////////////////////////////////////////////////////
  @Override
	public void compute(Iterable<IMessage<LongWritable, ReachMessage>> messages) throws IOException {

		ISubgraph<ReachState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
		long sgid = subgraph.getSubgraphId().get();
		ReachState state = subgraph.getSubgraphValue();

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
		

		////////////////////////////////////////////
		// SUPERSTEP 2: QUERY FOR ROOT VERTICES
		////////////////////////////////////////////
		LOG.info("Starting Query Execution");
		// Local BFS queue
		Queue<TraversalWithPath> queue;
		// Map from remote SGID to list of steps for it
		// Each writer will contains <(long)rootSGID, (long)rootVID,
		// (long)targetVID, (int)depth>+
		Map<Long, RevisitTraversalWriter> remoteTraversalMap = new HashMap<>();
		// Map from remote SGID to a Map for root vertex in that subgraph and its
		// result edge list
		// Each writer will contain <(long)sourceVID, (long)edgeID, (long)sinkVID>+
		// TODO: write the count of triples since open ended length
		Map<Long, ResultsWriter> remoteResultsMap = new HashMap<>();

		if (getSuperstep() == 2) {
			queue = initRootVertices();
		} // Done with special functions for sstep 1. Continue with rest...
		else
			queue = new ArrayDeque<>();

		///////////////////////////////////////
		// SUPERSTEP >= 2
		// 1. Parse input messages
		// 1a. Add results message to local root vertex's results
		// 1b. If traversal message target is not visited, add to results and if
		// depth not reached, add to BFS queue
		// 2. Do local BFS traversal on queue
		// 3. Send results and traversal messages
		////////////////////////////////////////////


		// ==========================================
		// PARSE INPUT MESSAGES
		long startMessageReceiveTimeMillis = System.currentTimeMillis();
		for (IMessage<LongWritable, ReachMessage> message : messages) {
			ReachMessage msg = message.getMessage();

			// HANDLE RESULTS MESSAGES
			if (msg.getMessageType() == ReachMessage.RESULTS_READER) {
				// Copy the contents of the results message (bytes of triples) to the
				// existing results for the root vertex present in this subgraph.
				ResultsReader resultsReader = msg.getResultsReader();
				Map<Long, ArrayList<DataReader>> newResults = resultsReader.getResults();
				// each entry has root vertex ID as key and bytes of triples <source
				// VID, edge ID, sink ID> as value
				for (Entry<Long, ArrayList<DataReader>> entry : newResults.entrySet()) {
					ArrayList<Path> currResults = state.results.get(entry.getKey());
					// sanity check that the root vertex is local
					if (currResults == null) {
						// if this SG has the root, there must be results entry created for
						// it when the root vertices were queried for
						throw new RuntimeException(
						        "Received a result message for which current subgraph does not have a result entry. Root vertex ID:"
						                + entry.getKey() + "; Current Subgraph ID:" + sgid);
					}
//					System.out.println("Got Remote Results back");  
					for(DataReader reader:entry.getValue() ){
					// copy all triples from remote results message to local resultset
					  
					    currResults.add(getPathsFromBytes(reader.getBytes()));
					}
				}

			} // done with result message
			else
			// HANDLE TRAVERSAL MESSAGES
			if (msg.getMessageType() == ReachMessage.REVISIT_TRAVERSAL_READER) {
				// Add step to current queue
				RevisitTraversalReader resultsReader = msg.getRevisitTraversalReader();
				List<TraversalWithPath> stepList = resultsReader.getRevisitTraversals();

				for (TraversalWithPath stepResult : stepList) {
					// sanity check that the target vertex is local
					long otherVID = stepResult.targetVertex;
					IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex =
					        subgraph.getVertexById(new LongWritable(otherVID));
					if (otherVertex == null) {
						throw new RuntimeException(
						        "Received a traversal message for which current subgraph does not have the target vertex:"
						                + stepResult + "; Current Subgraph ID:" + sgid);
					}

					// add traversal message to local queue
//					System.out.println("Adding Remote Traversal to Queue:" + stepResult.path.toString());
					queue.add(stepResult);
				} // end iteration over traversal steps
			} // done with traversal message
			else if (msg.getMessageType() == ReachMessage.STOP_READER) {
                           StopReader stopReader = msg.getStopReader();
                           int newDepth = stopReader.getNewDepth();
                           if(newDepth < state.query.getDepth()){
                             state.query.setDepth(newDepth);
                           }
                         
                  } // done with stop message
                  else
				throw new RuntimeException("Unknown message type seen: " + message);
		}
		state.messageReceiveTimeMillis += (System.currentTimeMillis() - startMessageReceiveTimeMillis);


		// ==========================================
		// DO LOCAL BFS TRAVERSALS AND CHECK FOR END VERTICES
		long startLocalBFSTimeMillis = System.currentTimeMillis();
		
		if(state.forwardQueue){
		  forwardTraversal(queue, remoteTraversalMap, remoteResultsMap);
		}else{
		  reverseTraversal(queue,remoteTraversalMap,remoteResultsMap);
		}
		
		state.localBFSTimeMillis += (System.currentTimeMillis() - startLocalBFSTimeMillis);


		// ==========================================
		// SEND RESULTS AND TRAVERSAL MESSAGES
		long startMessageSendTimeMillis = System.currentTimeMillis();
		// Send traversal messages, one per remote subgraph in the map
		// Each traversal message will contains <(long)rootSGID, (long)rootVID,
		// (long)targetVID, (int)depth>+
		for (Entry<Long, RevisitTraversalWriter> entry : remoteTraversalMap.entrySet()) {
			sendMessage(new LongWritable(entry.getKey()), new ReachMessage(entry.getValue()));
		}

		// Result message contains <(long)sourceVID, (long)edgeID, (long)sinkVID>+
		// TODO: We're keeping multiple copies in Map and in message. If we're
		// bloating memory, we may need to move from Map to message.
		for (Entry<Long, ResultsWriter> entry : remoteResultsMap.entrySet()) {
		        System.out.println("Sending Results Back:" + entry.getValue().toString());
			sendMessage(new LongWritable(entry.getKey()), new ReachMessage(entry.getValue()));
		}

		state.messageSendTimeMillis += (System.currentTimeMillis() - startMessageSendTimeMillis);


		// ==========================================
		// VOTE TO HALT IN SUPERSTEPS 1 AND BEYOND
		voteToHalt();

	}


	private Path getPathsFromBytes(byte[] pathBytes) throws IOException {
    // TODO Auto-generated method stub
	  System.out.println("GETTING PATH:");
	  DataReader reader = DataReader.newInstance(pathBytes);
//	  ArrayList<Path> pathList=new ArrayList<>();
	  boolean eof = false;
	  Path p = new Path(reader.readLong());
	  while (!eof) {
	      try {
//	           int pathSize=reader.readInt();
//	           pathSize=reader.readInt();
//	           System.out.println("Path Size:" + pathSize);
	           

	             p.addEV(reader.readLong(), reader.readLong());
	           
	          // read and use data
	      } catch (IOException e) {
	          eof = true;
	      }
	  }
	  
    return p;
  }


  @Override
	public void wrapup() throws IOException {
		// Writing results
	        LOG.info("Ending Query Execution");
		ReachState state = getSubgraph().getSubgraphValue();
		LOG.info("BFS TIME,messageReceiveTimeMillis," + state.messageReceiveTimeMillis + ",messageSendTimeMillis,"
		        + state.messageSendTimeMillis + ",localBFSTimeMillis," + state.localBFSTimeMillis);
		LOG.info("BFS Results (source_VID,edge_ID,sink_VID)");
		for (Entry<Long, ArrayList<Path>> entry : state.results.entrySet()) {
			
			ArrayList<Path> pathList = entry.getValue();
			LOG.info("Root Vertex," + entry.getKey() + ", PathCount," + entry.getValue().size());
			System.out.println("RESULT");
			for (Path p:pathList) {
				System.out.println(p.toString());
			}
		}

		// clearing Subgraph Value for next query
		// for cleaning up the subgraph value so that Results could be cleared while
		// Inedges won't be cleared so that it could be reused.
		state.clear();
		
	}
}

