package in.dream_lab.goffish.godb.path;

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
import in.dream_lab.goffish.godb.Path;
import in.dream_lab.goffish.godb.reach.ReachMessage;
import in.dream_lab.goffish.godb.path.PathMessage.InEdgesWriter;


public class DoTest extends
        AbstractSubgraphComputation<PathState, MapValue, MapValue, PathMessage, LongWritable, LongWritable, LongWritable>
        implements ISubgraphWrapup {

	public static final Log LOG = LogFactory.getLog(DoTest.class);
	//Clear this at the end of the query
	
	
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
		ISubgraph<PathState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
		PathState state = subgraph.getSubgraphValue();

//		state.rootQuerier = rootQuerier;
		queryParam="test";
		if (queryParam == null) {
			throw new RuntimeException("Invalid input query. Found NULL");
		}

		// Parse and load queries
		// TODO: Should this be part of application superstep to include its timing?
		if (LOG.isInfoEnabled()) LOG.info("***************ARGUMENTS************** :" + queryParam);
//		state.query = new ReachQuery(queryParam);

		// init data structures in state
		// results for traversed edges for local root vertices
//		state.results = new HashMap<>();

		// Load index
//		getSubgraph().getSubgraphId().get();
//		state.rootQuerier.loadIndex(getSubgraph().getSubgraphId().get());
		
		//Load Heuristics
//		heuristics=HueristicsLoad.getInstance();
		
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


	
	

////////////////////////////////////////////////////////////////
// SUPERSTEP N
//
////////////////////////////////////////////////////////////////
  @Override
	public void compute(Iterable<IMessage<LongWritable, PathMessage>> messages) throws IOException {

		ISubgraph<PathState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
		long sgid = subgraph.getSubgraphId().get();
		PathState state = subgraph.getSubgraphValue();

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
		PathState state = getSubgraph().getSubgraphValue();
		
		// clearing Subgraph Value for next query
		// for cleaning up the subgraph value so that Results could be cleared while
		// Inedges won't be cleared so that it could be reused.
//		state.clear();
		
	}
}

