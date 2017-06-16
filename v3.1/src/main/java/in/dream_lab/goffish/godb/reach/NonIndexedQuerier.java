package in.dream_lab.goffish.godb.reach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.MapValue;


public class NonIndexedQuerier implements IReachRootQuerier {

	public NonIndexedQuerier() {
	}

	/**
	 * Do not load the index.
	 */
	public boolean loadIndex(long sgid) {
		return false;
	}

	/**
	 * This queries for the root vertices in this subgraph and returns the list of
	 * vertex IDs that match.
	 * 
	 * Can be overriden to implement as indexed or non indexed versions. This is
	 * the indexed version below.
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<Long> queryRootVertices(ISubgraph<ReachState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph,int _startPoint) throws IOException {
		ReachState state = subgraph.getSubgraphValue();
		ReachQuery query=state.query;
		List<Long> rootVertices = new ArrayList<>();
		int startPoint=_startPoint;
                String propertyName=null;
                Object propertyValue=null;
                if(startPoint==0){
                  propertyName=query.getSourcePropertyName();
                  propertyValue=query.getSourcePropertyValue();
                }
                else if(startPoint==1){
                  propertyName=query.getSinkPropertyName();
                  propertyValue=query.getSinkPropertyValue();
                } else
      try {
        throw new Exception("Start Point Not Valid");
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
		
		for (IVertex<MapValue, MapValue, LongWritable, LongWritable> vertex : subgraph.getLocalVertices()) {
			if (vertex.isRemote()) continue;
			String vertexValue = vertex.getValue().get(propertyName).toString();
			// FIXME: Only supporting string value type for now
			if (propertyValue.toString().equals(vertexValue)) rootVertices.add(vertex.getVertexId().get());
		}
		// return root vertices
		return rootVertices;
	}
	
	public void clear() {}
}
