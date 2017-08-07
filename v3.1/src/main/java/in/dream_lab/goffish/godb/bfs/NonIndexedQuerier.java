package in.dream_lab.goffish.godb.bfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.MapValue;


public class NonIndexedQuerier implements IBFSRootQuerier {

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
	public List<Long> queryRootVertices(ISubgraph<BFSState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph) throws IOException {
		BFSState state = subgraph.getSubgraphValue();
		String queryProperty = state.query.getPropertyName();
		Object queryValue = state.query.getPropertyValue();
		List<Long> rootVertices = new ArrayList<>();

		for (IVertex<MapValue, MapValue, LongWritable, LongWritable> vertex : subgraph.getLocalVertices()) {
			if (vertex.isRemote()) continue;
			String val=vertex.getValue().get(queryProperty.toString());
			String vertexValue; 
			if(val==null)
			  continue;
			  
			vertexValue = val.toString();
			System.out.println("VertexVal:" + vertexValue);
			
			// FIXME: Only supporting string value type for now
			if (queryValue.toString().equals(vertexValue)) rootVertices.add(vertex.getVertexId().get());
		}
		// return root vertices
		return rootVertices;
	}
	
	public void clear() {}
}
