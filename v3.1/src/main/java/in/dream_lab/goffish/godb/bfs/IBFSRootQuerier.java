package in.dream_lab.goffish.godb.bfs;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.godb.MapValue;


/**
 * (Optionally) loads index and performs query over subgraph (optionally using index)
 * 
 * @author simmhan
 *
 */
interface IBFSRootQuerier {

	boolean loadIndex(long sgid);

	List<Long> queryRootVertices(
	        ISubgraph<BFSState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph)
	        throws IOException;
	
	void clear();
}
