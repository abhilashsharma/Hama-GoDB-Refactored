package in.dream_lab.goffish.godb.bfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.godb.util.DataWriter;


public class BFSState implements Writable {


	// name and value and depth of the query for the root vertex
	BFSQuery query;

	// Indexed or non-indexed implementation of a querier over root vertices
	IBFSRootQuerier rootQuerier;

	// Map from local root vertex to the results accumulated for it.
	// Write contains the edge list, given as a triple of
	// <(long)sourceVID,(long)edgeID,(long)sinkVID>.
	Map<Long, DataWriter> results;

	// For each local/remote vertex, maintain a bitset on whether this vertex has
	// been visited by a local or remote root vertex.
	// The number of items in the bitset will grow to the total number of root
	// vertices in the entire graph.
	Map<Long, BitSet> visited;

	// keeps track of the bitmask index assigned so that new unique ones can be
	// given to remote root vertices seen for the first time
	int lastAssignedBitmaskIndex = 0;

	// For each local root vertex, assign a contiguous bitindex.
	// For each remote vertex seen in future, increment this contiguous bitindex
	// and store in map.
	//
	// Maps from a local/remote vertex to a local
	// monotonically
	// increasing bitindex for this root. Has to be maintained across supersteps
	// to support revisits (in directed graphs).
	Map<Long, Integer> rootToBitIndex;

	long messageReceiveTimeMillis = 0, messageSendTimeMillis = 0, localBFSTimeMillis = 0;

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void clear() {
		query = null;
		lastAssignedBitmaskIndex = 0;
		messageReceiveTimeMillis = 0;
		messageSendTimeMillis = 0;
		localBFSTimeMillis = 0;
		if(results != null) results.clear();
		if(visited != null) visited.clear();
		if(rootToBitIndex != null) rootToBitIndex.clear();
		if(rootQuerier != null) rootQuerier.clear();
	}


}
