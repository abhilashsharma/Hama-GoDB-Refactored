package in.dream_lab.goffish.godb.bfs;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.ISubgraphWrapup;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.MapValue;
import in.dream_lab.goffish.godb.bfs.BFSMessage.ResultsReader;
import in.dream_lab.goffish.godb.bfs.BFSMessage.ResultsWriter;
import in.dream_lab.goffish.godb.bfs.BFSMessage.RevisitTraversalReader;
import in.dream_lab.goffish.godb.bfs.BFSMessage.RevisitTraversalWriter;
import in.dream_lab.goffish.godb.util.DataReader;
import in.dream_lab.goffish.godb.util.DataWriter;


public class DoBFSRevisit extends
        AbstractSubgraphComputation<BFSState, MapValue, MapValue, BFSMessage, LongWritable, LongWritable, LongWritable>
        implements ISubgraphWrapup {

	public static final Log LOG = LogFactory.getLog(DoBFSRevisit.class);

	// FIXME: We're copying this to the subgraph state in sstep 0. Is that fine?
	String queryParam;
	IBFSRootQuerier rootQuerier;

	/**
	 * Initialize BFS query with query string
	 * 
	 * @param initMsg
	 */
	public DoBFSRevisit(String initMsg) {
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
	 */
	private void doSuperstep0() {
		ISubgraph<BFSState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
		BFSState state = subgraph.getSubgraphValue();

		state.rootQuerier = rootQuerier;

		if (queryParam == null) {
			throw new RuntimeException("Invalid input query. Found NULL");
		}

		// Parse and load queries
		// TODO: Should this be part of application superstep to include its timing?
		if (LOG.isInfoEnabled()) LOG.info("***************ARGUMENTS************** :" + queryParam);
		state.query = new BFSQuery(queryParam);

		// init data structures in state
		// results for traversed edges for local root vertices
		state.results = new HashMap<>();

		// Load index
		getSubgraph().getSubgraphId().get();
		state.rootQuerier.loadIndex(getSubgraph().getSubgraphId().get());
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
	 * 
	 * @throws IOException
	 * 
	 */
	private Queue<TraversalStep> initRootVertices() throws IOException {

		ISubgraph<BFSState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
		long sgid = subgraph.getSubgraphId().get();
		BFSState state = subgraph.getSubgraphValue();

		// local root vertices
		List<Long> rootVertices = state.rootQuerier.queryRootVertices(getSubgraph());
		// visit each root vertex, set their bit index and value, and init their
		// results
		
		Queue<TraversalStep> traversalQueue = new ArrayDeque<>();
		if(rootVertices==null){
		  return traversalQueue;
		}
		for (long root : rootVertices) {
			// Add result container for the root vertex
			DataWriter resultWriter = DataWriter.newInstance();
			state.results.put(root, resultWriter);

			// add special edge from Long.MIN_VALUE to this root vertex in its
			// edge-list results
			resultWriter.writeLong(Long.MIN_VALUE);
			resultWriter.writeLong(root);
//			System.out.println("ARG:" + queryParam + "  ROOT:" + root + " SubgraphId:" + subgraph.getSubgraphId());
			// add root vertex to traversal queue with depth 0
			// provided the depth is greater than 0
			if (state.query.getDepth() > 0) traversalQueue.add(new TraversalStep(sgid, root, root, 0));
		}

		return traversalQueue;
	}


	////////////////////////////////////////////////////////////////
	// SUPERSTEP N
	//
	////////////////////////////////////////////////////////////////

	@Override
	public void compute(Iterable<IMessage<LongWritable, BFSMessage>> messages) throws IOException {

		ISubgraph<BFSState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
		long sgid = subgraph.getSubgraphId().get();
		BFSState state = subgraph.getSubgraphValue();

		////////////////////////////////////////////
		// SUPERSTEP 0: LOAD QUERY AND INITIALIZE LUCENE
		////////////////////////////////////////////

		if (getSuperstep() == 0) {

			doSuperstep0();
			return;
		} // Done with sstep 0. Finishing compute. Do NOT vote to halt.


		////////////////////////////////////////////
		// SUPERSTEP 1: QUERY FOR ROOT VERTICES
		////////////////////////////////////////////
		LOG.info("Starting Query Execution");
		// Local BFS queue
		Queue<TraversalStep> queue;
		// Map from remote SGID to list of steps for it
		// Each writer will contains <(long)rootSGID, (long)rootVID,
		// (long)targetVID, (int)depth>+
		Map<Long, RevisitTraversalWriter> remoteTraversalMap = new HashMap<>();
		// Map from remote SGID to a Map for root vertex in that subgraph and its
		// result edge list
		// Each writer will contain <(long)sourceVID, (long)edgeID, (long)sinkVID>+
		// TODO: write the count of triples since open ended length
		Map<Long, ResultsWriter> remoteResultsMap = new HashMap<>();

		if (getSuperstep() == 1) {
			queue = initRootVertices();
		} // Done with special functions for sstep 1. Continue with rest...
		else
			queue = new ArrayDeque<>();

		///////////////////////////////////////
		// SUPERSTEP >= 1
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
		for (IMessage<LongWritable, BFSMessage> message : messages) {
			BFSMessage msg = message.getMessage();

			// HANDLE RESULTS MESSAGES
			if (msg.getMessageType() == BFSMessage.RESULTS_READER) {
				// Copy the contents of the results message (bytes of triples) to the
				// existing results for the root vertex present in this subgraph.
				ResultsReader resultsReader = msg.getResultsReader();
				Map<Long, DataReader> newResults = resultsReader.getResults();
				// each entry has root vertex ID as key and bytes of triples <source
				// VID, edge ID, sink ID> as value
				for (Entry<Long, DataReader> entry : newResults.entrySet()) {
					DataWriter currResults = state.results.get(entry.getKey());
					// sanity check that the root vertex is local
					if (currResults == null) {
						// if this SG has the root, there must be results entry created for
						// it when the root vertices were queried for
						throw new RuntimeException(
						        "Received a result message for which current subgraph does not have a result entry. Root vertex ID:"
						                + entry.getKey() + "; Current Subgraph ID:" + sgid);
					}

					// copy all triples from remote results message to local resultset
					currResults.write(entry.getValue().getBytes());
				}

			} // done with result message
			else
			// HANDLE TRAVERSAL MESSAGES
			if (msg.getMessageType() == BFSMessage.REVISIT_TRAVERSAL_READER) {
				// Add step to current queue
				RevisitTraversalReader resultsReader = msg.getRevisitTraversalReader();
				List<TraversalStep> stepList = resultsReader.getRevisitTraversals();

				for (TraversalStep stepResult : stepList) {
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
					queue.add(stepResult);
				} // end iteration over traversal steps
			} // done with traversal message
			else
				throw new RuntimeException("Unknown message type seen: " + message);
		}
		state.messageReceiveTimeMillis += (System.currentTimeMillis() - startMessageReceiveTimeMillis);


		// ==========================================
		// DO LOCAL BFS TRAVERSALS
		long startLocalBFSTimeMillis = System.currentTimeMillis();

		TraversalStep step;
		while ((step = queue.poll()) != null) {

			// COMPLETED BFS DEPTH
			if (step.depth == state.query.getDepth()) {
				// should not happen since we are traversing step's children that are at
				// step.depth+1
				throw new RuntimeException("Found an invalid depth for step: " + step);
			}

			// DO NEXT STEP OF LOCAL BFS FOR VERTEX
			IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex =
			        subgraph.getVertexById(new LongWritable(step.targetVertex));

			// iterate through out edges
			for (IEdge<MapValue, LongWritable, LongWritable> edge : currentVertex.getOutEdges()) {

				IVertex<MapValue, MapValue, LongWritable, LongWritable> otherVertex =
				        getSubgraph().getVertexById(edge.getSinkVertexId());
				long otherVID = otherVertex.getVertexId().get();

				// Visit vertex at depth step.depth+1 (even if visited before)

				// Root vertex is local. Add edge (source,edge,target) to its results.
				if (step.rootSubgraph == sgid) {
					DataWriter resultsWriter = state.results.get(step.rootVertex);
					resultsWriter.writeLong(step.targetVertex);
					resultsWriter.writeLong(edge.getEdgeId().get());
					resultsWriter.writeLong(otherVID);
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

					remoteResults.addResult(step.rootVertex, step.targetVertex, edge.getEdgeId().get(), otherVID);
				}

				// if depth has not been reached, add the vertex to the traversal list
				// step.depth is the depth (edges) to currentVertex. So depth to
				// otherVertex that has just been visited is that+1.
				if ((step.depth + 1) < state.query.getDepth()) {
					if (!otherVertex.isRemote()) // other vertex is local
						queue.add(new TraversalStep(step.rootSubgraph, step.rootVertex, otherVID, step.depth + 1));
					else {
						// add it to the traversal messages to be sent to
						@SuppressWarnings("unchecked")
						long remoteSGID =
						        ((IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>) otherVertex)
						                .getSubgraphId().get();
						RevisitTraversalWriter traversalMessage = remoteTraversalMap.get(remoteSGID);
						if (traversalMessage == null) {
							traversalMessage = new RevisitTraversalWriter();
							remoteTraversalMap.put(remoteSGID, traversalMessage);
						}
						traversalMessage.addTraversal(step.rootSubgraph, step.rootVertex, otherVID, step.depth + 1);
					}
				}
				// else, skip adding. This traversal path has terminated.
			}
		} // done with one vertex traversal

		state.localBFSTimeMillis += (System.currentTimeMillis() - startLocalBFSTimeMillis);


		// ==========================================
		// SEND RESULTS AND TRAVERSAL MESSAGES
		long startMessageSendTimeMillis = System.currentTimeMillis();
		// Send traversal messages, one per remote subgraph in the map
		// Each traversal message will contains <(long)rootSGID, (long)rootVID,
		// (long)targetVID, (int)depth>+
		for (Entry<Long, RevisitTraversalWriter> entry : remoteTraversalMap.entrySet()) {
			sendMessage(new LongWritable(entry.getKey()), new BFSMessage(entry.getValue()));
		}

		// Result message contains <(long)sourceVID, (long)edgeID, (long)sinkVID>+
		// TODO: We're keeping multiple copies in Map and in message. If we're
		// bloating memory, we may need to move from Map to message.
		for (Entry<Long, ResultsWriter> entry : remoteResultsMap.entrySet()) {
			sendMessage(new LongWritable(entry.getKey()), new BFSMessage(entry.getValue()));
		}

		state.messageSendTimeMillis += (System.currentTimeMillis() - startMessageSendTimeMillis);


		// ==========================================
		// VOTE TO HALT IN SUPERSTEPS 1 AND BEYOND
		voteToHalt();

	}


	@Override
	public void wrapup() throws IOException {
		// Writing results
	        LOG.info("Ending Query Execution");
		BFSState state = getSubgraph().getSubgraphValue();
		LOG.info("BFS TIME,messageReceiveTimeMillis," + state.messageReceiveTimeMillis + ",messageSendTimeMillis,"
		        + state.messageSendTimeMillis + ",localBFSTimeMillis," + state.localBFSTimeMillis);
		LOG.info("BFS Results (source_VID,edge_ID,sink_VID)");
		for (Entry<Long, DataWriter> entry : state.results.entrySet()) {
			// create reader from writer
			byte[] rawBytes = entry.getValue().getBytes();
			int count = rawBytes.length / (3 * 8);
			DataReader reader = DataReader.newInstance(rawBytes);
			LOG.info("Root Vertex," + entry.getKey() + ", Count," + count);
			for (int i = 0; i < count; i++) {
				System.out.printf("%d,%d,%d%n", reader.readLong(), reader.readLong(), reader.readLong());
			}
		}

		// clearing Subgraph Value for next query
		// for cleaning up the subgraph value so that Results could be cleared while
		// Inedges won't be cleared so that it could be reused.
		state.clear();
	}
}

