package in.dream_lab.goffish.godb.bfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.godb.util.DataReader;
import in.dream_lab.goffish.godb.util.DataWriter;


/**
 * Encapsulates reads and writes of result and traversal messages
 * 
 * @author simmhan
 *
 */
public class BFSMessage implements Writable {

	// Use LSB+1 and LSB+2 bits for type of message
	private static final byte TRAVERSAL = 0b0010;
	private static final byte RESULTS = 0b0000;
	private static final byte REVISIT_TRAVERSAL = 0b0100;

	private static final byte READER = 0b0000;
	private static final byte WRITER = 0b0001;

	// Use LSB 0 for reader, LSB 1 for writer
	public static final byte TRAVERSAL_READER = TRAVERSAL | READER;
	public static final byte TRAVERSAL_WRITER = TRAVERSAL | WRITER;

	public static final byte REVISIT_TRAVERSAL_READER = REVISIT_TRAVERSAL | READER;
	public static final byte REVISIT_TRAVERSAL_WRITER = REVISIT_TRAVERSAL | WRITER;

	public static final byte RESULTS_READER = RESULTS | READER;
	public static final byte RESULTS_WRITER = RESULTS | WRITER;

	private byte messageType;

	// A results reader/writer, or traversal reader/write object
	private Object innerMessage;

	public BFSMessage() {
		// FIXME: Is default constructor required for Writable?
		messageType = Byte.MIN_VALUE;
	}

	public BFSMessage(ResultsWriter results) {
		innerMessage = results;
		messageType = RESULTS_WRITER;
	}

	public BFSMessage(TraversalWriter traversal) {
		innerMessage = traversal;
		messageType = TRAVERSAL_WRITER;
	}

	public BFSMessage(RevisitTraversalWriter rtraversal) {
		innerMessage = rtraversal;
		messageType = REVISIT_TRAVERSAL_WRITER;
	}


	public byte getMessageType() {
		return messageType;
	}

	public ResultsWriter getResultsWriter() {
		if (messageType == RESULTS_WRITER) return (ResultsWriter) innerMessage;
		throw new RuntimeException("This BFSMessage is not a RESULTS_WRITER message. Message type=" + messageType);
	}

	public TraversalWriter getTraversalWriter() {
		if (messageType == TRAVERSAL_WRITER) return (TraversalWriter) innerMessage;
		throw new RuntimeException("This BFSMessage is not a TRAVERSAL_WRITER message. Message type=" + messageType);
	}

	public RevisitTraversalWriter getRevisitTraversalWriter() {
		if (messageType == REVISIT_TRAVERSAL_WRITER) return (RevisitTraversalWriter) innerMessage;
		throw new RuntimeException(
		        "This BFSMessage is not a REVISIT_TRAVERSAL_WRITER message. Message type=" + messageType);
	}

	public ResultsReader getResultsReader() {
		if (messageType == RESULTS_READER) return (ResultsReader) innerMessage;
		throw new RuntimeException("This BFSMessage is not a RESULTS_READER message. Message type=" + messageType);
	}

	public TraversalReader getTraversalReader() {
		if (messageType == TRAVERSAL_READER) return (TraversalReader) innerMessage;
		throw new RuntimeException("This BFSMessage is not a TRAVERSAL_READER message. Message type=" + messageType);
	}

	public RevisitTraversalReader getRevisitTraversalReader() {
		if (messageType == REVISIT_TRAVERSAL_READER) return (RevisitTraversalReader) innerMessage;
		throw new RuntimeException(
		        "This BFSMessage is not a REVISIT_TRAVERSAL_READER message. Message type=" + messageType);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (messageType == RESULTS_WRITER) {
			out.writeByte(RESULTS);
			((ResultsWriter) innerMessage).write(out);
		} else if (messageType == TRAVERSAL_WRITER) {
			out.writeByte(TRAVERSAL);
			((TraversalWriter) innerMessage).write(out);
		} else if (messageType == REVISIT_TRAVERSAL_WRITER) {
			out.writeByte(REVISIT_TRAVERSAL);
			((RevisitTraversalWriter) innerMessage).write(out);
		} else {
			throw new RuntimeException("Unknown message type seen: " + messageType);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		byte mtype = in.readByte();
		if (mtype == RESULTS) {
			innerMessage = ResultsReader.read(in);
			messageType = RESULTS_READER;
		} else if (mtype == TRAVERSAL) {
			innerMessage = TraversalReader.read(in);
			messageType = TRAVERSAL_READER;
		} else if (mtype == REVISIT_TRAVERSAL) {
			innerMessage = RevisitTraversalReader.read(in);
			messageType = REVISIT_TRAVERSAL_READER;
		} else {
			throw new RuntimeException("Unknown message type seen: " + mtype);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("BFSMessage,").append(messageType);
		switch (messageType) {
			case TRAVERSAL_READER:
				sb.append(",TRAVERSAL_READER,");
				break;
			case TRAVERSAL_WRITER:
				sb.append(",TRAVERSAL_WRITER,");
				break;
			case REVISIT_TRAVERSAL_READER:
				sb.append(",REVISIT_TRAVERSAL_READER,");
				break;
			case REVISIT_TRAVERSAL_WRITER:
				sb.append(",REVISIT_TRAVERSAL_WRITER,");
				break;
			case RESULTS_READER:
				sb.append(",RESULTS_READER,");
				break;
			case RESULTS_WRITER:
				sb.append(",RESULTS_WRITER,");
				break;
			default:
				sb.append("Unknown Message Type,");
		}
		sb.append(innerMessage);
		return sb.toString();
	}


	//////////////////////////////////////////////
	/**
	 * <(byte)messageType> written by BFSMessage.
	 * This class writes:
	 * <(int)count> [(long)rootSGID, (long)rootVID,
	 * (long)sourceVID, (long)edgeID, (long)sink/targetVID, (int)depth]+
	 * 
	 * @author simmhan
	 *
	 */
	public static class TraversalWriter {
		protected DataWriter messageWriter;
		private int count;

		public TraversalWriter() {
			messageWriter = DataWriter.newInstance();
			count = 0;
		}

		public void addTraversal(long rootSubgraph, long rootVertex, long sourceVID, long edgeID, long sinkVID, int depth)
		        throws IOException {
			messageWriter.writeLong(rootSubgraph);
			messageWriter.writeLong(rootVertex);
			messageWriter.writeLong(sourceVID);
			messageWriter.writeLong(edgeID);
			messageWriter.writeLong(sinkVID);
			messageWriter.writeInt(depth);
			count++;
		}


		public int size() {
			return count;
		}

		public void write(DataOutput out) throws IOException {
			// <(int)count> <(long)rootSGID, (long)rootVID, (long)sourceVID,
			// (long)edgeID, (long)sink/targetVID, (int)depth>+
			out.writeInt(count);
			out.write(messageWriter.getBytes());
		}
	}

	/**
	 * Reads and instantiates the list of traversal steps, including result for
	 * visited vertex, from input stream
	 */
	public static class TraversalReader {
		private List<TraversalStep.WithResults> steps;
		private int count;

		private TraversalReader() {
			steps = null;
			count = 0;
		}

		public List<TraversalStep.WithResults> getTraversals() {
			return steps;
		}

		public static TraversalReader read(DataInput in) throws IOException {
			TraversalReader w = new TraversalReader();
			w.readFields(in);
			return w;
		}

		public void readFields(DataInput in) throws IOException {
			// <(int)count> [(long)rootSGID, (long)rootVID, (long)sourceVID,
			// (long)edgeID, (long)sink/targetVID, (int)depth]>+
			count = in.readInt();
			steps = new ArrayList<>(count);
			for (int i = 0; i < count; i++) {
				long rootSubgraph = in.readLong();
				long rootVertex = in.readLong();
				long sourceVID = in.readLong();
				long edge = in.readLong();
				long targetVID = in.readLong();
				int depth = in.readInt();
				steps.add(new TraversalStep.WithResults(rootSubgraph, rootVertex, sourceVID, edge, targetVID, depth));
			}
		}
	}

	//////////////////////////////////////////////
	/**
	 * <(byte)messageType> written by BFSMessage.
	 * This class writes: <(int)count> <(long)rootSGID, (long)rootVID,
	 * (long)targetVID, (int)depth>+
	 * 
	 * This is similar to TraversalWriter, but skips writing the result fields:
	 * sourceVID and edgeID
	 * 
	 * @author simmhan
	 *
	 */
	public static class RevisitTraversalWriter {
		protected DataWriter messageWriter;
		private int count;

		public RevisitTraversalWriter() {
			messageWriter = DataWriter.newInstance();
			count = 0;
		}

		public void addTraversal(long rootSubgraph, long rootVertex, long sinkVID, int depth) throws IOException {
			messageWriter.writeLong(rootSubgraph);
			messageWriter.writeLong(rootVertex);
			messageWriter.writeLong(sinkVID);
			messageWriter.writeInt(depth);
			count++;
		}


		public int size() {
			return count;
		}

		public void write(DataOutput out) throws IOException {
			// <(int)count> <(long)rootSGID, (long)rootVID, (long)sink/targetVID,
			// (int)depth>+
			out.writeInt(count);
			out.write(messageWriter.getBytes());
		}
	}

	/**
	 * Reads and instantiates the list of traversal steps from input stream
	 */
	public static class RevisitTraversalReader {
		private List<TraversalStep> steps;
		private int count;

		private RevisitTraversalReader() {
			steps = null;
			count = 0;
		}

		public List<TraversalStep> getRevisitTraversals() {
			return steps;
		}

		public static RevisitTraversalReader read(DataInput in) throws IOException {
			RevisitTraversalReader w = new RevisitTraversalReader();
			w.readFields(in);
			return w;
		}

		public void readFields(DataInput in) throws IOException {
			// <(int)count> [(long)rootSGID, (long)rootVID, (long)sink/targetVID,
			// (int)depth]>+
			count = in.readInt();
			steps = new ArrayList<>(count);
			for (int i = 0; i < count; i++) {
				long rootSubgraph = in.readLong();
				long rootVertex = in.readLong();
				long targetVID = in.readLong();
				int depth = in.readInt();
				steps.add(new TraversalStep(rootSubgraph, rootVertex, targetVID, depth));
			}
		}
	}


	//////////////////////////////////////////////
	/**
	 * Buffers result messages for each remote subgraph.
	 * Maintains a Map from each remote vertex in that subgraph to the list of
	 * result triples to that vertex.
	 * 
	 * @author simmhan
	 *
	 */
	public static class ResultsWriter {

		// for each remote root vertex, maintain the visited edges
		private Map<Long, DataWriter> vertexResultsWriter;
		private int count;

		public ResultsWriter() {
			vertexResultsWriter = new HashMap<>();
			count = 0;
		}

		// <(long)sourceVID, (long)edgeID, (long)sinkVID>+
		public void addResult(long rootVertex, long sourceVertex, long edge, long sinkVertex) throws IOException {
			DataWriter resultWriter = vertexResultsWriter.get(rootVertex);
			if (resultWriter == null) {
				resultWriter = DataWriter.newInstance();
				vertexResultsWriter.put(rootVertex, resultWriter);
			}
			resultWriter.writeLong(sourceVertex);
			resultWriter.writeLong(edge);
			resultWriter.writeLong(sinkVertex);
			count++;
		}

		/**
		 * Total number of source-edge-sink triples being sent to this subgraph
		 */
		public int edgeCount() {
			return count;
		}

		/**
		 * Total number of distinct vertices
		 * 
		 * @return
		 */
		public int vertexCount() {
			return vertexResultsWriter.size();
		}

		/**
		 * Returns the map of vertex to its results
		 * 
		 * @return
		 */
		public Map<Long, DataWriter> getResults() {
			return vertexResultsWriter;
		}


		public void write(DataOutput out) throws IOException {
			// (int)vertexCount, [(long)vertex id, (int)triplecount, bytes]+
			// triplecount = bytes / 3*long

			// count of number of vertices
			out.writeInt(vertexResultsWriter.size());
			for (Entry<Long, DataWriter> entry : vertexResultsWriter.entrySet()) {
				out.writeLong(entry.getKey()); // write remote vertex ID
				byte[] triples = entry.getValue().getBytes();
				int tripleCount = triples.length /( 3 * 8);
//				System.out.println("Triple Count Write:"+entry.getKey()+ "," + tripleCount);
				out.writeInt(tripleCount); // write number of result triples
				out.write(triples); // write contents of all the triples
			}
		}
	}


	/**
	 * Reads the edge-triples results from the stream into a Map, with entries
	 * assocaited with vertices in this subgraph
	 * 
	 * @author simmhan
	 *
	 */
	public static class ResultsReader {
		private Map<Long, DataReader> vertexResultsReader;
		private int count;

		private ResultsReader() {
			vertexResultsReader = new HashMap<>();
			count = 0;
		}

		public Map<Long, DataReader> getResults() {
			return vertexResultsReader;
		}

		/**
		 * Total number of source-edge-sink triples being sent to this subgraph
		 */
		public int edgeCount() {
			return count;
		}

		/**
		 * Total number of distinct vertices
		 * 
		 * @return
		 */
		public int vertexCount() {
			return vertexResultsReader.size();
		}

		public static ResultsReader read(DataInput in) throws IOException {
			ResultsReader w = new ResultsReader();
			w.readFields(in);
			return w;
		}

		public void readFields(DataInput in) throws IOException {
			// (int)vertexCount, [(long)vertex id, (int)triplecount, bytes]+
			int vertexCount = in.readInt();

			for (int i = 0; i < vertexCount; i++) {
				long vertexId = in.readLong();
				int tripleCount = in.readInt();
				count += tripleCount;
//				System.out.println("Triple Count Read:"+vertexId+"," + tripleCount);
				byte[] tripleBytes = new byte[tripleCount * 3 * 8];
//				in.readFully(tripleBytes);
				 Arrays.fill(tripleBytes, (byte) 0);
		                     try {
		                           in.readFully(tripleBytes);
		                     } catch (EOFException eof) {
		                           eof.printStackTrace();
		                           int i1 = 0;
		                           for (byte b : tripleBytes)
		                                System.out.printf("%d %x %n", i1++, b);
		                     }
				vertexResultsReader.put(vertexId, DataReader.newInstance(tripleBytes));
			}
		}
	}
}
