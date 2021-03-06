package in.dream_lab.goffish.godb.path;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.godb.Path;
import in.dream_lab.goffish.godb.PathWithDir;
import in.dream_lab.goffish.godb.PathWithDir.EVPair;
import in.dream_lab.goffish.godb.path.TraversalStep.TraversalWithState;
import in.dream_lab.goffish.godb.reach.TraversalStep.TraversalWithPath;
import in.dream_lab.goffish.godb.util.DataReader;
import in.dream_lab.goffish.godb.util.DataWriter;


/**
 * Encapsulates reads and writes of result and traversal messages
 * 
 * @author simmhan
 *
 */
public class PathMessage implements Writable {

        public static final Log LOG = LogFactory.getLog(PathMessage.class);
	// Use LSB+1 and LSB+2 bits for type of message
	private static final byte TRAVERSAL = 0b0010;
	private static final byte RESULTS = 0b0000;
	private static final byte REVISIT_TRAVERSAL = 0b0100;
	private static final byte INEDGES = 0b1000;
	private static final byte STOP=0b1100;
	
	private static final byte READER = 0b0000;
	private static final byte WRITER = 0b0001;

	// Use LSB 0 for reader, LSB 1 for writer
	public static final byte TRAVERSAL_READER = TRAVERSAL | READER;
	public static final byte TRAVERSAL_WRITER = TRAVERSAL | WRITER;
	
	public static final byte REVISIT_TRAVERSAL_READER = REVISIT_TRAVERSAL | READER;
	public static final byte REVISIT_TRAVERSAL_WRITER = REVISIT_TRAVERSAL | WRITER;

	public static final byte RESULTS_READER = RESULTS | READER;
	public static final byte RESULTS_WRITER = RESULTS | WRITER;

        public static final byte INEDGES_READER = INEDGES | READER;
        public static final byte INEDGES_WRITER = INEDGES | WRITER;
        
        public static final byte STOP_READER = STOP | READER;
        public static final byte STOP_WRITER = STOP | WRITER;
        
	private byte messageType;

	// A results reader/writer, or traversal reader/write object
	private Object innerMessage;

	public PathMessage() {
		// FIXME: Is default constructor required for Writable?
		messageType = Byte.MIN_VALUE;
	}

	public PathMessage(ResultsWriter results) {
		innerMessage = results;
		messageType = RESULTS_WRITER;
	}

	public PathMessage(TraversalWriter traversal) {
		innerMessage = traversal;
		messageType = TRAVERSAL_WRITER;
	}

	public PathMessage(RevisitTraversalWriter rtraversal) {
		innerMessage = rtraversal;
		messageType = REVISIT_TRAVERSAL_WRITER;
	}
	  
	public PathMessage(InEdgesWriter inedges) {
          innerMessage = inedges;
          messageType = INEDGES_WRITER;
        }

	public PathMessage(StopWriter stopmsg){
	  innerMessage = stopmsg;
	  messageType = STOP_WRITER;
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
	
	public InEdgesWriter getInEdgesWriter() {
          if (messageType == INEDGES_WRITER) return (InEdgesWriter) innerMessage;
          throw new RuntimeException("This BFSMessage is not a INEDGES_WRITER message. Message type=" + messageType);
        }
	
	public StopWriter getStopMessageWriter() {
          if (messageType == STOP_WRITER) return (StopWriter) innerMessage;
          throw new RuntimeException("This BFSMessage is not a STOP_WRITER message. Message type=" + messageType);
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
			
	public InEdgesReader getInEdgesReader() {
          if (messageType == INEDGES_READER) return (InEdgesReader) innerMessage;
          throw new RuntimeException("This BFSMessage is not a INEDGES_READER message. Message type=" + messageType);
        }
	
	public StopReader getStopReader() {
          if (messageType == STOP_READER) return (StopReader) innerMessage;
          throw new RuntimeException("This BFSMessage is not a STOP_READER message. Message type=" + messageType);
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
		}else if(messageType == INEDGES_WRITER){
		        out.writeByte(INEDGES);
		        ((InEdgesWriter) innerMessage).write(out);
	        }else if(messageType == STOP_WRITER){
                        out.writeByte(STOP);
                        ((StopWriter) innerMessage).write(out);
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
		}else if (mtype == INEDGES) {
                        innerMessage = InEdgesReader.read(in);
                        messageType = INEDGES_READER;
                }else if (mtype == STOP) {
                  innerMessage = StopReader.read(in);
                  messageType = STOP_READER;
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
//		        LOG.info("TraversalReader");
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
		//(stuff.queryId,stuff.rootSubgraph,stuff.rootVertex, stuff.previousSubgraph,stuff.startVertex,stuff.targetVertex,stuff.depth,false);
		public void addTraversal(long queryId,long rootSubgraph, long rootVertex,long previousSubgraph,long previousVertex, long sinkVID, int depth,boolean dir) throws IOException {
			messageWriter.writeLong(queryId);
		        messageWriter.writeLong(rootSubgraph);
			messageWriter.writeLong(rootVertex);
			messageWriter.writeLong(previousSubgraph);
			messageWriter.writeLong(previousVertex);
			messageWriter.writeLong(sinkVID);
			messageWriter.writeInt(depth);
			messageWriter.writeBoolean(dir);
			
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
		private List<TraversalMsg> steps;
		private int count;

		private RevisitTraversalReader() {
			steps = null;
			count = 0;
		}

		public List<TraversalMsg> getRevisitTraversals() {
			return steps;
		}

		public static RevisitTraversalReader read(DataInput in) throws IOException {
			RevisitTraversalReader w = new RevisitTraversalReader();
			w.readFields(in);
			return w;
		}

		public void readFields(DataInput in) throws IOException {
			// <(int)count> [(long)rootSGID, (long)rootVID, (long)sink/targetVID,
			// (int)depth,pathSize,vertex,edge,...,vertex]>+
//		        LOG.info("RevisitTraversalReader");
			count = in.readInt();
			steps = new ArrayList<>(count);
			for (int i = 0; i < count; i++) {
			        long queryId=in.readLong();
				long rootSubgraph = in.readLong();
				long rootVertex = in.readLong();
				long previousSubgraph=in.readLong();
				long previousVertex = in.readLong();
				long targetVID = in.readLong();
				int depth = in.readInt();
				boolean dir=in.readBoolean();
				
				
				steps.add(new TraversalMsg(queryId,rootSubgraph, rootVertex,previousSubgraph,previousVertex,targetVID,depth,dir));
			}
		}
		
		public static class TraversalMsg{
		  
      long queryId;
      long rooSubgraph;
      long rootVertex;
      long previousSubgraph;
      long previousVertex;
      long targetVID;
      int depth;
      boolean dir;

      public TraversalMsg(long queryId,long rootSubgraph, long rootVertex,long previousSubgraph,long previousVertex, long sinkVID, int depth,boolean dir){
		    this.queryId=queryId;
		    this.rooSubgraph=rootSubgraph;
		    this.rootVertex=rootVertex;
		    this.previousSubgraph=previousSubgraph;
		    this.previousVertex=previousVertex;
		    this.targetVID=sinkVID;
		    this.depth=depth;
		    this.dir=dir;
		  }
		  
		}
		
	}

	
	

	//////////////////////////////////////////////
	/**
	 
	 * 
	 * @author simmhan
	 *
	 */
	public static class ResultsWriter {

		// for each remote SGID , main List of Paths
		private Map<Long, DataWriter> vertexResultsWriter;
		private int count;

		public ResultsWriter() {
			vertexResultsWriter = new HashMap<>();
			count = 0;
		}

		// <(long)sourceVID, (long)edgeID, (long)sinkVID>+
		public void addResult(long queryId,int startDepth,long previousVertex,boolean direction,PathWithDir p) throws IOException {
			DataWriter resultWriter = vertexResultsWriter.get(queryId);
			if (resultWriter == null) {
				resultWriter = DataWriter.newInstance();
				vertexResultsWriter.put(queryId, resultWriter);
			}
			resultWriter.writeInt(startDepth);
			resultWriter.writeLong(previousVertex);
			resultWriter.writeBoolean(direction);
			resultWriter.writeInt(p.path.size());
			resultWriter.writeLong(p.startVertex);
			for(EVPair item: p.path){
			  resultWriter.writeLong(item.edgeId);
			  resultWriter.writeLong(item.vertexId);
			  resultWriter.writeBoolean(item.direction);;
			}
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
				byte[] path = entry.getValue().getBytes();
//				int tripleCount = triples.length /( 3 * 8);
//				System.out.println("Triple Count Write:"+entry.getKey()+ "," + tripleCount);
//				out.writeInt(tripleCount); // write number of result triples
				out.write(path); // write contents of all the triples
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
		private Map<Long, ArrayList<OutputReader>> vertexResultsReader;
		private int count;

		private ResultsReader() {
			vertexResultsReader = new HashMap<>();
			count = 0;
		}

		public Map<Long, ArrayList<OutputReader>> getResults() {
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
		  try{
			int vertexCount = in.readInt();

			for (int i = 0; i < vertexCount; i++) {
				long qId = in.readLong();
				int startDepth=in.readInt();
				long previousVertex=in.readLong();
				boolean dir=in.readBoolean();
//				LOG.info("ResultReader");
				int pathSize=in.readInt();
				long startVertex=in.readLong();
				byte[] pathBytes = new byte[17*pathSize];//FIX this..DOne 8*2+1
//				in.readFully(tripleBytes);
				//FIX:CHANGE this for PATHS
				 Arrays.fill(pathBytes, (byte) 0);
		                     try {
		                           in.readFully(pathBytes);
		                     } catch (EOFException eof) {
		                           eof.printStackTrace();
		                           int i1 = 0;
		                           for (byte b : pathBytes)
		                                System.out.printf("%d %x %n", i1++, b);
		                     }
		                ArrayList<OutputReader> pathList=vertexResultsReader.get(qId);
		                if(pathList==null){
		                  pathList=new ArrayList<>();
		                  vertexResultsReader.put(qId,pathList);
		                }
				vertexResultsReader.get(qId).add(new OutputReader(startDepth,previousVertex,dir,startVertex, DataReader.newInstance(pathBytes)));
			}
		  }
		  catch(Exception e){
		    e.printStackTrace();
		  }
		}
		
		
		
		public static class OutputReader{
		  int startDepth;
		  long previousVertex;
		  boolean dir;
		  PathWithDir path;
		  
		  public OutputReader(int _sd,long _pV,boolean dir,long _sV,DataReader reader){
		    this.startDepth=_sd;
		    this.previousVertex=_pV;
		    this.dir=dir;
		    PathWithDir p=new PathWithDir(_sV);
		    boolean eof=false;
		    while (!eof) {
	              try {
//	                 int pathSize=reader.readInt();
//	                 pathSize=reader.readInt();
//	                 System.out.println("Path Size:" + pathSize);
	                   

	                     p.addEV(reader.readLong(), reader.readLong(),reader.readBoolean());
	                   
	                  // read and use data
	              } catch (IOException e) {
	                  eof = true;
	              }
	          }
		    
		    path=p;
		  }
		}
	}

        //////////////////////////////////////////////
        /**
         * Writing InEdges
         *
         *
         */
        public static class InEdgesWriter {

                
                private byte[] InEdgesMessage;
                

                public InEdgesWriter(byte[] msg) {
                        InEdgesMessage = msg;
                        
                }

               



                public void write(DataOutput out) throws IOException {
                        

                        
                        out.writeInt(InEdgesMessage.length);
                        out.write(InEdgesMessage);
                }
        }


        /**
         * Reading InEdges 
         *
         */
        public static class InEdgesReader {
                
                private byte[] InEdgesMessage;

                private InEdgesReader() {
                  
                }

                public byte[] getInEdgesMessage() {
                  return InEdgesMessage;
          }
               

                public static InEdgesReader read(DataInput in) throws IOException {
                        InEdgesReader w = new InEdgesReader();
                        w.readFields(in);
                        return w;
                }

                public void readFields(DataInput in) throws IOException {
                        // (int)vertexCount, [(long)vertex id, (int)triplecount, bytes]+
                        int Length = in.readInt();
                        InEdgesMessage=new byte[Length];
                        in.readFully(InEdgesMessage);
                }
        }

        //////////////////////////////////////////////
        /**
         * Writing Stop Message
         *
         *
         */
        public static class StopWriter {

                
                private int newDepth;
                

                public StopWriter(int _newDepth) {
                        this.newDepth=_newDepth;
                        
                }

                public void write(DataOutput out) throws IOException {
                        
                        out.writeInt(newDepth);
                }
        }


        /**
         * Reading InEdges 
         *
         */
        public static class StopReader {
                
                private int newDepth;

                private StopReader() {
                  
                }

                public int getNewDepth() {
                  return newDepth;
                }
               

                public static StopReader read(DataInput in) throws IOException {
                        StopReader w = new StopReader();
                        w.readFields(in);
                        return w;
                }

                public void readFields(DataInput in){
                        // (int)vertexCount, [(long)vertex id, (int)triplecount, bytes]+
//                        LOG.info("StopReader");
                        try {
                          newDepth = in.readInt();
                        } catch (IOException e) {
                          // TODO Auto-generated catch block
                          e.printStackTrace();
                        }
                }
        }        
        

}



