

package in.dream_lab.goffish.godb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
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
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.ISubgraphWrapup;


public class SerializeFullHeu extends AbstractSubgraphComputation<pathDistrSubgraphState, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable>
implements ISubgraphWrapup{
	// Variables and DataStructures
	
	private PrintWriter writer;

	public SerializeFullHeu(String initMsg){
	  argument=initMsg;
	}
	String argument=null;
	private Hueristics hueristics = new Hueristics(); 
	
	
	private Double[] queryCostHolder;
	
	static File vertexIndexDir;
	static Directory vertexDirectory;
	static Analyzer analyzer;
	static IndexReader indexReader;
	static IndexSearcher indexSearcher;
	static BooleanQuery query;
	static ScoreDoc[] hits;
	static boolean WriteDone = false ;
	static boolean queryMade = false;
	private static final Object WriteLock = new Object();
	


	enum Type{EDGE,VERTEX}
	enum Direction{OUT,IN}
	
	
	
	/**
	 * Class for storing the traversal path V->E->V->E->E.....
	 */
	private class Step{
		Type type = null;
		Direction direction = null;
		String property;
		Object value;
		Step(Type t,Direction d,String p,Object v){
			this.type = t;
			this.direction = d;
			this.property = p;
			this.value = v;
		}
	}


	
	class ResultSet{
		ArrayList<String> forwardResultSet;
		ArrayList<String> revResultSet;
		public ResultSet() {
			forwardResultSet = new ArrayList<String>();
			revResultSet = new ArrayList<String>();
		}
	}

	private ArrayList<Step> path = null;
	

//	private HashMap<Long,HashMap<String,LinkedList<Long>>> inVerticesMap;
//	private HashMap<Long,Long> remoteSubgraphMap; 

	private Integer noOfSteps = null;
	
	//TODO: change to relative path
	FileOutputStream fos;
	ObjectOutputStream oos;
	String heuristicsBasePath = ConfigFile.basePath+"heuristics/hue_";
	
	private final Base64 base64 = new Base64();
	
	private int startPos  = 0;
	
	private Double networkCoeff = new Double(0.116);

	// For InEdges
	public class EdgeAttr
	{
		String Attr_name;
		String Value;
		long EdgeId;
		boolean isRemote;
		Long subgraphId;
		Integer partitionId;
		EdgeAttr(String _Attr,String _Value,long _EdgeId,boolean _isRemote,Long _subgraphId,Integer _partitionId)
		{
			this.Attr_name=_Attr;
			this.Value=_Value;
			this.EdgeId=_EdgeId;
			this.isRemote=_isRemote;
			this.subgraphId=_subgraphId;
			this.partitionId=_partitionId;
		}
	}

	//Data Structure for storing inedges 
	HashMap<Long,HashMap<Long,EdgeAttr>>  InEdges = new HashMap<Long,HashMap<Long,EdgeAttr>>();
	
	// Recursive Output COllection data structures	
	// TODO: Populate the hash appropriately
	long time;
	class Pair{
        Long prevSubgraphId;
        Integer prevPartitionId;    
        public Pair(Long _prevSubgraphId, Integer _prevPartitionId) {
            this.prevSubgraphId = _prevSubgraphId;
            this.prevPartitionId = _prevPartitionId;
        }
    }
		
		class RecursivePathMaintained{
			Long startVertex;
			String path;
			int direction=0;
			public RecursivePathMaintained(Long _startVertex, String _path, int _direction){
				this.startVertex=_startVertex;
				this.path=_path;
				this.direction = _direction;
			}
		}
		//Hashmap for recursive path maintainance
		//Hashmap for recursive path maintainance
		HashMap<Long, HashMap<Long,Pair>> outputPathMaintainance = new HashMap<Long, HashMap<Long,Pair>>();
		
		//Hashmap for mapping end vertex to starting vertex and paths
		HashMap<Long, List<RecursivePathMaintained>> recursivePaths = new HashMap<Long, List<RecursivePathMaintained>>();
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/**
	 * Initialize the class variables
	 * 
	 */
	private void init(Iterable<IMessage<LongWritable, Text>> messageList){
	
		System.out.println("IN INIT");

		time=System.currentTimeMillis();
		// read heuristics from memory
		try{
			FileInputStream fis = new FileInputStream(heuristicsBasePath+String.valueOf(getSubgraph().getSubgraphId())+".ser"); 
			ObjectInputStream ois = new ObjectInputStream(fis);
			hueristics = (Hueristics)ois.readObject();
			ois.close();
		}catch(Exception e){e.printStackTrace();}	
		
		System.out.println("Exchanging heuristics");
		// exchange hueristics
		exchangeHueristics();
	}

	
	
	
	
	
	
	
	
	
	
	
	
	/**
	 * Different subgraphs exchange their local heuristics and get a global view of the graphs
	 */
	private void exchangeHueristics() {
		try {
			String data = serializeObjectToString(hueristics);
			Text msg = new Text(data.getBytes());
			
			sendToAll( msg); 
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private void gatherHueristics(Iterable<IMessage<LongWritable, Text>> messageList) {
		hueristics = new Hueristics();//Re initialize as it has to accummulate statistics from all partitions including its own.
		for(IMessage<LongWritable, Text> message:messageList) {
			try {
				Hueristics _hueristics = (Hueristics)deserializeObjectFromString(new String(message.getMessage().toString()));
				hueristics.numVertices += _hueristics.numVertices;
				hueristics.numEdges += _hueristics.numEdges;
				// gather vertex stats
				for(Map.Entry<String,HashMap<String,vertexPredicateStats>> entry:_hueristics.vertexPredicateMap.entrySet()){
					if ( !hueristics.vertexPredicateMap.containsKey(entry.getKey()) ) {
						hueristics.vertexPredicateMap.put(entry.getKey(), entry.getValue());
					}
					else {
						for(Map.Entry<String, vertexPredicateStats> entry_inside: entry.getValue().entrySet()){
							if( !hueristics.vertexPredicateMap.get(entry.getKey()).containsKey(entry_inside.getKey()) )
								hueristics.vertexPredicateMap.get(entry.getKey()).put(entry_inside.getKey(), entry_inside.getValue());
							else {
								Double number = hueristics.vertexPredicateMap.get(entry.getKey()).get(entry_inside.getKey()).numberMatchingPredicate;
								Double out = hueristics.vertexPredicateMap.get(entry.getKey()).get(entry_inside.getKey()).numOutDegree;
								Double in = hueristics.vertexPredicateMap.get(entry.getKey()).get(entry_inside.getKey()).numInDegree;
								Double rOut = hueristics.vertexPredicateMap.get(entry.getKey()).get(entry_inside.getKey()).numRemoteOutDegree;
                                                                Double rIn = hueristics.vertexPredicateMap.get(entry.getKey()).get(entry_inside.getKey()).numRemoteInDegree;
                                                                
								vertexPredicateStats vertexStats = new vertexPredicateStats();
								vertexStats.numberMatchingPredicate = number + entry_inside.getValue().numberMatchingPredicate;
								vertexStats.numOutDegree = out + entry_inside.getValue().numOutDegree;
								vertexStats.numInDegree = in + entry_inside.getValue().numInDegree;
                                                                vertexStats.numRemoteOutDegree = rOut + entry_inside.getValue().numRemoteOutDegree;
                                                                vertexStats.numRemoteInDegree = rIn + entry_inside.getValue().numRemoteInDegree;
								hueristics.vertexPredicateMap.get(entry.getKey()).put(entry_inside.getKey(),vertexStats);
							}
								
						}
					}
				}
				// gather edge stats
				for(Map.Entry<String,HashMap<String,edgePredicateStats>> entry:_hueristics.edgePredicateMap.entrySet()){
					if ( !hueristics.edgePredicateMap.containsKey(entry.getKey()) ) {
						hueristics.edgePredicateMap.put(entry.getKey(), entry.getValue());
					}
					else {
						for(Map.Entry<String, edgePredicateStats> entry_inside: entry.getValue().entrySet()) {
							if( !hueristics.edgePredicateMap.get(entry.getKey()).containsKey(entry_inside.getKey()) )
								hueristics.edgePredicateMap.get(entry.getKey()).put(entry_inside.getKey(), entry_inside.getValue());
							else {
								Double number = hueristics.edgePredicateMap.get(entry.getKey()).get(entry_inside.getKey()).numberMatchingPredicate;
								edgePredicateStats edgeStats = new edgePredicateStats();
								edgeStats.numberMatchingPredicate = number + entry_inside.getValue().numberMatchingPredicate;
								hueristics.edgePredicateMap.get(entry.getKey()).put(entry_inside.getKey(),edgeStats);
							}
								
						}
					}
				}
				
				
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		for(Map.Entry<String,HashMap<String,vertexPredicateStats>> entry:hueristics.vertexPredicateMap.entrySet()){
			for(Map.Entry<String, vertexPredicateStats> entry_inside: entry.getValue().entrySet()){
				entry_inside.getValue().probability = entry_inside.getValue().numberMatchingPredicate / hueristics.numVertices;
				entry_inside.getValue().avgOutDegree = entry_inside.getValue().numOutDegree / entry_inside.getValue().numberMatchingPredicate;
				entry_inside.getValue().avgInDegree = entry_inside.getValue().numInDegree / entry_inside.getValue().numberMatchingPredicate;
				entry_inside.getValue().avgRemoteOutDegree = entry_inside.getValue().numRemoteOutDegree / entry_inside.getValue().numberMatchingPredicate;
                                entry_inside.getValue().avgRemoteInDegree = entry_inside.getValue().numRemoteInDegree / entry_inside.getValue().numberMatchingPredicate;
			}
		}
		for(Map.Entry<String,HashMap<String,edgePredicateStats>> entry:hueristics.edgePredicateMap.entrySet()){
			for(Map.Entry<String, edgePredicateStats> entry_inside: entry.getValue().entrySet()){
				entry_inside.getValue().probability = entry_inside.getValue().numberMatchingPredicate / hueristics.numEdges;
			}
		}
		
		//HACK: to reduce time required in first two supersteps.... storing the heuristics of the whole graph in each partition instead of for each subgraph
		//finding the smallest subgraphid
		
		
		
		
			System.out.println("Writing FULL Heuristics to Disk");
			try
			{
				File file = new File(heuristicsBasePath+String.valueOf("FULL")+".ser");
				if ( !file.exists() ) {
			
					file.getParentFile().mkdirs();
					file.createNewFile();
			
				}
				fos = new FileOutputStream(file); 
				oos = new ObjectOutputStream(fos);
				oos.writeObject(hueristics);
				oos.close();
				
			
		}catch(Exception e){
			e.printStackTrace();
				}
		
		
		
	}	
	
	
	
	

	
	
	
	
	
	
	
	
	


	
	
	
	Long minSubgraphId;
	
	
	
	
	@Override
	public void compute(Iterable<IMessage<LongWritable, Text>> messageList) {
		
		
//		System.out.println("**********SUPERSTEPS***********:" + getSuperStep() +"Message List Size:" + _messageList.size() + " Running Serializing Heu");
		
		
		// STATIC ONE TIME PROCESSES
		{
		  
		        if(getSuperstep()==0){
		          Text sgid=new Text(getSubgraph().getSubgraphId().toString());
		          sendToAll(sgid);
		        }
			// LOAD QUERY AND INITIALIZE LUCENE
		        else if(getSuperstep() == 1){
				
		          minSubgraphId=Long.MAX_VALUE;
	                 for(IMessage<LongWritable,Text> m:messageList){
	                   long sgid=Long.parseLong(m.getMessage().toString());
	                   if(sgid<minSubgraphId){
	                     minSubgraphId=sgid;
	                   }
	                 }
			        
			        
//				System.out.println("Minimum SGID:" + minSubgraphId);
					init(messageList);
					// TODO: uncomment after indexing
					
				
			}
			
			
			else if (getSuperstep()==2) {
				// GATHER HEURISTICS FROM OTHER SUBGRAPHS
				System.out.println("*****************Gathering heuristics from other Subgraphs to get Global Heuristics********************");
				if(getSubgraph().getSubgraphId().get()==minSubgraphId)
					gatherHueristics(messageList);
				System.out.println("*******************************************************************************************************");
				System.out.println("TIME GATHERING HEURISTICS:" + (System.currentTimeMillis()-time));
				
				// TODO: remove this hack
				// TRANSMIT TO REMOTE VERTICES SO THEY CAN KEEP TAB OF REMOTE IN EDGES
//				HashMap<Long,HashMap<String,StringBuilder>> remoteMap = new HashMap<Long,HashMap<String,StringBuilder>>();
//				
//				ISubgraphInstance instance = null;
//				try {
//					instance = subgraph.getInstances(0, Long.MAX_VALUE, new PropertySet(new ArrayList<Property>()), subgraph.getEdgeProperties(), false).iterator().next();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
				
//				for(ITemplateVertex vertex: subgraph.vertices()){
//					
//					for(ITemplateEdge edge:vertex.outEdges()) {
//						if ( edge.getSink().isRemote() ) {
//							String relation = instance.getPropertiesForEdge(edge.getId()).getValue("relation").toString();
//							System.out.println("RELATION:" + relation);
//							ITemplateVertex remoteVertex = edge.getSink();
//							if ( !remoteMap.containsKey(remoteVertex.getId()) ) {
//								remoteMap.put(new Long(remoteVertex.getId()), new HashMap<String, StringBuilder>());
//								remoteMap.get(remoteVertex.getId()).put(relation, new StringBuilder(""));
//								remoteMap.get(remoteVertex.getId()).get(relation).append(vertex.getId());
//							}
//							else {
//								if (remoteMap.get(remoteVertex.getId()).containsKey(relation))
//									remoteMap.get(remoteVertex.getId()).get(relation).append(","+vertex.getId());
//								else {
//									remoteMap.get(remoteVertex.getId()).put(relation, new StringBuilder(""));
//									remoteMap.get(remoteVertex.getId()).get(relation).append(vertex.getId());
//								}
//							}	
//						}
//					}
//				} 
//				
//				for(Map.Entry<Long, HashMap<String,StringBuilder>> entry: remoteMap.entrySet()) {
//					StringBuilder _message = new StringBuilder(subgraph.getId() + "/" +entry.getKey().toString() + ":");
//					int count=0;
//					for(Map.Entry<String, StringBuilder> entry_inside: entry.getValue().entrySet() ) {
//						
//						_message.append(entry_inside.getKey() + "?" + entry_inside.getValue());
//						if(count!=(entry.getValue().entrySet().size()-1))
//								{
//								 count++;
//								 _message.append("|");
//								}
//					}
//					
//					SubGraphMessage remoteM = new SubGraphMessage(_message.toString().getBytes());
//					remoteM.setTargetSubgraph(subgraph.getVertex(entry.getKey()).getRemoteSubgraphId());
//					sendMessage(remoteM);
//					System.out.println("SENDING MESSAGE:" + new String(remoteM.getData()));
//				}
				
			}
		}	
			voteToHalt();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	// UTILITY FUNCTIONS
	
	private void output(int partId, long subId, String path) {

		try {
			//writer = new PrintWriter(new FileWriter("PWI_" + partId + "_" + subId + ".txt", true));
			writer = new PrintWriter(new FileWriter("PWI_" + "00" + "_" + "00" + ".txt", true));

		} catch (IOException e) {
			e.printStackTrace();
		}
		writer.print(path + "\n");
		writer.flush();
		writer.close();
	}

	private String serializeObjectToString(Object object) throws IOException {
        try (
                ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
                GZIPOutputStream gzipOutputStream = new GZIPOutputStream(arrayOutputStream);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(gzipOutputStream);) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
            objectOutputStream.close();
            gzipOutputStream.close();
            return new String(base64.encode(arrayOutputStream.toByteArray()));
        }
    }

    private Object deserializeObjectFromString(String objectString) throws IOException, ClassNotFoundException {
        try (
                ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(base64.decode(objectString));
                GZIPInputStream gzipInputStream = new GZIPInputStream(arrayInputStream);
                ObjectInputStream objectInputStream = new ObjectInputStream(gzipInputStream)) {
        	return objectInputStream.readObject();
        }
        
    }













    @Override
    public void wrapup() {
      // TODO Auto-generated method stub
      
    }
	
}
