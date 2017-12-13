package in.dream_lab.goffish.hama.succinctstructure;

import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.pathDistrSuccinctStructure;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.commons.math.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static in.dream_lab.goffish.hama.succinctstructure.SuccinctArraySubgraph.Log;

/**
 * Created by sandy on 9/9/17.
 */
public class SuccinctArrayVertex12<V extends Writable, E extends Writable, I extends Writable, J extends Writable> implements IVertex<V, E, I, J> {
//	public static final Log LOG = LogFactory.getLog(SuccinctVertex.class);
	public static final Log LOG = LogFactory.getLog(SuccinctArrayVertex12.class);
	private I vid;
    private List<SuccinctIndexedFileBuffer>  ebufferList;
    private SuccinctIndexedFileBuffer vertexSuccinctBuffer;
//    private char delim;
    private HashMap<String, SuccinctIndexedFileBuffer> propertySuccinctBufferMap;
    private static Splitter splitter;
    public SuccinctArrayVertex12(I vid, SuccinctIndexedFileBuffer _vertexSuccinctBuffer,HashMap<String, SuccinctIndexedFileBuffer> propertySuccinctBufferMap, List<SuccinctIndexedFileBuffer> ebufferList)
    {
        this.vid = vid;
        this.setVertexSuccinctBuffer(_vertexSuccinctBuffer);
        this.setPropertySuccinctBufferMap(propertySuccinctBufferMap);
        this.ebufferList = ebufferList;
//        this.delim = delim;
        splitter = Splitter.createSplitter();
    }
    public I getVertexId()
    {
        return vid;
    }
    /**
     * returns both LocalSinkList and RemoteSinkList
     * @return
     */
    public Tuple<List<Long>, List<Long>> getEdges()
    {
        Log.info("getEdges");
    	SuccinctIndexedFileBuffer ebuffer = null;
    	Long searchQuery=((LongWritable)vid).get();
//    	LOG.info("GETEDGES search:" + searchQuery.toString().concat("@") );
//    	LOG.info("EBUFFER size:" + ebufferList.size());
    	String wholeQuery="#"+searchQuery.toString().concat("@");
    	long countStart=System.nanoTime();
    	int iteration=0;
    	for(SuccinctIndexedFileBuffer ebuf:ebufferList) {
    		iteration++;
    		long count = ebuf.count(wholeQuery.getBytes());
    		if(count > 0) {
//    			LOG.info("FOUND COUNT to be:" +count);
    			ebuffer=ebuf;
    			break;
    		}
    		
    	}
    	
    	LOG.info("Count(Edge): " + (System.nanoTime()-countStart) + " " + iteration);
    	
       String[] tokens;
        
        String record;
        List<Long> localSinks = new ArrayList<>();
        List<Long> remoteSinks = new ArrayList<>();
        if(ebuffer==null) {
//        	LOG.info("Returning null edge buffer");
    	 	return new Tuple<>(localSinks, remoteSinks); 
    	}
        long start = System.nanoTime();
        Integer[] recordID = ebuffer.recordSearchIds(wholeQuery.getBytes());
        LOG.info("Lookup record id(edge): " + (System.nanoTime() - start) + " ns " + recordID.length);
        for (Integer rid : recordID)
        {

            start = System.nanoTime();
            record = ebuffer.getRecord(rid);
            LOG.info("Extract until(edge): " + (System.nanoTime() - start) + " ns" );
            LOG.info("# Extracted Bytes: " + record.length());
//            tokens = splitter.splitLong(record);
            tokens = record.split("\\W");
            int lCount=Integer.parseInt(tokens[2]);
            for(int i=3; i < 3 + lCount ; i++)
                localSinks.add(Long.parseLong(tokens[i]));
            for(int i= 3 + lCount; i < tokens.length; i++)
                remoteSinks.add(Long.parseLong(tokens[i]));
        }
        return new Tuple<>(localSinks, remoteSinks);
    }
    
    /**
     * not being used now obsolete
     */
    public Iterable<IEdge<E, I, J>> getOutEdges()
    {
        int offset;
        String[] tokens;
        String record;
        List<IEdge<E, I, J>> localsinks = new ArrayList<>();
        Long searchQuery=((LongWritable)vid).get();
        //FIXME: uncomment and implement this
//        Integer[] recordID = ebuffer.recordSearchIds(searchQuery.toString().concat("@").getBytes());
////        LOG.info("RecordIDLength:"+ recordID.length);
//        for (Integer rid : recordID)
//        {
//            offset = ebuffer.getRecordOffset(rid);
//            record = ebuffer.extractUntil(offset, delim);
//            tokens=record.split("\\W");
////            LOG.info("tokenLength:"+ tokens.length);
//            // TODO: Implement Better Solution for below FOR loop @Swapnil
//            for(int i=3; i < 3 + Integer.parseInt(tokens[2]); i++) {
//                localsinks.add(new SuccinctEdge<E, I, J>((I)new LongWritable(Long.parseLong(tokens[i]))));
//            }
//        }
        return localsinks;
    }
    
    /**
     * obsolete
     * @return
     */
    public Iterable<IEdge<E, I, J>> getRemoteOutEdges()
    {
        int offset;
        String[] tokens;
        String record;
        List<IEdge<E, I, J>> remotesinks = new ArrayList<>();
        Long searchQuery=((LongWritable)vid).get();
        //FIXME: ucomment and implement this for arrays
//        Integer[] recordID = ebuffer.recordSearchIds(searchQuery.toString().concat("@").getBytes());
//        for (Integer rid : recordID)
//        {
//            offset = ebuffer.getRecordOffset(rid);
//            record = ebuffer.extractUntil(offset, delim);
//            tokens = record.split("\\W");
//            // TODO: Implement Better Solution for below FOR loop @Swapnil
//            for(int i = 3 + Integer.parseInt(tokens[2]); i < tokens.length; i++) {
//                remotesinks.add(new SuccinctEdge<E, I, J>((I)new LongWritable(Long.parseLong(tokens[i]))));
//            }
//        }
        return remotesinks;
    }
    public boolean isRemote()
    {
        throw new UnsupportedOperationException("Remote vertex information is not stored");
    }
    public V getValue()
    {
        throw new UnsupportedOperationException("Vertex values are not stored");
    }
    public IEdge<E, I, J> getOutEdge(I vertexId)
    {
        throw new UnsupportedOperationException("We do not return a single out edge");
    }
    public void setValue(V value)
    {
        throw new UnsupportedOperationException("We do not set the vertex value");
    }
    public String getPropforVertex(String propName)
    {
        Log.info("getPropforVertex");
    	Long searchQuery=((LongWritable)vid).get();
    	String wholeQuery="#"+searchQuery.toString().concat("@");
    	SuccinctIndexedFileBuffer vbuffer=null;
    	long countStart=System.nanoTime();
    	int iteration=0;
		
    	SuccinctIndexedFileBuffer propBuffer = getPropertySuccinctBufferMap().get(propName);
		LOG.info("Count(Vertex): " + (System.nanoTime()-countStart) + " " + iteration);
		
  
        long start = System.nanoTime();
        Integer[] recordID=getVertexSuccinctBuffer().recordSearchIds(wholeQuery.getBytes());
        LOG.info("Lookup record id(property): " + (System.nanoTime() - start) + " ns");
        start = System.nanoTime();
        String sRecord = propBuffer.getRecord(recordID[0]);
        LOG.info("Extract until(property): " + (System.nanoTime() - start) + " ns");
        LOG.info("# Extracted Bytes: " + sRecord.length());

        return sRecord.substring(1, sRecord.length()-1);
    }
	public SuccinctIndexedFileBuffer getVertexSuccinctBuffer() {
		return vertexSuccinctBuffer;
	}
	public void setVertexSuccinctBuffer(SuccinctIndexedFileBuffer vertexSuccinctBuffer) {
		this.vertexSuccinctBuffer = vertexSuccinctBuffer;
	}
	public HashMap<String, SuccinctIndexedFileBuffer> getPropertySuccinctBufferMap() {
		return propertySuccinctBufferMap;
	}
	public void setPropertySuccinctBufferMap(HashMap<String, SuccinctIndexedFileBuffer> propertySuccinctBufferMap) {
		this.propertySuccinctBufferMap = propertySuccinctBufferMap;
	}
    
  
}
