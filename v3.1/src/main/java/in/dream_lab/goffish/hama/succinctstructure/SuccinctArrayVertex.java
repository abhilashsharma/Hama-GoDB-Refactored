package in.dream_lab.goffish.hama.succinctstructure;

import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.pathDistrSuccinctStructure;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.commons.math.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sandy on 9/9/17.
 */
public class SuccinctArrayVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable> implements IVertex<V, E, I, J> {
//	public static final Log LOG = LogFactory.getLog(SuccinctVertex.class);
	public static final Log LOG = LogFactory.getLog(SuccinctArrayVertex.class);
	private I vid;
    private List<SuccinctIndexedFileBuffer> vbufferList, ebufferList;
    private char delim;
    public SuccinctArrayVertex(I vid, List<SuccinctIndexedFileBuffer> vbufferList, List<SuccinctIndexedFileBuffer> ebufferList, char delim)
    {
        this.vid = vid;
        this.vbufferList = vbufferList;
        this.ebufferList = ebufferList;
        this.delim = delim;
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
    	SuccinctIndexedFileBuffer ebuffer = null;
    	Long searchQuery=((LongWritable)vid).get();
    	LOG.info("GETEDGES search:" + searchQuery.toString().concat("@") );
    	LOG.info("EBUFFER size:" + ebufferList.size());
    	for(SuccinctIndexedFileBuffer ebuf:ebufferList) {
    		long count = ebuf.count(searchQuery.toString().concat("@").getBytes());
    		if(count > 0) {
    			LOG.info("FOUND COUNT to be:" +count);
    			ebuffer=ebuf;
    			break;
    		}
    	}
    	
    	int offset;
        String[] tokens;
        
        String record;
        List<Long> localSinks = new ArrayList<>();
        List<Long> remoteSinks = new ArrayList<>();
        if(ebuffer==null) {
        	LOG.info("Returning null edge buffer");
    	 	return new Tuple<>(localSinks, remoteSinks); 
    	}
        Integer[] recordID = ebuffer.recordSearchIds(searchQuery.toString().concat("@").getBytes());
        for (Integer rid : recordID)
        {
            offset = ebuffer.getRecordOffset(rid);
            record = ebuffer.extractUntil(offset, delim);
            tokens=record.split("\\W");
            for(int i=3; i < 3 + Integer.parseInt(tokens[2]); i++) 
                localSinks.add(Long.parseLong(tokens[i]));
            for(int i=3 + Integer.parseInt(tokens[2]); i < tokens.length; i++) 
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
    public String getPropforVertex(int index)
    {	
    	Long searchQuery=((LongWritable)vid).get();
    	SuccinctIndexedFileBuffer vbuffer=null;
		for(SuccinctIndexedFileBuffer vbuf:vbufferList) {
    		if(vbuf.count(searchQuery.toString().concat("@").getBytes()) ==1) {
    			vbuffer=vbuf;
    			break;
    		}
    	}
    	
        int offset;
        String[] tokens;
        String record;
        
        Integer[] recordID=vbuffer.recordSearchIds(searchQuery.toString().concat("@").getBytes());
        offset = vbuffer.getRecordOffset(recordID[0]);
        record = vbuffer.extractUntil(offset, '|');
        tokens=record.split("\\W");
        return tokens[index+1];
    }
    
    public String[] getAllPropforVertex()
    {
    	
    	Long searchQuery=((LongWritable)vid).get();
    	LOG.info("ALLPROP search:" + searchQuery.toString().concat("@") );
    	LOG.info("VBUFFER size:" + vbufferList.size());
    	SuccinctIndexedFileBuffer vbuffer=null;
		for(SuccinctIndexedFileBuffer vbuf:vbufferList) {
			long count=vbuf.count(searchQuery.toString().concat("@").getBytes());
    		if( count>0) {
    			LOG.info("FOUND COUNT to be:" +count);
    			vbuffer=vbuf;
    			break;
    		}
    	}
    	
		
		
        int offset;
        String[] tokens = null;
        String record;
        if(vbuffer==null) {
        	LOG.info("Returning null vertex buffer");
    	 	return tokens; 
    	}
        Integer[] recordID=vbuffer.recordSearchIds(searchQuery.toString().concat("@").getBytes());
        offset = vbuffer.getRecordOffset(recordID[0]);
        record = vbuffer.extractUntil(offset, '|');
        tokens=record.split("\\W");
        return tokens;
    }
}
