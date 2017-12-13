package in.dream_lab.goffish.hama.succinctstructure;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.commons.math.Tuple;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static in.dream_lab.goffish.hama.succinctstructure.SuccinctArraySubgraph.Log;
/**
 * Created by sandy on 9/9/17.
 */
public class SplitPropertySuccinctArrayVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable> implements IVertex<V, E, I, J> {
    public static final Log LOG = LogFactory.getLog(SuccinctArrayVertex.class);
    private I vid;
    private ObjectArrayList<SuccinctIndexedFileBuffer> ebufferList;
    private SuccinctIndexedFileBuffer vbuffer;
    private Object2ObjectOpenHashMap<String, SuccinctIndexedFileBuffer> propertySuccinctBufferMap;
    private static Splitter splitter;
    public SplitPropertySuccinctArrayVertex(I vid, SuccinctIndexedFileBuffer vbuffer, ObjectArrayList<SuccinctIndexedFileBuffer> ebufferList, Object2ObjectOpenHashMap<String, SuccinctIndexedFileBuffer> propertySuccinctBufferMap)
    {
        this.vid = vid;
        this.vbuffer = vbuffer;
        this.ebufferList = ebufferList;
        this.propertySuccinctBufferMap = propertySuccinctBufferMap;
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
    public Tuple<List<Long>, List<Long>> getOEdges()
    {
        Log.info("getEdges");
        SuccinctIndexedFileBuffer ebuffer = null;
        long searchQuery=((LongWritable)vid).get();
//    	LOG.info("GETEDGES search:" + searchQuery.toString().concat("@") );
//    	LOG.info("EBUFFER size:" + ebufferList.size());
        byte[] wholeQuery= ("#" + searchQuery + "@").getBytes();//TODO: # is not used now, remove that..DONE
        long countStart=System.nanoTime();
        int iteration=0;
        for(SuccinctIndexedFileBuffer ebuf:ebufferList) {
            iteration++;
            long count = ebuf.count(wholeQuery);
            if(count > 0) {
//    			LOG.info("FOUND COUNT to be:" +count);
                ebuffer=ebuf;
                break;
            }
        }
        LOG.info("Count(Edge): " + (System.nanoTime()-countStart) + " " + iteration);
//        int offset;
        LongArrayList tokens;
        byte[] record;
        List<Long> localSinks = new ArrayList<>();
        List<Long> remoteSinks = new ArrayList<>();

        if(ebuffer==null) {
//        	LOG.info("Returning null edge buffer");
            return new Tuple<>(localSinks, remoteSinks);
        }
        long start = System.nanoTime();
        Integer[] recordID = ebuffer.recordSearchIds(wholeQuery);
        LOG.info("Lookup record id(edge): " + (System.nanoTime() - start) + " ns " + recordID.length);
        for (Integer rid : recordID)//TODO: remove this as the number of records is only 1.
        {
//            start = System.nanoTime();
//            offset = ebuffer.getRecordOffset(rid);
//            LOG.info("Lookup record offset(edge): " + (System.nanoTime() - start) + " ns");
            start = System.nanoTime();
            record = ebuffer.getRecordBytes(rid);//TODO: use getRecordBytes instead of this...DONE
            LOG.info("Extract until(edge): " + (System.nanoTime() - start) + " ns" );
            LOG.info("# Extracted Bytes: " + record.length);
            tokens = splitter.splitLong(record);
 
            for(int i=2; i < 2 + tokens.getLong(1); i++)
                localSinks.add(tokens.getLong(i));
            for(long i= 2 + tokens.getLong(1); i < tokens.size(); i++)
                remoteSinks.add(tokens.getLong((int)i));
        }
        return new Tuple<>(localSinks, remoteSinks);
    }
    
    /**
     * returns both LocalSinkList and RemoteSinkList
     * @return
     */
    public Tuple<List<Long>, List<Long>> getIEdges()
    {
        Log.info("getEdges");
        SuccinctIndexedFileBuffer ebuffer = null;
        long searchQuery=((LongWritable)vid).get();
//    	LOG.info("GETEDGES search:" + searchQuery.toString().concat("@") );
//    	LOG.info("EBUFFER size:" + ebufferList.size());
        byte[] wholeQuery= ("#" + searchQuery + "@").getBytes();//TODO: # is not used now, remove that..DONE
        long countStart=System.nanoTime();
        int iteration=0;
        for(SuccinctIndexedFileBuffer ebuf:ebufferList) {//TODO: iterate through the Inedge buffer list
            iteration++;
            long count = ebuf.count(wholeQuery);
            if(count > 0) {
//    			LOG.info("FOUND COUNT to be:" +count);
                ebuffer=ebuf;
                break;
            }
        }
        LOG.info("Count(Edge): " + (System.nanoTime()-countStart) + " " + iteration);
//        int offset;
        LongArrayList tokens;
        byte[] record;
        List<Long> localSinks = new ArrayList<>();
        List<Long> remoteSinks = new ArrayList<>();
        if(ebuffer==null) {
//        	LOG.info("Returning null edge buffer");
            return new Tuple<>(localSinks, remoteSinks);
        }
        long start = System.nanoTime();
        Integer[] recordID = ebuffer.recordSearchIds(wholeQuery);
        LOG.info("Lookup record id(edge): " + (System.nanoTime() - start) + " ns " + recordID.length);
        for (Integer rid : recordID)//TODO: remove this as the number of records is only 1.
        {
//            start = System.nanoTime();
//            offset = ebuffer.getRecordOffset(rid);
//            LOG.info("Lookup record offset(edge): " + (System.nanoTime() - start) + " ns");
            start = System.nanoTime();
            record = ebuffer.getRecordBytes(rid);//TODO: use getRecordBytes instead of this.
            LOG.info("Extract until(edge): " + (System.nanoTime() - start) + " ns" );
            LOG.info("# Extracted Bytes: " + record.length);
            tokens = splitter.splitLong(record);
            for(int i=2; i < 2 + tokens.getLong(1); i++)
                localSinks.add(tokens.getLong(i));
            for(long i= 2 + tokens.getLong(1); i < tokens.size(); i++)
                remoteSinks.add(tokens.getLong((int)i));
        }
        return new Tuple<>(localSinks, remoteSinks);
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
    public String getPropforVertex(String name)
    {
        Log.info("getPropforVertex");
        String searchQuery=("#" +((LongWritable)vid).get() + "@");
        byte[] wholeQuery= searchQuery.toString().getBytes();
        SuccinctIndexedFileBuffer propBuffer = propertySuccinctBufferMap.get(name);
//        int offset;
        byte[] record;
//        String r=null;//USED FOR TESTING
        long start = System.nanoTime();
        Integer[] recordId=vbuffer.recordSearchIds(wholeQuery);
        LOG.info("Lookup record id(property): " + (System.nanoTime() - start) + " ns");
//        start = System.nanoTime();
//        offset = propBuffer.getRecordOffset(recordID[0]);
//        LOG.info("Lookup record offset(property): " + (System.nanoTime() - start) + " ns");
        start = System.nanoTime();
//        record = propBuffer.getRecordBytes(recordId[0]);
        String sRecord = propBuffer.getRecord(recordId[0]);
        LOG.info("Extract until(property): " + (System.nanoTime() - start) + " ns");
        LOG.info("# Extracted Bytes: " + sRecord.length());

        return sRecord.substring(1, sRecord.length()-1);
    }
    
//TODO: Only for query generation
    public List<String> getAllPropforVertex()
    {
        List<String> props = new ArrayList<>();
        Long searchQuery=((LongWritable)vid).get();
        byte[] wholeQuery= searchQuery.toString().getBytes();
//    	LOG.info("ALLPROP search:" + searchQuery.toString().concat("@") );
//    	LOG.info("VBUFFER size:" + vbufferList.size());
        int offset;
        byte[] record;
        Integer[] recordID=vbuffer.recordSearchIds(wholeQuery);
        for (SuccinctIndexedFileBuffer propBuffer: propertySuccinctBufferMap.values()) {
            offset = propBuffer.getRecordOffset(recordID[0]);
            props.add((String)splitter.splitString(propBuffer.extractBytesUntil(offset, '|')).get(0));
        }
        return props;
    }
	@Override
	public Iterable<IEdge<E, I, J>> getOutEdges() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Get OutEdges is supported using getOEdges");
	}
}
