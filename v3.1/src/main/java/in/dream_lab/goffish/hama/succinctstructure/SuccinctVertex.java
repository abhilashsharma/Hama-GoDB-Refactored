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
public class SuccinctVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable> implements IVertex<V, E, I, J> {
//	public static final Log LOG = LogFactory.getLog(SuccinctVertex.class);
	private I vid;
    private SuccinctIndexedFileBuffer vbuffer, ebuffer;
    private char delim;
    public SuccinctVertex(I vid, SuccinctIndexedFileBuffer vbuffer, SuccinctIndexedFileBuffer ebuffer, char delim)
    {
        this.vid = vid;
        this.vbuffer = vbuffer;
        this.ebuffer = ebuffer;
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
    	int offset;
        String[] tokens;
        String record;
        List<Long> localSinks = new ArrayList<>();
        List<Long> remoteSinks = new ArrayList<>();
        Long searchQuery=((LongWritable)vid).get();
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
        Integer[] recordID = ebuffer.recordSearchIds(searchQuery.toString().concat("@").getBytes());
//        LOG.info("RecordIDLength:"+ recordID.length);
        for (Integer rid : recordID)
        {
            offset = ebuffer.getRecordOffset(rid);
            record = ebuffer.extractUntil(offset, delim);
            tokens=record.split("\\W");
//            LOG.info("tokenLength:"+ tokens.length);
            // TODO: Implement Better Solution for below FOR loop @Swapnil
            for(int i=3; i < 3 + Integer.parseInt(tokens[2]); i++) {
                localsinks.add(new SuccinctEdge<E, I, J>((I)new LongWritable(Long.parseLong(tokens[i]))));
            }
        }
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
        Integer[] recordID = ebuffer.recordSearchIds(searchQuery.toString().concat("@").getBytes());
        for (Integer rid : recordID)
        {
            offset = ebuffer.getRecordOffset(rid);
            record = ebuffer.extractUntil(offset, delim);
            tokens = record.split("\\W");
            // TODO: Implement Better Solution for below FOR loop @Swapnil
            for(int i = 3 + Integer.parseInt(tokens[2]); i < tokens.length; i++) {
                remotesinks.add(new SuccinctEdge<E, I, J>((I)new LongWritable(Long.parseLong(tokens[i]))));
            }
        }
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
        int offset;
        String[] tokens;
        String record;
        Long searchQuery=((LongWritable)vid).get();
        Integer[] recordID=vbuffer.recordSearchIds(searchQuery.toString().concat("@").getBytes());
        offset = vbuffer.getRecordOffset(recordID[0]);
        record = vbuffer.extractUntil(offset, '|');
        tokens=record.split("\\W");
        return tokens[index+1];
    }
}
