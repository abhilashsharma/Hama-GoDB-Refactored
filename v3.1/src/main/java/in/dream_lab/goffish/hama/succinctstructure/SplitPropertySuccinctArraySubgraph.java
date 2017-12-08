package in.dream_lab.goffish.hama.succinctstructure;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
public class SplitPropertySuccinctArraySubgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> implements ISubgraph<S, V, E, I, J, K> {
    public Map<Long, Long> remotevertexToSubgraph;
    public K subgraphId;
    S _value;
    public static final org.apache.commons.logging.Log Log = LogFactory.getLog(SuccinctArraySubgraph.class);
    SuccinctIndexedFileBuffer vertexSuccinctBuffer;
    Object2ObjectOpenHashMap<String, SuccinctIndexedFileBuffer> propertySuccinctBufferMap;
    ObjectArrayList<SuccinctIndexedFileBuffer> edgeSuccinctBufferList;
    private static Splitter splitter;
    public SplitPropertySuccinctArraySubgraph(K subgraphId, SuccinctIndexedFileBuffer vertexSuccinctBuffer, Object2ObjectOpenHashMap<String, SuccinctIndexedFileBuffer> propertySuccinctBufferMap, ObjectArrayList<SuccinctIndexedFileBuffer> edgeSuccinctBufferList)
    {
        this.vertexSuccinctBuffer = vertexSuccinctBuffer;
        this.propertySuccinctBufferMap = propertySuccinctBufferMap;//TODO: to populate this while reading graph
        this.edgeSuccinctBufferList = edgeSuccinctBufferList;
        this.subgraphId = subgraphId;
        splitter = Splitter.createSplitter();
    }
    public SuccinctIndexedFileBuffer getVertexBuffer()
    {
        return vertexSuccinctBuffer;
    }
    public SuccinctIndexedFileBuffer getEdgeBuffer(int index)
    {
        return edgeSuccinctBufferList.get(index);
    }
    public ObjectArrayList<SuccinctIndexedFileBuffer> getEdgeBufferList()
    {
        return edgeSuccinctBufferList;
    }
    public SuccinctIndexedFileBuffer getPropertyBuffer(String name)
    {
        return propertySuccinctBufferMap.get(name);
    }
    
    public Object2ObjectOpenHashMap<String, SuccinctIndexedFileBuffer> getPropertyBufferMap()
    {
        return propertySuccinctBufferMap;
    }
    
    public void setRemoteMap(Map<Long, Long> v2s)
    {
        this.remotevertexToSubgraph = v2s;
    }
    public Map<Long, Long> getRemoteMap()
    {
        return remotevertexToSubgraph;
    }
    public K getSubgraphId()
    {
        return subgraphId;
    }
    public IVertex<V, E, I, J> getVertexById(I vertexId)
    {
        throw new UnsupportedOperationException("Function not supported");
    }
    public long getVertexCount()
    {
        throw new UnsupportedOperationException("We do not count the number of vertices");
    }
    public long getLocalVertexCount()
    {
        throw new UnsupportedOperationException("We do not count the number of local vertices");
    }
    public Iterable<IVertex<V, E, I, J>> getVertices()
    {
        throw new UnsupportedOperationException("We do not get the list of vertices from a subgraph");
    }
    public Iterable<IVertex<V, E, I, J>> getLocalVertices()
    {
        throw new UnsupportedOperationException("We do not get the list of local vertices");
    }
    public Iterable<IRemoteVertex<V, E, I, J, K>> getRemoteVertices()
    {
        throw new UnsupportedOperationException("We do not get the list of remote vertices");
    }
    public Iterable<IEdge<E, I, J>> getOutEdges()
    {
        throw new UnsupportedOperationException("We do not get the list of out edges");
    }
    public IEdge<E, I, J> getEdgeById(J edgeID)
    {
        throw new UnsupportedOperationException("We do not get an edge by its id");
    }
    @Override
    public void setSubgraphValue(S value) {
        _value = value;
    }

    @Override
    public S getSubgraphValue() {

        return _value;
    }
    //TODO:Change This..DONE
    public List<Long> getVertexIDs()
    {
        List<Long> vertices = new ArrayList<>();
        long size = vertexSuccinctBuffer.count("#".getBytes());
        Integer offset;
        byte[] v;
        long start;
        Log.info("getVertxIDs");
        for (int i = 0; i < size; i++) {
            start = System.nanoTime();
            offset = vertexSuccinctBuffer.getRecordOffset(i);
            Log.info("Lookup record offset(vertex): "+ (System.nanoTime() - start) + " ns\n"+"Size: "+offset.toString().length());
            start = System.nanoTime();
            v = vertexSuccinctBuffer.extractBytesUntil(offset, '@');
            Log.info("VFORMAT:" +v);
            Log.info("Extract until(vertex): "+(System.nanoTime() - start) + " ns");
            vertices.add(splitter.splitLong(v).getLong(1));
        }
        return vertices;
    }
    //TODO:Change this..DONE
    public List<Long> getVertexByProp(String name, String value)//TODO: remove delimiter
    {
        long start = System.nanoTime();
        List<Long> vid = new ArrayList<>();
        Log.info("getVertexByProp");
        SuccinctIndexedFileBuffer propBuffer = propertySuccinctBufferMap.get(name);
//        Integer offset;
        byte[] record;
        LongArrayList tokens;
        long startFine = System.nanoTime();
        Integer[] recordID = propBuffer.recordSearchIds(("#" +value + "@").getBytes());
        Log.info("Lookup record id(vertex): "+ (System.nanoTime() - startFine)+ " ns " + recordID.length);
            for (Integer rid : recordID)
           {
//              startFine = System.nanoTime();
//              offset = vertexSuccinctBuffer.getRecordOffset(rid);
//              Log.info("Lookup record offset(vertex): " + (System.nanoTime() - startFine) + " ns");
              startFine = System.nanoTime();
              record = vertexSuccinctBuffer.getRecordBytes(rid);
              Log.info("Extract until(vertex): "+(System.nanoTime() - startFine) + " ns");
              Log.info("# Extracted Bytes: " + record.length);
              tokens=splitter.splitLong(record);
              for(long token : tokens)
                  vid.add(token);
           }
        Log.info("Querying Time:" + (System.nanoTime() - start));
        return vid;
    }
}
