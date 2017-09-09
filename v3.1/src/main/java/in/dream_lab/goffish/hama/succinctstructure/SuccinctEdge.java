package in.dream_lab.goffish.hama.succinctstructure;

import in.dream_lab.goffish.api.IEdge;
import org.apache.hadoop.io.Writable;

/**
 * Created by sandy on 9/9/17.
 */
public class SuccinctEdge<E extends Writable, I extends Writable, J extends Writable> implements IEdge<E, I , J> {
    private I sinkid;
    public SuccinctEdge(I sinkid)
    {
        this.sinkid = sinkid;
    }
    public J getEdgeId()
    {
        throw new  UnsupportedOperationException("Edge Id's are not stored");
    }
    public I getSinkVertexId()
    {
        return sinkid;
    }
    public E getValue()
    {
        throw new UnsupportedOperationException("Edges don't have values as of now");
    }
    @Override
    public void setValue(E value)
    {
        throw new UnsupportedOperationException("Edges don't have values as of now");
    }
}
