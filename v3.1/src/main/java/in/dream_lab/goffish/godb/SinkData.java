package in.dream_lab.goffish.godb;


//For InEdges
public class SinkData
{       
        String Attribute;
        String Value;
        long srcId;
	long EdgeId;
	boolean isRemote;
	Long sinkSubgraphId;

	public SinkData(String _Attribute,String _Value,long _srcId,long _EdgeId,boolean _isRemote, Long _sinkSubgraphId)
	{
	        this.Attribute=_Attribute;
	        this.Value=_Value;
		this.srcId=_srcId;
		this.EdgeId=_EdgeId;
		this.isRemote=_isRemote;
		this.sinkSubgraphId = _sinkSubgraphId;
	}
	
	public SinkData(String _Attribute,String _Value,long srcId,int _EdgeId,boolean _isRemote, Long _sinkSubgraphId)
        {
                this.Attribute=_Attribute;
                this.Value=_Value;
                this.srcId=srcId;
                this.EdgeId=_EdgeId;
                this.isRemote=_isRemote;
                this.sinkSubgraphId = _sinkSubgraphId;
        }
	
	public long getEdgeId(){
	  return this.EdgeId;
	}
	
	public long getSinkSubgraphId(){
	  return sinkSubgraphId;
	}
	
	public boolean isRemote(){
	  return isRemote;
	}
	
	public String getValue(){
	  return Value;
	}
	
	public long getSrcId(){
	  return srcId;
	}
}
