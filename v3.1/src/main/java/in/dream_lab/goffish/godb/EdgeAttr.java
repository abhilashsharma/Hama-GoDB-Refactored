package in.dream_lab.goffish.godb;


//For InEdges
public class EdgeAttr
{
	String Attr_name;
	String Value;
	long EdgeId;
	boolean isRemote;
	Long sinkSubgraphId;

	public EdgeAttr(String _Attr,String _Value,long _EdgeId,boolean _isRemote, Long _sinkSubgraphId)
	{
		this.Attr_name=_Attr;
		this.Value=_Value;
		this.EdgeId=_EdgeId;
		this.isRemote=_isRemote;
		this.sinkSubgraphId = _sinkSubgraphId;
	}
	
	public EdgeAttr(String _Attr,String _Value,int _EdgeId,boolean _isRemote, Long _sinkSubgraphId)
        {
                this.Attr_name=_Attr;
                this.Value=_Value;
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
}
