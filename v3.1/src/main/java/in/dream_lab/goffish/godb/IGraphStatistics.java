package in.dream_lab.goffish.godb;

public interface IGraphStatistics {

	double probabilityOfVertex(String property,String value);
	double probabilityOfEdge(String property,String value);
	double avgDeg(String property,String value,boolean direction,boolean forORrev);
	double avgRemoteDeg(String property,String value,boolean direction,boolean forORrev);
	
}
