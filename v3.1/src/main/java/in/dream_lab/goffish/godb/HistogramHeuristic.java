package in.dream_lab.goffish.godb;

import java.io.Serializable;
import java.util.HashMap;

public class HistogramHeuristic implements Serializable,IGraphStatistics {

	private static final long serialVersionUID = 1L;
	
	public Double numVertices=new Double(0);
	public Double numRemoteVertices=new Double(0);
	public Double numEdges=new Double(0);
	//VertexHistogram will contain Histogram for frequency, avg. outdegree, avg. indegree for each property
	HashMap<String,VertexHistogram> vertex;
	//EdgeHistogram will contain Histogram for frequency for each property
	HashMap<String,EdgeHistogram> edge;
	
	public HistogramHeuristic(){
		this.vertex=new HashMap<String,VertexHistogram>();
		this.edge=new HashMap<String,EdgeHistogram>();
	}
	
	public int addVertexProperty(String prop,VertexHistogram H){
	
		if(!vertex.containsKey(prop)){
			vertex.put(prop, H);
			return 0;
		}
		else return -1;
	}
	
	public int addEdgeProperty(String prop, EdgeHistogram H){
		if(!edge.containsKey(prop)){
			edge.put(prop, H);
			return 0;
		}
		else return -1;
	}

	@Override
	public double probabilityOfVertex(String property, String value) {
		// TODO Auto-generated method stub
		return vertex.get(property).FrequencyHistogram.probability_of_element(value);
	}

	@Override
	public double probabilityOfEdge(String property, String value) {
		// TODO Auto-generated method stub
		return edge.get(property).FrequencyHistogram.probability_of_element(value);
	}

	@Override
	public double avgDeg(String property, String value, boolean direction, boolean forORrev) {
		// TODO Auto-generated method stub
		if(property==null){
			return this.numEdges/this.numVertices;
		}else{
			if(direction==true){
				if(forORrev==true){
					Histograms H= vertex.get(property).AvgOutDegHistogram;
					if(H.Total==0)
						return 0;
					return H.probability_of_element(value)*H.Total;
				}else
				{
					Histograms H= vertex.get(property).AvgInDegHistogram;
					if(H.Total==0)
						return 0;
					return H.probability_of_element(value)*H.Total;
				}
				
				
				
			}else {
				if(forORrev==true){
					Histograms H= vertex.get(property).AvgInDegHistogram;
					if(H.Total==0)
						return 0;
					return H.probability_of_element(value)*H.Total;
					
				}else
				{
					Histograms H= vertex.get(property).AvgOutDegHistogram;
					if(H.Total==0)
						return 0;
					return H.probability_of_element(value)*H.Total;
					
				}
				
			}
			
		}
	}

	@Override
	public double avgRemoteDeg(String property, String value, boolean direction, boolean forORrev) {
		// TODO Auto-generated method stub
		if(property==null){
			return this.numEdges/this.numVertices;
		}else{
			if(direction==true){
				if(forORrev==true){
					Histograms H= vertex.get(property).AvgRemoteOutDegHistogram;
					if(H.Total==0)
						return 0;
					return H.probability_of_element(value)*H.Total;
				}else
				{
					Histograms H= vertex.get(property).AvgRemoteInDegHistogram;
					if(H.Total==0)
						return 0;
					return H.probability_of_element(value)*H.Total;
				}
				
				
				
			}else {
				if(forORrev==true){
					Histograms H= vertex.get(property).AvgRemoteInDegHistogram;
					if(H.Total==0)
						return 0;
					return H.probability_of_element(value)*H.Total;
					
				}else
				{
					Histograms H= vertex.get(property).AvgRemoteOutDegHistogram;
					if(H.Total==0)
						return 0;
					return H.probability_of_element(value)*H.Total;
					
				}
				
			}
			
		}
	}
}
