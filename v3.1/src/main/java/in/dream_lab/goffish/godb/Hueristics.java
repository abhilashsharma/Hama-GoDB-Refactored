package in.dream_lab.goffish.godb;

import java.io.Serializable;
import java.util.HashMap;


public class Hueristics implements Serializable,IGraphStatistics{
	private static final long serialVersionUID = 1L;
	public Double numVertices=new Double(0);
	public Double numRemoteVertices=new Double(0);
	public Double numEdges=new Double(0);
	public HashMap<String,HashMap<String,vertexPredicateStats>> vertexPredicateMap;
	public HashMap<String,HashMap<String,edgePredicateStats>> edgePredicateMap;
	
	public Hueristics(){
		vertexPredicateMap = new HashMap<String,HashMap<String,vertexPredicateStats>>();
		edgePredicateMap = new HashMap<String,HashMap<String,edgePredicateStats>>();
	}
	
	public double probabilityOfVertex(String property,String value){
		if(this.vertexPredicateMap.get(property)==null){
			return -1;
		}
		return this.vertexPredicateMap.get(property).get(value).probability;
	}
	
	public double probabilityOfEdge(String property,String value){
		if(this.edgePredicateMap.get(property)==null){
			return -1;
		}
			
		return this.edgePredicateMap.get(property).get(value).probability;
	}
	
	public double avgDeg(String property,String value,boolean direction,boolean forORrev){
		
		if(forORrev==true){
			
		
		if(direction == true){
			if ( property == null) {
				return this.numEdges/this.numVertices;
				
			}	
			else { 
				return this.vertexPredicateMap.get(property).get(value).avgOutDegree; 
					
			}	
		}else if(direction == false){
			if ( property == null) {
				return this.numEdges/this.numVertices;
				

			}	
			else { 
				return this.vertexPredicateMap.get(property).get(value).avgInDegree;
					
			}		
		}
		
		}else if(forORrev==false){
			if(direction == true){
				if ( property == null) {
					return this.numEdges/this.numVertices;
					
				}	
				else { 
					return this.vertexPredicateMap.get(property).get(value).avgInDegree; 
						
				}	
			}else if(direction == false){
				if ( property == null) {
					return this.numEdges/this.numVertices;
					

				}	
				else { 
					return this.vertexPredicateMap.get(property).get(value).avgOutDegree;
						
				}		
			}
			
		}
		
		return 0;
	}
	
	
	public double avgRemoteDeg(String property,String value,boolean direction,boolean forORrev){
		double avgdeg=avgDeg(null,null,true,true);
		if(forORrev==true){
			
		if(direction == true){
			if (property == null) {
				
				return this.numRemoteVertices/(this.numVertices+this.numRemoteVertices)* avgdeg;
			}	
			else { 
				 
				return this.vertexPredicateMap.get(property).get(value).avgRemoteOutDegree ;	
			}	
		}else if(direction == false){
			if ( property == null) {
				
				return this.numRemoteVertices/(this.numVertices+this.numRemoteVertices)* avgdeg;

			}	
			else { 
				
				return this.vertexPredicateMap.get(property).get(value).avgRemoteInDegree ;	
			}		
		}
		
		}else if(forORrev==false){
			if(direction == true){
				if (property == null) {
					
					return this.numRemoteVertices/(this.numVertices+this.numRemoteVertices)* avgdeg;
				}	
				else { 
					 
					return this.vertexPredicateMap.get(property).get(value).avgRemoteInDegree ;	
				}	
			}else if(direction == false){
				if ( property == null) {
					
					return this.numRemoteVertices/(this.numVertices+this.numRemoteVertices)* avgdeg;

				}	
				else { 
					
					return this.vertexPredicateMap.get(property).get(value).avgRemoteOutDegree ;	
				}		
			}
			
			
		}
		return 0;
	}
	
}
