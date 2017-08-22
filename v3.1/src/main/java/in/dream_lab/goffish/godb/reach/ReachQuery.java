package in.dream_lab.goffish.godb.reach;

import java.util.ArrayList;
import java.util.regex.Pattern;



public class ReachQuery {

	/**
	 * BFS query(with depth)
	 * Example "label:string[toyota___rav_4]:4//0//163"
	 * 
	 */
	public ReachQuery(String queryParam) {
	  String[] _string = queryParam.split(Pattern.quote("//"))[0].split(Pattern.quote("@"));
	  depth = Integer.valueOf(_string[0]);
          
          for (int i=1;i<3;i++)
          {
                          String p = _string[i].split(Pattern.quote(":"))[0];
                          String typeAndValue = _string[i].split(Pattern.quote(":"))[1];
                          Object v = null;
                          String type = typeAndValue.substring(0, typeAndValue.indexOf("["));
                          if(type.equals("float")) {
                                  v = Float.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );
                          }
                          else if(type.equals("double")) { 
                                  v = Double.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

                          }
                          else if(type.equals("int")) { 
                                  v = Integer.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

                          }
                          else { 
                                  v = String.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

                          }
                          
                          if (i==1){
                            sourcePropertyName=p;
                            sourcePropertyValue=v;
                          }
                                  
                          if (i==2){
                            sinkPropertyName=p;
                            sinkPropertyValue=v;
                          }
                                  
          }


	}

	

	private String sourcePropertyName;
	private Object sourcePropertyValue;
	private String sinkPropertyName;
        private Object sinkPropertyValue;
	private int depth;

	public String getSourcePropertyName() {
		return sourcePropertyName;
	}

	public Object getSourcePropertyValue() {
		return sourcePropertyValue;
	}
	
	public String getSinkPropertyName() {
                return sinkPropertyName;
        }

        public Object getSinkPropertyValue() {
                return sinkPropertyValue;
        }
        
        
	public int getDepth() {
		return depth;
	}
	
	public void setDepth(int newDepth) {
          depth=newDepth;
        }
	
	
}
