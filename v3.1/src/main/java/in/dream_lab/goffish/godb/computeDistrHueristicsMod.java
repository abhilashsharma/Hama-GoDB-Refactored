package in.dream_lab.goffish.godb;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;

import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.ISubgraphWrapup;


public class computeDistrHueristicsMod 
extends AbstractSubgraphComputation<pathDistrSubgraphState, MapWritable, MapWritable, Text, LongWritable, LongWritable, LongWritable>
implements ISubgraphWrapup{


  
        public computeDistrHueristicsMod(String initMsg){
          argument=initMsg;
        }
        String argument = null;
	private HueristicsTreeMap subgraphHueristics=new HueristicsTreeMap();
	private Set<String> vertexProperties = new HashSet<String>();
	private Set<String> edgeProperties = new HashSet<String>();
	private HistogramHeuristic subgraphHistogram=new HistogramHeuristic();
	private HashMap<Long,Double> inVerticesMap = new HashMap<Long,Double>();
	
	FileOutputStream fos;
	ObjectOutputStream oos;
	String heuristicsBasePath = ConfigFile.basePath+"heuristics/hue_";
	
	
	

	
				long time;
				
	@Override
	public void compute(Iterable<IMessage<LongWritable, Text>> messageList) {
		
		// SUPERSTEP TO SEND MESSAGE ALONG REMOTE EDGES
		// THIS IS TO NOTIFY THE REMOTE VERTICES IN THE 
		// OTHER SUBGRAPH THAT THEY HAVE AN INCOMING EDGE
		// REMOTE SOURCE OF INCOMING EDGES ARE MAINTAINED 
		// IN A TEMPORARY MAP 
		//TODO: REMOVE THIS HACK AND ADD ACTUAL SUPPORT FOR THIS
		
	      if(getSuperstep()==0){
	        Text sgid=new Text(getSubgraph().getSubgraphId().toString());
                sendToAll(sgid);
	      }

		if(  getSuperstep() == 1) {
		  long minSubgraphId = Long.MAX_VALUE;
                  for(IMessage<LongWritable,Text> m:messageList){
                    long sgid=Long.parseLong(m.getMessage().toString());
                    if(sgid<minSubgraphId){
                      minSubgraphId=sgid;
                    }
                  }
		  if(getSubgraph().getSubgraphId().get()==minSubgraphId){
			try{/*Directly reading the gathered heuristics*/
				FileInputStream fis = new FileInputStream(heuristicsBasePath+String.valueOf("FULL")+".ser"); 
				ObjectInputStream ois = new ObjectInputStream(fis);
				subgraphHueristics = (HueristicsTreeMap)ois.readObject();
				ois.close();
			}catch(Exception e){e.printStackTrace();}
			
			subgraphHistogram.numEdges=subgraphHueristics.numEdges;
			subgraphHistogram.numVertices=subgraphHueristics.numVertices;
			subgraphHistogram.numRemoteVertices=subgraphHueristics.numRemoteVertices;
			//for each property , populate a EdgeHistogram Object
			
			Iterator<String> it=edgeProperties.iterator();
			while(it.hasNext()){
				String prop=it.next();
				System.out.println("Edge PROPERTY:" + prop);
				TreeMap<String,edgePredicateStats> edgeValueMap=subgraphHueristics.edgePredicateMap.get(prop);
			    Object[] KeyArr=edgeValueMap.keySet().toArray();
				ArrayList<Double> FreqList=new ArrayList<Double>();
				long Total=0;
				for(Object Key: KeyArr){
					FreqList.add(edgeValueMap.get(Key).numberMatchingPredicate);
					Total+=edgeValueMap.get(Key).numberMatchingPredicate;
				}
				Double[] freqArr=FreqList.toArray(new Double[FreqList.size()]);
				EquiDepthQuantile e=new EquiDepthQuantile(100, KeyArr, freqArr, Total);
				Histograms H=new Histograms(e.quantileArray(),e.number_per_bin,e.frequency_per_bin,e.Total);
				EdgeHistogram EHist=new EdgeHistogram(H);
				subgraphHistogram.addEdgeProperty(prop, EHist);
			
			}
			//for each property , create three Histograms and populate Vertex Histogram Object
			vertexProperties.add("patid");
                        vertexProperties.add("nclass");
                        vertexProperties.add("country");
			it=vertexProperties.iterator();
			while(it.hasNext()){
				
				String prop=it.next();
				System.out.println("Vertex PROPERTY:"+prop);
				TreeMap<String,vertexPredicateStats> vertexValueMap=subgraphHueristics.vertexPredicateMap.get(prop);
				Object[] KeyArr=vertexValueMap.keySet().toArray();
				ArrayList<Double> FreqList=new ArrayList<Double>();
				ArrayList<Double> InDegreeList=new ArrayList<Double>();
				ArrayList<Double> OutDegreeList=new ArrayList<Double>();
				ArrayList<Double> remInDegreeList=new ArrayList<Double>();
				ArrayList<Double> remOutDegreeList=new ArrayList<Double>();
				long Total=0;
				long TotalIn=0;
				long TotalOut=0;
				long TotalRemIn=0;
				long TotalRemOut=0;
				for(Object Key: KeyArr){
					FreqList.add(vertexValueMap.get(Key).numberMatchingPredicate);
					InDegreeList.add(vertexValueMap.get(Key).avgInDegree);
					OutDegreeList.add(vertexValueMap.get(Key).avgOutDegree);
					remInDegreeList.add(vertexValueMap.get(Key).avgRemoteInDegree);
					remOutDegreeList.add(vertexValueMap.get(Key).avgRemoteOutDegree);
					Total+=vertexValueMap.get(Key).numberMatchingPredicate;
					TotalIn+=vertexValueMap.get(Key).avgInDegree;
					TotalOut+=vertexValueMap.get(Key).avgOutDegree;
					TotalRemIn+=vertexValueMap.get(Key).avgRemoteInDegree;
					TotalRemOut+=vertexValueMap.get(Key).avgRemoteOutDegree;
				}
				Double[] freqArr=FreqList.toArray(new Double[FreqList.size()]);
				Double[] InDegArr=InDegreeList.toArray(new Double[InDegreeList.size()]);
				Double[] OutDegArr=OutDegreeList.toArray(new Double[OutDegreeList.size()]);
				Double[] remInDegArr=remInDegreeList.toArray(new Double[remInDegreeList.size()]);
				Double[] remOutDegArr=remOutDegreeList.toArray(new Double[remOutDegreeList.size()]);
				
				//TODO: dynamically calculate number of bins
				EquiDepthQuantile e1=new EquiDepthQuantile(1000, KeyArr, freqArr, Total);
				
				Histograms H1=new Histograms(e1.quantileArray(),e1.number_per_bin,e1.frequency_per_bin,e1.Total);
				
				EquiDepthQuantile e2=new EquiDepthQuantile(1000, KeyArr, InDegArr, TotalIn);
				Histograms H2=new Histograms(e2.quantileArray(),e2.number_per_bin,e2.frequency_per_bin,e2.Total);
				
				EquiDepthQuantile e3=new EquiDepthQuantile(1000, KeyArr, OutDegArr, TotalOut);
				Histograms H3=new Histograms(e3.quantileArray(),e3.number_per_bin,e3.frequency_per_bin,e3.Total);
				
				EquiDepthQuantile e4=new EquiDepthQuantile(1000, KeyArr, remOutDegArr, TotalRemOut);
				Histograms H4=new Histograms(e4.quantileArray(),e4.number_per_bin,e4.frequency_per_bin,e4.Total);
				
				EquiDepthQuantile e5=new EquiDepthQuantile(1000, KeyArr, remInDegArr, TotalRemIn);
				Histograms H5=new Histograms(e5.quantileArray(),e5.number_per_bin,e5.frequency_per_bin,e5.Total);
				System.out.println("Quantile arrays:" + e1.quantileArray().length + " " + e2.quantileArray().length + " " + e3.quantileArray().length + " " + e4.quantileArray().length + " " + e5.quantileArray().length + " ");
				
				VertexHistogram VHist=new VertexHistogram(H1, H3, H2,H4,H5);
				subgraphHistogram.addVertexProperty(prop, VHist);
			}
			
			try
			{
			
				System.out.println("Writing Subgraph Histogram Object:");
				File file = new File(heuristicsBasePath+String.valueOf(getSubgraph().getSubgraphId())+"Hist"+".ser");
				if ( !file.exists() ) {
				
					file.getParentFile().mkdirs();
					file.createNewFile();
				
				}
				fos = new FileOutputStream(file); 
				oos = new ObjectOutputStream(fos);
				oos.writeObject(subgraphHistogram);
				oos.close();
			}catch(Exception e){
				//
			}
			
		  }
			
		}
		
		if (getSuperstep()>=1)
			voteToHalt();
		
	}
	
	

  @Override
  public void wrapup() {
    // TODO Auto-generated method stub
    
  }

}
