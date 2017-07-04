package in.dream_lab.goffish.godb.reach;

import in.dream_lab.goffish.godb.Hueristics;
import in.dream_lab.goffish.godb.IQueryOptimizer;


public class ReachabilityHeuristicsOptimizer implements IQueryOptimizer {

  Hueristics heuristics;
  
  Double[] queryCostHolder;
  Double networkCoeff;
  public ReachabilityHeuristicsOptimizer(Hueristics _heuristics,Double _networkCoeff){
    this.heuristics=_heuristics;
    this.queryCostHolder = new Double[2];
    this.networkCoeff=_networkCoeff;
  }
  /*
   * (non-Javadoc)
   * @see in.dream_lab.goffish.godb.reach.IQueryOptimizer#Optimize(java.lang.Object)
   * 
   * returns either source vertex or sink vertex as per graph statistics
   */
  @Override
  public Integer Optimize(Object query) {
    // TODO Auto-generated method stub
    ReachQuery reachQuery=(ReachQuery)query;
    String propertyName;
    Object propertyValue;
  //###################################COMPUTE HUERISTIC BASED QUERY COST#################################
//  {        
//          //###########################forward cost#######################################################
            {       
                    Double totalCost = new Double(0);
                    Double prevScanCost = heuristics.numVertices;
                    {
                            
                            {
                                    propertyName=reachQuery.getSourcePropertyName();
                                    propertyValue=reachQuery.getSourcePropertyValue();
                                    Double probability = null;
                                    
                                    if ( heuristics.vertexPredicateMap.get(propertyName).containsKey(propertyValue.toString()) )
                                                    probability = heuristics.vertexPredicateMap.get(propertyName).get(propertyValue.toString()).probability;
                                    else {
                                                    totalCost = new Double(-1);
                                    }
                                    
                                    Double avgDeg = null;
                                    Double avgRemoteDeg = null;
                                    System.out.println("Hue:"+ heuristics + " propertyName:" + propertyName + " propertyValue:" + propertyValue.toString());
                                    avgDeg = heuristics.vertexPredicateMap.get(propertyName).get(propertyValue.toString()).avgOutDegree; 
                                    avgRemoteDeg = heuristics.vertexPredicateMap.get(propertyName).get(propertyValue.toString()).avgRemoteOutDegree;
                                    if (totalCost!=-1)
                                            totalCost = prevScanCost * probability *  ( avgDeg + (1+networkCoeff) * avgRemoteDeg);
                            }       
                                                            
                    }
                    queryCostHolder[0] = totalCost;
            }
//          //###########################forward cost#######################################################
//          //###########################reverse cost#######################################################
            {
                    Double totalCost = new Double(0);
                    Double prevScanCost = heuristics.numVertices;
                    {
                            
                            {
                                    Double probability = null;
                                    propertyName=reachQuery.getSinkPropertyName();
                                    propertyValue=reachQuery.getSinkPropertyValue();
                                    if ( heuristics.vertexPredicateMap.get(propertyName).containsKey(propertyValue.toString()) )
                                                    probability = heuristics.vertexPredicateMap.get(propertyName).get(propertyValue.toString()).probability;
                                    else {
                                                    totalCost = new Double(-1);
                                    }
                                    
                                    Double avgDeg = null;
                                    Double avgRemoteDeg = null;
//                                  System.out.println("Property:" +currentStep.property);
//                                  System.out.println("Value:" + currentStep.value);
                                    avgDeg = heuristics.vertexPredicateMap.get(propertyName).get(propertyValue.toString()).avgInDegree; 
                                    avgRemoteDeg = heuristics.vertexPredicateMap.get(propertyName).get(propertyValue.toString()).avgRemoteInDegree;    
                                    if (totalCost!=-1)
                                            totalCost = prevScanCost * probability *  ( avgDeg + (1+networkCoeff) * avgRemoteDeg);
                            }       
                                                            
                    }
                    queryCostHolder[1] = totalCost;
            }
//          //###########################reverse cost#######################################################
//  }               
//  //###################################COMPUTE HUERISTIC BASED QUERY COST#################################        

    
    if(queryCostHolder[0] < queryCostHolder [1])
      return 0;
    else 
      return 1;
  }

  @Override
  public Double getCost(int startPoint) {
    // TODO Auto-generated method stub
    return queryCostHolder[startPoint];
  }
  

}
