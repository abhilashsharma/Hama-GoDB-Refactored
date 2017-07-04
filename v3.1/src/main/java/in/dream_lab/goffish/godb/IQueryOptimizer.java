package in.dream_lab.goffish.godb;

public interface IQueryOptimizer {

  //create generalizable IQuery
  //returns Starting point with lowest cost
  Integer Optimize(Object query);
  
  //get QueryCost for a starting Point
  Double getCost(int StartPoint);
  
}
