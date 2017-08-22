package in.dream_lab.goffish.godb;

/**
 * Class for storing the traversal path V->E->V->E->E.....
 */
public class Step{
	public String property;
	public Object value;
	public Direction direction;
	public Type type;
	public enum Type{EDGE,VERTEX}
	public enum Direction{OUT,IN}
	
	//used in reachability query
	public Step(String p,Object v){
		this.property = p;
		this.value = v;
	}
	
	//used in path query
	public Step(Type t,Direction d,String p,Object v){
		this.type = t;
		this.direction = d;
		this.property = p;
		this.value = v;
	}

}
