package in.dream_lab.goffish.godb;

import java.io.Serializable;

public abstract class predicateStats implements Serializable{
	private static final long serialVersionUID = 1L;
	public Double numberMatchingPredicate=new Double(0);
	public Double probability=new Double(0);
}