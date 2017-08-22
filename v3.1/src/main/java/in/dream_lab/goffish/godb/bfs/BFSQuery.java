package in.dream_lab.goffish.godb.bfs;

import java.util.regex.Pattern;


public class BFSQuery {

	/**
	 * BFS query(with depth)
	 * Example "label:string[toyota___rav_4]:4//0//163"
	 * 
	 */
	public BFSQuery(String queryParam) {
		// Parse range of instances query//start//end (???) // TODO
		String[] tokens = queryParam.split(Pattern.quote("//"));

		// Parse predicate (name:type[value])
		String[] query = tokens[0].split(Pattern.quote(":"));
		String typeAndValue = query[1];
		int valStart = typeAndValue.indexOf("[");
		int valEnd = typeAndValue.indexOf("]");

		String type = typeAndValue.substring(0, valStart);
		propertyValue = null;
		switch (type) {
			case "string":
				propertyValue = typeAndValue.substring(valStart + 1, valEnd);
				break;
			case "float":
				propertyValue = Float.parseFloat(typeAndValue.substring(valStart + 1, valEnd));
				break;
			case "double":
				propertyValue = Double.parseDouble(typeAndValue.substring(valStart + 1, valEnd));
				break;
			case "int":
				propertyValue = Integer.parseInt(typeAndValue.substring(valStart + 1, valEnd));
				break;
			default:
				propertyValue = typeAndValue.substring(valStart + 1, valEnd);
		}

		depth = Short.parseShort(query[2]);
		propertyName = query[0];
	}

	public BFSQuery(String name_, Object value_, int depth_) {
		propertyName = name_;
		propertyValue = value_;
		depth = depth_;
	}

	private String propertyName;
	private Object propertyValue;
	private int depth;

	public String getPropertyName() {
		return propertyName;
	}

	public Object getPropertyValue() {
		return propertyValue;
	}

	public int getDepth() {
		return depth;
	}
}
