package in.dream_lab.goffish.godb.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;


public class DataWriter extends DataOutputStream {

	private ByteArrayOutputStream baos;

	private DataWriter(ByteArrayOutputStream out) {
		super(out);
		baos = out;
	}

	public byte[] getBytes() {
		return baos.toByteArray();
	}

	public static DataWriter newInstance() {
		return newInstance(1024);
	}
	
	public static DataWriter newInstance(int capacity) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(capacity);
		return new DataWriter(baos);
	}
}