package in.dream_lab.goffish.godb;

import java.io.Serializable;

public class EdgeHistogram implements Serializable {
	
	private static final long serialVersionUID = 1L;
	Histograms FrequencyHistogram;
	
	public EdgeHistogram(Histograms _freq){
		FrequencyHistogram=_freq;
	}
	
}
