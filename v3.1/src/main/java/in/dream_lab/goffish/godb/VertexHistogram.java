package in.dream_lab.goffish.godb;

import java.io.Serializable;

public class VertexHistogram implements Serializable {

	
	private static final long serialVersionUID = 1L;
	Histograms FrequencyHistogram;
	Histograms AvgOutDegHistogram;
	Histograms AvgInDegHistogram;
	Histograms AvgRemoteOutDegHistogram;
	Histograms AvgRemoteInDegHistogram;
	
	public VertexHistogram(Histograms _freq,Histograms _avgOut,Histograms _avgIn,Histograms _avgRemOut,Histograms _avgRemIn){
		FrequencyHistogram=_freq;
		AvgOutDegHistogram=_avgOut;
		AvgInDegHistogram=_avgIn;
		AvgRemoteOutDegHistogram=_avgRemOut;
		AvgRemoteInDegHistogram=_avgRemIn;
	}
}
