package in.dream_lab.goffish.godb;
import java.io.Serializable;
import java.util.Arrays;

//import cern.jet.stat.quantile.EquiDepthHistogram;

public class Histograms implements Serializable,IHistogram {
	private static final long serialVersionUID = 1L;
	/*these will calculated according to application logic and passed on to this class, for example equidepth histograms,
	quantile elements will be chosen so that frequencies per bin is approximately constant*/
	Object[] quantileElements;
	Double[] number_per_bin;
	Double[] frequency_per_bin;
	long Total;
	public Histograms(Object[] _quantileElements,Double[] _number_per_bin,Double[] _frequency_per_bin,long _Total){
		this.quantileElements=_quantileElements;
		this.number_per_bin=_number_per_bin;
		this.frequency_per_bin=_frequency_per_bin;
		this.Total=_Total;
	}
	
	
//public static void main(String... args){
////	Object[] quantileElements = 
////		  {-1f, 3.0f, 6.0f, 7.0f, 13.0f,14.0f};
//	Object[] quantileElements = 
//		  {"0","d","g","h","l","o"};
//
//	long[] number_per_bin = {4,3,1,6,1};
//	long[] frequency_per_bin={9,8,9,11,8};
//	long Total=45;
//	
//	Histograms H=new Histograms(quantileElements,number_per_bin,frequency_per_bin,Total);
//	
//	
//	
//    
//	String Element="1";
//	System.out.println(H.probability_of_element(Element));
//	
//	
//	
//		 
//}

@Override
public
int binOfElement(Object element){
	
	if(element.getClass()==Double.class){
	
		if(((double)element)< (double)quantileElements[0] || ((double)element) > (double)quantileElements[quantileElements.length-1]){
			return -1;
		}
	
			int retval= Arrays.binarySearch(quantileElements, (double)element);
			if(retval<0){
				return -(retval+1);
			}
			else
			return retval;
		
	}
	else//for strings
	{
		if(element.toString().compareTo(quantileElements[0].toString()) < 0 || element.toString().compareTo(quantileElements[quantileElements.length-1].toString()) >0){
			return -1;
		}
		
		int retval=Arrays.binarySearch(quantileElements, element.toString());
		if(retval<0){
			return -(retval+1);
		}
		else
		return retval;
	}
	
}

@Override
public
double probability_of_element(Object element){
	
	
	if(element.getClass()==Double.class)
	{
	
		int bin=binOfElement((double)element);
		if(bin>0)
			return (frequency_per_bin[bin-1]/number_per_bin[bin-1])/Total;
	}
	else{
		int bin=binOfElement(element.toString());
		if(bin>0)
			return (frequency_per_bin[bin-1]/number_per_bin[bin-1])/Total;
	}
	
	return -1;
}
	
}
