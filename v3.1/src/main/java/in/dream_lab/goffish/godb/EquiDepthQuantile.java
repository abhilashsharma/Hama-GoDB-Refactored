package in.dream_lab.goffish.godb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/*class for calculating equidepth histogram*/
public class EquiDepthQuantile {

	
	List<Object> quantileElements=new ArrayList<Object>();
	Double[] number_per_bin;
	Double[] frequency_per_bin;
	long Total;
	boolean exactFlag=false;
	public EquiDepthQuantile(int num_bins,Object[] orderedKeys,Double[] frequencies,long total){
		System.out.println("num_bins:" + num_bins + " Keys_count:" + orderedKeys.length + " Total:" + total);
		if(num_bins >= orderedKeys.length){
			num_bins=orderedKeys.length;
			exactFlag=true;
			System.out.println("Storing Exact Stats");
		}
		
		if(exactFlag){
			quantileElements.add("0");
			for(Object o: orderedKeys ){
				quantileElements.add(o);
			}
			
			number_per_bin=new Double[num_bins];
			frequency_per_bin=new Double[num_bins];
			for(int i=0;i<num_bins;i++){
				number_per_bin[i]=1d;
				frequency_per_bin[i]=frequencies[i];
			}
			
			this.Total=total;
		}
		else
		{
		number_per_bin=new Double[num_bins];
		frequency_per_bin=new Double[num_bins];
		System.out.println("STARTING");
		this.Total=total;
		long approxFrequenciesPerBin=total/num_bins;
		System.out.println("Approximate frequency per bin:" + approxFrequenciesPerBin);
		List<Object> quantileElementsList=new ArrayList<Object>();
		
		long traversed=0;
		int i=0;
		int currentKeyIndex=0;
		int lastindex=0;
//		while(traversed<total)
//		{
//			long currentBinFrequency=0;
//			long lastBinFrequency=0;
//			int count=0;
//			while(currentBinFrequency<approxFrequenciesPerBin){
//				if(currentKeyIndex == orderedKeys.length)
//					break;
//				lastBinFrequency=currentBinFrequency;
//				count++;
//				currentBinFrequency+=frequencies[currentKeyIndex];
//				currentKeyIndex++;
//				
//			}
//			currentKeyIndex--;
//			System.out.println("ADDING" + orderedKeys[currentKeyIndex]);
//			quantileElementsList.add(orderedKeys[currentKeyIndex]);
//			currentKeyIndex++;
//			traversed+=lastBinFrequency;
//			count--;
//			if(currentKeyIndex == orderedKeys.length)
//				break;
//			
//		}
//		
//		quantileElementsList.add(orderedKeys[orderedKeys.length-1] + 1);
//		float[] floatArray = new float[quantileElementsList.size()];
//		for(Object e:quantileElementsList){
//			this.quantileElements.add((float)e-1);
//		}
		
		quantileElementsList.add("0");
		
		while((currentKeyIndex+1)<orderedKeys.length){
			System.out.println("START INDEX:" + currentKeyIndex + " LAST INDEX:" + lastindex);
			Double lastElementBinFrequency=0d;
			Double currentBinFrequency=frequencies[currentKeyIndex];
			int count=0;
			while(currentBinFrequency <= approxFrequenciesPerBin  || count==0 ){
				if((currentKeyIndex+1)<orderedKeys.length){	
					System.out.println("CURRENT FREQ:" + frequencies[currentKeyIndex] +" BIN FREQ:" + currentBinFrequency + " CURRENT KEYINDEX:" + currentKeyIndex);
					lastElementBinFrequency=currentBinFrequency;
					lastindex=currentKeyIndex;
					
					currentBinFrequency+=frequencies[++currentKeyIndex];
					count++;
				}
				else break;
				
				
			}
			System.out.println("END:"+ currentKeyIndex + " LAST:" + lastindex);
			quantileElementsList.add(orderedKeys[--currentKeyIndex]);
			System.out.println("ADDING:" +orderedKeys[currentKeyIndex] + " COUNT:" + --count + " CURRENT BIN FREQ:" +currentBinFrequency + " LAST:" +lastElementBinFrequency );
			currentKeyIndex++;
			
			
		}
		
		quantileElementsList.remove(quantileElementsList.size()-1);
		quantileElementsList.add(orderedKeys[orderedKeys.length-1]);
		
		System.out.println("QUANTILE ELEMENTS:");
		
		for(Object e : quantileElementsList){
			System.out.println(e.toString());
			quantileElements.add(e);
		}
		
		
	
		int currentCount=0;
		int currentFrequency=0;
		for(int j=0;j<orderedKeys.length && i<num_bins;j++){
			currentFrequency+=frequencies[j];
			currentCount++;
			if(this.quantileElements.contains(orderedKeys[j])){
				number_per_bin[i]=(double)currentCount;
				frequency_per_bin[i]=(double)currentFrequency;
				i++;
				currentCount=0;
				currentFrequency=0;
			}
		}
		}//else ends for approximate histogram calculation
		
		
	}
	
	
	public Object[] quantileArray(){
		
		Object[] arr=new Object[this.quantileElements.size()];
		int i=0;
		for(Object e:this.quantileElements ){
			arr[i]=e;
			i++;
		}
		return arr;
	}
	
	public static void main(String[] args){
		
		int num_bins=5;
		Object[] orderedKeys={"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o"};
		Double[] frequencies={0d,0d,0d,0d,0d,0d,0d,0d,0d,0d,0d,0d,0d,0d,0d};
		long total=45;
		EquiDepthQuantile e=new EquiDepthQuantile(num_bins, orderedKeys, frequencies, total);
		
		System.out.println("Object Array:");
		for(Object o:e.quantileArray()){
			System.out.println(o);
		}
		
		System.out.println("ELEMENTS PER BIN:");
		for(double n:e.number_per_bin){
			System.out.println(n);
		}
		
		System.out.println("FREQUENCY PER BIN:");
		for(double n:e.frequency_per_bin){
			System.out.println(n);
		}
//		System.out.println(Arrays.binarySearch(quantile, "e"));
//		Object a1="b";
//		Object a2="a";
//		if(a1.getClass()==Integer.class){
//			System.out.println((Integer)a1 < (Integer)a2);
//		}
//		else if(a1.getClass()==String.class){
//			System.out.println(a1.toString().compareTo(a2.toString()));
//			
//		}
	}
	
}
