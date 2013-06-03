package com.yahoo.ycsb.generator;

import org.apache.commons.math3.distribution.LogNormalDistribution;

public class KBLogNormalIntegerGenerator extends IntegerGenerator {
//values are multiplied by 1024 to represent KB
	LogNormalDistribution lnd;
	int last;
	int min=-1;
	int max=-1;
	public KBLogNormalIntegerGenerator(double mean, double standardDeviation){
		lnd = new LogNormalDistribution(mean, standardDeviation);
	}
	public KBLogNormalIntegerGenerator(int minValue, int maxValue, double mean, double standardDeviation){
		lnd = new LogNormalDistribution(mean, standardDeviation);
		if(minValue<maxValue){
		min=minValue;
		max=maxValue;
		}else{
			System.out.println("min value ("+minValue+") > max value ("+maxValue+")! Ignoring min and max.");
		}
	}
	
	@Override
	public String nextString() {
		// TODO Auto-generated method stub
		return String.valueOf(nextInt());
	}

	@Override
	public String lastString() {
		// TODO Auto-generated method stub
		return String.valueOf(lastDouble());
	}
	
	public double lastDouble() {
		// TODO Auto-generated method stub
		return last;
	}

	public int nextInt(){
		//long st = System.currentTimeMillis();
		int sample;
		if(min>-1&&max>-1){			
			sample=(int)Math.round(lnd.sample()*1024);			
			if(sample/1024<min){
				sample=min;
			}else if(sample/1024>max){
				sample=max;
			}
		}else{
			sample=(int)Math.round(lnd.sample()*1024);
		}
		last=sample;
		//long en = System.currentTimeMillis();
		//System.out.println("sampling time: "+((en-st))+" ms");
		return sample;
		
	}

	@Override
	public double mean() {
		//Not supported right now
		return lnd.getNumericalMean();
		
	}

	
}
