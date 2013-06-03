package com.yahoo.ycsb.generator;

import org.apache.commons.math3.distribution.LogNormalDistribution;

public class LogNormalIntegerGenerator extends IntegerGenerator {

	LogNormalDistribution lnd;
	int last;
	int min=-1;
	int max=-1;
	public LogNormalIntegerGenerator(double mean, double standardDeviation){
		lnd = new LogNormalDistribution(mean, standardDeviation);
	}
	public LogNormalIntegerGenerator(int minValue, int maxValue, double mean, double standardDeviation){
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
			
			int sample;
			if(min>-1&&max>-1){			
				sample=(int)Math.round(lnd.sample());			
				if(sample<min){
					sample=min;
				}else if(sample>max){
					sample=max;
				}
			}else{
				sample=(int)Math.round(lnd.sample());
			}
			last=sample;
			last=sample;
			return sample;
			
		}

	@Override
	public double mean() {
		//Not supported right now
		return lnd.getNumericalMean();
	}

	
}
