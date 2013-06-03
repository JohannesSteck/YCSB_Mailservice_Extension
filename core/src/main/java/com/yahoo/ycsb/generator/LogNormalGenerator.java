package com.yahoo.ycsb.generator;

import org.apache.commons.math3.distribution.LogNormalDistribution;

public class LogNormalGenerator extends Generator{
//just for positive Values
	LogNormalDistribution lnd;
	double last;
	double min=-1;
	double max=-1;
	public LogNormalGenerator(double mean, double standardDeviation){
		lnd = new LogNormalDistribution(mean, standardDeviation);
	}
	public LogNormalGenerator(double minValue, double maxValue, double mean, double standardDeviation){
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
		return String.valueOf(nextDouble());
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

	public double nextDouble(){
		
		double sample;
		if(min>-1&&max>-1){			
			sample=lnd.sample();			
			if(sample<min){
				sample=min;
			}else if(sample>max){
				sample=max;
			}
		}else{
			sample=lnd.sample();
		}
		last=sample;
		return sample;
		
	}
	

	
}
