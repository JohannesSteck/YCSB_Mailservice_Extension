package com.yahoo.ycsb.workloads;

import java.io.IOException;
import java.util.Properties;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.KBLogNormalIntegerGenerator;
import com.yahoo.ycsb.generator.LogNormalIntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;

public abstract class MailAppWorkload extends Workload{

	@Override
	public abstract boolean doInsert(DB db, Object threadstate);



		/**
		 * The name of the database table to run queries against.
		 */
		public static final String MAILBOXTABLENAME_PROPERTY="mailboxtablename";
		public static final String COUNTERTABLENAME_PROPERTY="countertablename";
		public static final String SIZETABLENAME_PROPERTY="sizetablename";
		

		//Tablenames. DB must be configured with the same names
		public static final String MAILBOXTABLENAME_PROPERTY_DEFAULT="mailboxtable";
		public static final String COUNTERTABLENAME_PROPERTY_DEFAULT="countertable";
		public static final String SIZETABLENAME_PROPERTY_DEFAULT="countertable";
		
		public static String mailboxtable;
		public static String countertable;
		public static String sizetable;
		

		


		//constant, uniform or zipfian
		public static final String MESSAGECOUNT_DISTRIBUTION_PROPERTY="messagecountdistribution";
		public static final String MESSAGECOUNT_DISTRIBUTION_PROPERTY_DEFAULT="lognormal";	
		public static String messageCountDistribution;
		
		//sets max initial Message count per Mailbox. More Messages may be added during transaction phase!
		public static final String MAXMESSAGECOUNT_PROPERTY="maxmessagecount";
		public static final String MAXMESSAGECOUNT_PROPERTY_DEFAULT="100";
		//sets min initial Message count per Mailbox
		public static final String MINMESSAGECOUNT_PROPERTY="minmessagecount";
		public static final String MINMESSAGECOUNT_PROPERTY_DEFAULT="1";
		
		public static final String MESSAGECOUNT_LOGNORMAL_SIGMA_PROPERTY="messagecount_lognormal_sigma";
		public static final String MESSAGECOUNT_LOGNORMAL_SIGMA_PROPERTY_DEFAULT="1.0";
		public static double messagecount_lognormal_sigma;
		
		public static final String MESSAGECOUNT_LOGNORMAL_MEAN_PROPERTY="messagecount_lognormal_mean";
		public static final String MESSAGECOUNT_LOGNORMAL_MEAN_PROPERTY_DEFAULT="2.135";		
		public static double messagecount_lognormal_mean;
		
		IntegerGenerator messageCountGenerator;
		
		
		public static final String MESSAGESIZE_DISTRIBUTION_PROPERTY="messagesizedistribution";
		//constant uniform zipfian or lognormal
		public static final String MESSAGESIZE_DISTRIBUTION_PROPERTY_DEFAULT="lognormal";
		
		public static final String MESSAGESIZE_LOGNORMAL_SIGMA_PROPERTY="messagesize_lognormal_sigma";
		public static final String MESSAGESIZE_LOGNORMAL_SIGMA_PROPERTY_DEFAULT="0.739";
		public static double messagesize_lognormal_sigma;
		
		public static final String MESSAGESIZE_LOGNORMAL_MEAN_PROPERTY="messagesize_lognormal_mean";
		public static final String MESSAGESIZE_LOGNORMAL_MEAN_PROPERTY_DEFAULT="0.87";		
		public static double messagesize_lognormal_mean;
		
		public static String messageSizeDistribution;
		
		//max Bytes per Message (whole Message with header)
		public static final String MAXMESSAGESIZE_PROPERTY="maxmessagesize";
		public static final String MAXMESSAGESIZE_PROPERTY_DEFAULT="63000";	
		
		//min Bytes per Message (whole Message with header)
		public static final String MINMESSAGESIZE_PROPERTY="minmessagesize";
		public static final String MINMESSAGESIZE_PROPERTY_DEFAULT="1000";	
		
		static int maxMessageCount;
		static int minMessageCount;
		
		IntegerGenerator messageSizegenerator;

		public static final String POP_DELETEALL_PROPORTION_PROPERTY="popdeleteallproportion";
		public static final String POP_DELETEALL_PROPORTION_PROPERTY_DEFAULT="0.05";
		
		public static final String POP_DELETE_PROPORTION_PROPERTY="popdeleteproportion";
		public static final String POP_DELETE_PROPORTION_PROPERTY_DEFAULT="0.10";
		
		public static final String POP_EMPTY_PROPORTION_PROPERTY="popemptyproportion";
		public static final String POP_EMPTY_PROPORTION_PROPERTY_DEFAULT="0.60";
		
		public static final String POP_PROPORTION_PROPERTY="popproportion";
		public static final String POP_PROPORTION_PROPERTY_DEFAULT="0.81";
		
		public static final String SMTP_PROPORTION_PROPERTY="smtpproportion";
		public static final String SMTP_PROPORTION_PROPERTY_DEFAULT="0.85";
		
		IntegerGenerator keysequence;

		DiscreteGenerator operationchooser;

		IntegerGenerator keychooser;

		Generator fieldchooser;

		CounterGenerator transactioninsertkeysequence;
		
		IntegerGenerator scanlength;
		
		int usercount;
		
		//counter Support
		static String incrementTag;
		static String decrementTag;
		
		public static String messagecountfieldkey="messagecount";
		public static String mailboxsizefieldkey="mailboxsize";

		String keyDelimiter = "#";
		String inboxSuffix = "inbox";
		String outboxSuffix ="outbox";
		
		/**
		 * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
		 */
		public static final String REQUEST_DISTRIBUTION_PROPERTY="requestdistribution";
		
		/**
		 * The default distribution of requests across the keyspace
		 */
		public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT="uniform";

		/**
		 * The name of the property for the order to insert records. Options are "ordered" or "hashed"
		 */
		public static final String INSERT_ORDER_PROPERTY="insertorder";
		
		/**
		 * Default insert order.
		 */
		public static final String INSERT_ORDER_PROPERTY_DEFAULT="hashed";
		
		boolean orderedinserts;

		/**
		   * Percentage data items that constitute the hot set.
		   */
		  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
		  
		  /**
		   * Default value of the size of the hot set.
		   */
		  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";
		  
		  /**
		   * Percentage operations that access the hot set.
		   */
		  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
		  
		  /**
		   * Default value of the percentage operations accessing the hot set.
		   */
		  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";
		
		  /**
			 * The name of a property that specifies the filename containing the message count histogram.
			 */
			public static final String MESSAGECOUNT_HISTOGRAM_FILE_PROPERTY = "messagecounthistogram";
			/**
			 * The default filename containing a message count histogram.
			 */
			public static final String MESSAGECOUNT_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";


			/**
			 * The name of a property that specifies the filename containing the message size histogram.
			 */
			public static final String MESSAGESIZE_HISTOGRAM_FILE_PROPERTY = "messagesizehistogram";
			/**
			 * The default filename containing a message size histogram.
			 */
			public static final String MESSAGESIZE_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";


			//
			public static String messageRetrieveCountDistribution;
			public static final String MESSAGERETRIEVECOUNTDISTRIBUTION_PROPERTY = "messageretrievecountdistribution";
			public static final String MESSAGERETRIEVECOUNTDISTRIBUTION_PROPERTY_DEFAULT = "uniform";
			
			public static String messageDeleteCountDistribution;
			public static final String MESSAGEDELETECOUNTDISTRIBUTION_PROPERTY = "messagedeletecountdistribution";
			public static final String MESSAGEDELETECOUNTDISTRIBUTION_PROPERTY_DEFAULT = "uniform";
		/**
		 * Initialize the scenario. 
		 * Called once, in the main client thread, before any operations are started.
		 */
		public void init(Properties p) throws WorkloadException
		{
			mailboxtable = p.getProperty(MAILBOXTABLENAME_PROPERTY,MAILBOXTABLENAME_PROPERTY_DEFAULT);
			countertable = p.getProperty(COUNTERTABLENAME_PROPERTY,COUNTERTABLENAME_PROPERTY_DEFAULT);
			sizetable = p.getProperty(SIZETABLENAME_PROPERTY,SIZETABLENAME_PROPERTY_DEFAULT);
			
			
			double popDeleteProportion=Double.valueOf(p.getProperty(POP_DELETE_PROPORTION_PROPERTY,POP_DELETE_PROPORTION_PROPERTY_DEFAULT));
			double popDeleteAllProportion=Double.valueOf(p.getProperty(POP_DELETEALL_PROPORTION_PROPERTY,POP_DELETEALL_PROPORTION_PROPERTY_DEFAULT));
			double popEmptyProportion=Double.valueOf(p.getProperty(POP_EMPTY_PROPORTION_PROPERTY,POP_EMPTY_PROPORTION_PROPERTY_DEFAULT));
			
			double popProportion=Double.valueOf(p.getProperty(POP_PROPORTION_PROPERTY,POP_PROPORTION_PROPERTY_DEFAULT));
			double smtpProportion=Double.valueOf(p.getProperty(SMTP_PROPORTION_PROPERTY,SMTP_PROPORTION_PROPERTY_DEFAULT));
			
			messageSizegenerator = getMessageSizeGenerator(p);
			messageCountGenerator = getMessageCountGenerator(p);
			
			messageRetrieveCountDistribution = p.getProperty(MESSAGERETRIEVECOUNTDISTRIBUTION_PROPERTY, MESSAGERETRIEVECOUNTDISTRIBUTION_PROPERTY_DEFAULT);
			messageDeleteCountDistribution = p.getProperty(MESSAGEDELETECOUNTDISTRIBUTION_PROPERTY, MESSAGEDELETECOUNTDISTRIBUTION_PROPERTY_DEFAULT);
			
			
			
	        this.incrementTag = p.getProperty("incrementtag","#inc#");
	        this.decrementTag = p.getProperty("decrementtag","#dec#");
			
			
			operationchooser=new DiscreteGenerator();
			if (popDeleteProportion>0)
			{
				operationchooser.addValue(popDeleteProportion,"POP_DELETE");
			}
			
			if (popDeleteAllProportion>0)
			{
				operationchooser.addValue(popDeleteAllProportion,"POP_DELETEALL");
			}
			
			if (popEmptyProportion>0)
			{
				operationchooser.addValue(popEmptyProportion,"POP_EMPTY");
			}
			
			if (popProportion>0)
			{
				operationchooser.addValue(popProportion,"POP");
			}

			if (smtpProportion>0)
			{
				operationchooser.addValue(smtpProportion,"SMTP");
			}

			usercount=Integer.valueOf(p.getProperty(Client.RECORD_COUNT_PROPERTY));
			
			transactioninsertkeysequence=new CounterGenerator(usercount);
			
			int insertstart=Integer.valueOf(p.getProperty(INSERT_START_PROPERTY,INSERT_START_PROPERTY_DEFAULT));
			
			String requestdistrib=p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
			
			if (p.getProperty(INSERT_ORDER_PROPERTY,INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed")==0)
			{
				orderedinserts=false;
			}
			else if (requestdistrib.compareTo("exponential")==0)
			{
	                    double percentile = Double.valueOf(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
	                                                                         ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
	                    double frac       = Double.valueOf(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
	                                                                         ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
	                    keychooser = new ExponentialGenerator(percentile, usercount*frac);
			}
			else
			{
				orderedinserts=true;
			}

			keysequence=new CounterGenerator(insertstart);
			
			
			if (requestdistrib.compareTo("uniform")==0)
			{
				keychooser=new UniformIntegerGenerator(0,usercount-1);
			}
			else if (requestdistrib.compareTo("zipfian")==0)
			{
				//it does this by generating a random "next key" in part by taking the modulus over the number of keys
				//if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
				//so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
				//of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
				//plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
				//just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator
				
				int opcount=Integer.valueOf(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
				int expectednewkeys=(int)(((double)opcount)*smtpProportion*2.0); //2 is fudge factor
				
				keychooser=new ScrambledZipfianGenerator(usercount+expectednewkeys);
			}
			else if (requestdistrib.compareTo("latest")==0)
			{
				keychooser=new SkewedLatestGenerator(transactioninsertkeysequence);
			}
			 else if (requestdistrib.equals("hotspot")) 
			{
	      double hotsetfraction = Double.valueOf(p.getProperty(
	          HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
	      double hotopnfraction = Double.valueOf(p.getProperty(
	          HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
	      keychooser = new HotspotIntegerGenerator(0, usercount - 1, 
	          hotsetfraction, hotopnfraction);
	    }
			else
			{
				throw new WorkloadException("Unknown request distribution \""+requestdistrib+"\"");
			}
			
		}

		protected static IntegerGenerator getMessageCountGenerator(Properties p) throws WorkloadException{
			IntegerGenerator messageCountGenerator;
			String messageCountDistribution = p.getProperty(MESSAGECOUNT_DISTRIBUTION_PROPERTY, MESSAGECOUNT_DISTRIBUTION_PROPERTY_DEFAULT);
			maxMessageCount=Integer.valueOf(p.getProperty(MAXMESSAGECOUNT_PROPERTY,MAXMESSAGECOUNT_PROPERTY_DEFAULT));
			minMessageCount=Integer.valueOf(p.getProperty(MINMESSAGECOUNT_PROPERTY,MINMESSAGECOUNT_PROPERTY_DEFAULT));
			String messagecounthistogram = p.getProperty(MESSAGECOUNT_HISTOGRAM_FILE_PROPERTY, MESSAGECOUNT_HISTOGRAM_FILE_PROPERTY_DEFAULT);
			if(messageCountDistribution.compareTo("lognormal") == 0) {
				messagecount_lognormal_mean=Double.valueOf(p.getProperty(MESSAGECOUNT_LOGNORMAL_MEAN_PROPERTY,MESSAGECOUNT_LOGNORMAL_MEAN_PROPERTY_DEFAULT));
				messagecount_lognormal_sigma=Double.valueOf(p.getProperty(MESSAGECOUNT_LOGNORMAL_SIGMA_PROPERTY,MESSAGECOUNT_LOGNORMAL_SIGMA_PROPERTY_DEFAULT));
				messageCountGenerator = new LogNormalIntegerGenerator(minMessageCount, maxMessageCount,messagecount_lognormal_mean,messagecount_lognormal_sigma );
			} else if(messageCountDistribution.compareTo("constant") == 0) {
				messageCountGenerator = new ConstantIntegerGenerator(maxMessageCount);
			} else if(messageCountDistribution.compareTo("uniform") == 0) {
				messageCountGenerator = new UniformIntegerGenerator(minMessageCount, maxMessageCount);
			} else if(messageCountDistribution.compareTo("zipfian") == 0) {
				messageCountGenerator = new ZipfianGenerator(minMessageCount, maxMessageCount);
			}
			else if(messageSizeDistribution.compareTo("histogram") == 0) {
				try {
					messageCountGenerator = new HistogramGenerator(messagecounthistogram);
				} catch(IOException e) {
					throw new WorkloadException("Couldn't read message count histogram file: "+messagecounthistogram, e);
				}
			} else {
				throw new WorkloadException("Unknown message count distribution \""+messageCountDistribution+"\"");
			}
			return messageCountGenerator;
		}
		protected static IntegerGenerator getMessageSizeGenerator(Properties p) throws WorkloadException{
			IntegerGenerator messageSizeGenerator;
			String messageSizeDistribution = p.getProperty(MESSAGESIZE_DISTRIBUTION_PROPERTY, MESSAGESIZE_DISTRIBUTION_PROPERTY_DEFAULT);
			int maxMessageSize=Integer.valueOf(p.getProperty(MAXMESSAGESIZE_PROPERTY,MAXMESSAGESIZE_PROPERTY_DEFAULT));
			int minMessageSize=Integer.valueOf(p.getProperty(MINMESSAGESIZE_PROPERTY,MINMESSAGESIZE_PROPERTY_DEFAULT));
			String messagesizehistogram = p.getProperty(MESSAGESIZE_HISTOGRAM_FILE_PROPERTY, MESSAGESIZE_HISTOGRAM_FILE_PROPERTY_DEFAULT);
			if(messageSizeDistribution.compareTo("lognormal") == 0) {
				messagesize_lognormal_mean=Double.valueOf(p.getProperty(MESSAGESIZE_LOGNORMAL_MEAN_PROPERTY,MESSAGESIZE_LOGNORMAL_MEAN_PROPERTY_DEFAULT));
				messagesize_lognormal_sigma=Double.valueOf(p.getProperty(MESSAGESIZE_LOGNORMAL_SIGMA_PROPERTY,MESSAGESIZE_LOGNORMAL_SIGMA_PROPERTY_DEFAULT));
				messageSizeGenerator = new KBLogNormalIntegerGenerator(Math.round(minMessageSize/1024),Math.round(maxMessageSize/1024),messagesize_lognormal_mean, messagesize_lognormal_sigma);
			} else if(messageSizeDistribution.compareTo("constant") == 0) {
				messageSizeGenerator = new ConstantIntegerGenerator(maxMessageSize);
			} else if(messageSizeDistribution.compareTo("uniform") == 0) {
				messageSizeGenerator = new UniformIntegerGenerator(minMessageSize, maxMessageSize);
			} else if(messageSizeDistribution.compareTo("zipfian") == 0) {
				messageSizeGenerator = new ZipfianGenerator(minMessageSize, maxMessageSize);
			}
			else if(messageSizeDistribution.compareTo("histogram") == 0) {
				try {
					messageSizeGenerator = new HistogramGenerator(messagesizehistogram);
				} catch(IOException e) {
					throw new WorkloadException("Couldn't read message size histogram file: "+messagesizehistogram, e);
				}
			} else {
				throw new WorkloadException("Unknown message size distribution \""+messageSizeDistribution+"\"");
			}
			return messageSizeGenerator;
		}
		
		protected static IntegerGenerator getMessageRetrieveCountGenerator(int min, int max) throws WorkloadException{
			IntegerGenerator messageRetrieveCountGenerator;
			
			if(messageRetrieveCountDistribution.compareTo("uniform") == 0) {
				messageRetrieveCountGenerator = new UniformIntegerGenerator(min, max);
			} else if(messageRetrieveCountDistribution.compareTo("zipfian") == 0) {
				messageRetrieveCountGenerator = new ZipfianGenerator(min, max);
			}
			else {
				throw new WorkloadException("Unknown messageRetrieveCountDistribution \""+messageRetrieveCountDistribution+"\"");
			}
			return messageRetrieveCountGenerator;
		}
		
		protected static IntegerGenerator getMessageDeleteCountGenerator(int min, int max) throws WorkloadException{
			IntegerGenerator messageDeleteCountGenerator;
			
			if(messageDeleteCountDistribution.compareTo("uniform") == 0) {
				messageDeleteCountGenerator = new UniformIntegerGenerator(min, max);
			} else if(messageDeleteCountDistribution.compareTo("zipfian") == 0) {
				messageDeleteCountGenerator = new ZipfianGenerator(min, max);
			}
			else {
				throw new WorkloadException("Unknown messageDeleteCountDistribution \""+messageDeleteCountDistribution+"\"");
			}
			return messageDeleteCountGenerator;
		}
	
		
		/**
		 * Do one transaction operation. Because it will be called concurrently from multiple client threads, this 
		 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
		 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
		 * effects other than DB operations.
		 */
		public boolean doTransaction(DB db, Object threadstate)
		{
			String op=operationchooser.nextString();

			/*if (op.compareTo("POP_READONLY")==0)
			{
			
				try {
					doTransactionPopReadOnly(db);
				} catch (WorkloadException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else if (op.compareTo("POP_DELETEALL")==0)
			{
				doTransactionPopReadDeleteAll(db);
			}else*/ if (op.compareTo("POP_EMPTY")==0)
			{
				doTransactionPopEmpty(db);
			}else if (op.compareTo("POP")==0)
			{
				try {
					doTransactionPop(db);
				} catch (WorkloadException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else if (op.compareTo("POP_DELETE")==0)
			{
				try {
					doTransactionPopDelete(db);
				} catch (WorkloadException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else if (op.compareTo("SMTP")==0)
			{
				doTransactionSMTP(db);
			}
			
			return true;
		}

		//public abstract void doTransactionPopReadOnly(DB db) throws WorkloadException;
		//public abstract void doTransactionPopReadDeleteAll(DB db);
		public abstract void doTransactionPopEmpty(DB db);
		public abstract void doTransactionPopDelete(DB db)throws WorkloadException;
		public abstract void doTransactionPop(DB db) throws WorkloadException;
		public abstract void doTransactionSMTP(DB db);
}
