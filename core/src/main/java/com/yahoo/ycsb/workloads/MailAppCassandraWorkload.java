/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import java.util.Properties;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.math3.stat.regression.UpdatingMultipleLinearRegression;


/*Cassandra MaiApp Worload
 * 
 * required Poperties:
 * - Countertable and Mailboxtable names
 * - usercount(initial?)=mailboxcount(*2? inbox/outbox)
 * - Proportion of pop-actions and smtp-actions (per user -> which distrubution?)
 * - frequency Distribution of pop-actions -> requestdistribution for selecting a record
 * - frequency Distribution of smtp-actions -> requestdistribution for selecting a record
 * - Distribution of Messagecount per user -> represent different Mailboxsizes of Users -> zipfian, hotspot?
 * - Distribution of Messagesizes (global sufficient? some users may regularly write long messages)-> fieldlengthdistribution, max. ~64KB (no attachments because of DynamoDB limitations)
 * - Distribution of Message Deletion (all Messages, some Messages) 
 * - max Mailbox size as hard limit?
 * 
 * Map to different EMail clients (mobile, desktop)
 * pop_getWholeMailboxDeleteAll
 * pop_getWholeMailboxDeleteNone
 * pop_getWholeMailboxDeleteSome
 * pop_getPartMailboxDeleteAll
 * pop_getPartMailboxDeleteNone
 * pop_getPartMailboxDeleteSome
 * pop_getStat
 * smtp_storeMessage
 * 
 * 
 */



public class MailAppCassandraWorkload extends MailAppWorkload
{

	//"range key" support for deleting single columns
	private String hashAndRangeKeyDelimiter;

	//extra table for cassandra schema
	public static final String SIZETABLENAME_PROPERTY="cassandra.sizetablename";
	public static final String SIZETABLENAME_PROPERTY_DEFAULT="sizetable";
	public static String sizetable;
	
	public void init(Properties p) throws WorkloadException{
		super.init(p);
		hashAndRangeKeyDelimiter = p.getProperty("cassandra.rangekeydelimiter","_rangeKeyFollows_");
		sizetable = p.getProperty(SIZETABLENAME_PROPERTY,SIZETABLENAME_PROPERTY_DEFAULT);

		
	}
	
	public String buildKeyName(long keynum /* ~username */, String mailboxSuffix /* ~inbox or outbox */) {

		
 		if (!orderedinserts)
 		{
 			keynum=Utils.hash(keynum);
 		}
		return keynum+keyDelimiter+mailboxSuffix;
	}
	
	
	HashMap<String, ByteIterator> buildNewMessage() {
 		HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

  		
 			String fieldkey="field"+UUID.randomUUID();
 			ByteIterator data= new RandomByteIterator(messageSizegenerator.nextInt());
 			values.put(fieldkey,data);
 		
		return values;
	}
	
	HashMap<String, ByteIterator> buildValues() {
 		HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

 		//build as many "Messages" as Generator says
 		//cannot request specific fields later, because they might not exist on all records
 		int fieldcount = messageCountGenerator.nextInt();
 		
 		for (int i=0; i<fieldcount; i++)
 		{
 			String fieldkey="field"+i;
 			ByteIterator data= new RandomByteIterator(messageSizegenerator.nextInt());
 			values.put(fieldkey,data);
 		}
		return values;
	}

	ArrayList<HashMap<String, ByteIterator>> sizetable_buildSingleMessageValuesArray(String id) {
		//return equally sized copies in an array
		ArrayList<HashMap<String, ByteIterator>> valuesArray = new ArrayList<HashMap<String, ByteIterator>>();

 		HashMap<String,ByteIterator> values1=new HashMap<String,ByteIterator>();
 		HashMap<String,ByteIterator> values2=new HashMap<String,ByteIterator>();


 		//build as one message 		
 		
 		
 		

 			//add new unique field key (column name doesn't matter in this context as long as it is unique)
 			String fieldkey=id;
 			

 			int msgSize=messageSizegenerator.nextInt();

 			//only add size as value
 			StringByteIterator data1= new StringByteIterator(""+msgSize);
 			StringByteIterator data2= new StringByteIterator(""+msgSize);

 			values1.put(fieldkey,data1);
 			values2.put(fieldkey,data2);
 			 		
 		
 		valuesArray.add(values1);
 		valuesArray.add(values2);
		return valuesArray;
		
	}
	
	ArrayList<HashMap<String, ByteIterator>> buildSingleMessageValuesArray(String id) {
		//return equally sized copies in an array (because ByteIterators are consumed when read)
		ArrayList<HashMap<String, ByteIterator>> valuesArray = new ArrayList<HashMap<String, ByteIterator>>();

 		HashMap<String,ByteIterator> values1=new HashMap<String,ByteIterator>();
 		HashMap<String,ByteIterator> values2=new HashMap<String,ByteIterator>();


 		//build as one message 		
 		
 		
 		

 			//add new unique field key (column name doesn't matter in this context as long as it is unique)
 			String fieldkey=id;
 			
 			//long stSize=System.nanoTime();
 			int msgSize=messageSizegenerator.nextInt();
 			//long enSize=System.nanoTime();
 			//Measurements.getMeasurements().measure("generateMessageSIZE", (int)((enSize-stSize)/1000));
 			
 			//long stIter=System.nanoTime();
 			ByteIterator data1= new RandomByteIterator(msgSize);
 			ByteIterator data2= new RandomByteIterator(msgSize);
 			//long enIter=System.nanoTime();
 			//Measurements.getMeasurements().measure("BUILD2ITERATORS", (int)((enIter-stIter)/1000));
 			
 			values1.put(fieldkey,data1);
 			values2.put(fieldkey,data2);
 			 		
 		
 		valuesArray.add(values1);
 		valuesArray.add(values2);
		return valuesArray;
		
	}
	/*
	ArrayList<HashMap<String, ByteIterator>> buildValuesArray() {
		//return equally sized copies in an array (because ByteIterators are consumed when read)
		ArrayList<HashMap<String, ByteIterator>> valuesArray = new ArrayList<HashMap<String, ByteIterator>>();

 		HashMap<String,ByteIterator> values1=new HashMap<String,ByteIterator>();
 		HashMap<String,ByteIterator> values2=new HashMap<String,ByteIterator>();


 		//build as many "Messages" as Generator says
 		//cannot request specific fields later, because they might not exist on all records
 		//long st=System.nanoTime();
 		int fieldcount = messageCountGenerator.nextInt();
 		//long en=System.nanoTime();
 		//Measurements.getMeasurements().measure("generateMessageCOUNT", (int)((en-st)/1000));
 		//System.out.println("messagecount: "+fieldcount+" generated in "+(en-st)/1000);
 		for (int i=0; i<fieldcount; i++)
 		{
 			
 			String fieldkey="field"+i;
 			

 			int msgSize=messageSizegenerator.nextInt();

 			

 			ByteIterator data1= new RandomByteIterator(msgSize);
 			ByteIterator data2= new RandomByteIterator(msgSize);


 			
 			values1.put(fieldkey,data1);
 			values2.put(fieldkey,data2);
 			
 		}
 		//en=System.nanoTime();
 		//System.out.println("--messagecount: "+fieldcount+" took "+(en-st)/1000+" ms to buidl");
 		valuesArray.add(values1);
 		valuesArray.add(values2);
		return valuesArray;
		
	}
	*/
	HashMap<String, ByteIterator> buildCounterValues() {
 		HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();
			
 			//fill with pseudo message count
 			int messagecount = messageCountGenerator.nextInt();
 			ByteIterator messageCountData= new StringByteIterator(""+messagecount) ;
 			values.put(messagecountfieldkey,messageCountData);
 			
 			
 			//fill with pseudo message size based on count
 			ByteIterator messageSizeData= new StringByteIterator(""+messagecount*messageSizegenerator.nextInt());
 			values.put(mailboxsizefieldkey,messageSizeData);
 		
		return values;
	}
    private HashMap<String, ByteIterator> buildMessageCounterValues(long newValue) {
    	HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();
	
    	

		ByteIterator messageCountData;
		
		messageCountData= new StringByteIterator(String.valueOf(newValue)) ;

			values.put(messagecountfieldkey,messageCountData);
			

	return values;
	}
    private HashMap<String, ByteIterator> buildMailboxsizeCounterValues(long newValue) {
    	HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();
	
    	

		ByteIterator messageCountData;
		
		messageCountData= new StringByteIterator(String.valueOf(newValue)) ;

			values.put(mailboxsizefieldkey,messageCountData);
			

	return values;
	}

	
	/**
	 * Do one insert operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
	 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
	 * effects other than DB operations.
	 */
	
	public boolean doInsert(DB db, Object threadstate)
	//Befüllen der Tabelle in der Loadphase (je 1 record pro Aufruf)
	{
		/*
		int keynum=keysequence.nextInt();
		String inboxdbkey = buildKeyName(keynum, inboxSuffix);
		String outboxdbkey = buildKeyName(keynum, outboxSuffix);

		//System.out.println("generating values for key: "+inboxdbkey);
		//Get two similar ByteIterators with same length
		//long stV=System.nanoTime();
		ArrayList<HashMap<String, ByteIterator>> valueList = buildValuesArray();
		//long enV=System.nanoTime();
		//Measurements.getMeasurements().measure("buildingValues", (int)((enV-stV)/1000));
		
		HashMap<String, ByteIterator> valuesInbox = valueList.get(0);
		HashMap<String, ByteIterator> valuesOutbox = valueList.get(1);
		
		//System.out.println("statring with doInsert: key:"+inboxdbkey);
		//long st=System.nanoTime();
		
		boolean insertInbox = db.insert(mailboxtable,inboxdbkey,valuesInbox) == 0 ;
		boolean updateCounterTableInbox = db.update(countertable, inboxdbkey, buildCounterValues()) == 0;
		boolean insertOutbox = db.insert(mailboxtable,outboxdbkey,valuesOutbox) == 0 ;
		boolean updateCounterTableOutbox = db.update(countertable, outboxdbkey, buildCounterValues()) == 0;
	
		//long en=System.nanoTime();
		//Measurements.getMeasurements().measure("loadMailbox", (int)((en-st)/1000));
		
		if (insertInbox && insertOutbox && updateCounterTableInbox && updateCounterTableOutbox)
			return true;
		else
		*/
			return false;
			
	}



    public void doTransactionPopReadOnly(DB db) throws WorkloadException{
    	//choose a random key
		int keynum = nextKeynum();
		
		String keynameInbox=buildKeyName(keynum, inboxSuffix);

		HashSet<String> fields=null;
		
		HashSet<String> counterColumnNames =new HashSet<String>();
		counterColumnNames.add(messagecountfieldkey);
		counterColumnNames.add(mailboxsizefieldkey);
		
		HashMap<String, ByteIterator> counterResult = new HashMap<String,ByteIterator>();
		HashMap<String, ByteIterator> uidlResult = new HashMap<String,ByteIterator>();
		HashMap<String, ByteIterator> result = new HashMap<String,ByteIterator>();
		
		HashSet<String> retrievedMessageIDList = new HashSet<String>();
		long st=System.nanoTime();
		
		//read counter Table (POP3 STAT Command)
		db.read(incrementTag+countertable,keynameInbox,counterColumnNames,counterResult);
		if(counterResult.isEmpty()){
			long en=System.nanoTime();
			Measurements.getMeasurements().measure("POP_ReadOnly-Transaction", (int)((en-st)/1000));
			return;
		}
		//get UIDL/List-List
		db.read(sizetable,keynameInbox,fields,uidlResult);
		//Retr x Messages
		int retrieveCount = getMessageRetrieveCountGenerator(0, uidlResult.size()).nextInt();
		for(int i=0;i<retrieveCount;i++){
			//request one message by key and single column name
			String columnName = uidlResult.entrySet().iterator().next().getKey();
			HashSet<String> requestColumn = new HashSet<String>();
			requestColumn.add(columnName);
			db.read(mailboxtable, keynameInbox,requestColumn , result);
			retrievedMessageIDList.add(columnName);
		}
		long en=System.nanoTime();
		Measurements.getMeasurements().measure("POP_ReadOnly-Transaction", (int)((en-st)/1000));
    	    	
    }
    /* Not used anymore
    public void doTransactionPopReadDeleteAll(DB db){
    	//choose a random key
		int keynum = nextKeynum();
		
		String keynameInbox=buildKeyName(keynum, inboxSuffix);

		HashSet<String> fields=null;
		
		HashSet<String> counterColumnNames =new HashSet<String>();
		counterColumnNames.add(messagecountfieldkey);
		counterColumnNames.add(mailboxsizefieldkey);
		
		HashMap<String, ByteIterator> counterResult = new HashMap<String,ByteIterator>();
		HashMap<String, ByteIterator> uidlResult = new HashMap<String,ByteIterator>();
		HashMap<String, ByteIterator> result = new HashMap<String,ByteIterator>();
		
		HashSet<String> retrievedMessageIDList = new HashSet<String>();
		long st=System.nanoTime();
		
		//read counter Table (POP3 STAT Command)
		db.read(incrementTag+countertable,keynameInbox,counterColumnNames,counterResult);
		if(counterResult.isEmpty()){
			long en=System.nanoTime();
			Measurements.getMeasurements().measure("POP_ReadDeleteAll-Transaction", (int)((en-st)/1000));
			return;
		}
		//get UIDL/List-List
		db.read(sizetable,keynameInbox,fields,uidlResult);
		//Retr all Messages
		int retrieveCount = uidlResult.size();
		Iterator<Entry<String, ByteIterator>> uidlResultIterator = uidlResult.entrySet().iterator();
		for(int i=0;i<retrieveCount;i++){
			//request one message by key and single column name
			String columnName = uidlResultIterator.next().getKey();
			HashSet<String> requestColumn = new HashSet<String>();
			requestColumn.add(columnName);
			db.read(mailboxtable, keynameInbox,requestColumn , result);
			retrievedMessageIDList.add(columnName);
		}

		//delete all Messages. DB.delete() only allows deletion of whole rows, so  columnNames are appended with delimiter
		if(retrievedMessageIDList.size()>0){
			int deleteCount= uidlResult.size();
			Iterator<String> retrievedIdsIterator = retrievedMessageIDList.iterator();
			 for(int i=0;i<deleteCount;i++){
				 
				 String columnNameDelete=hashAndRangeKeyDelimiter+retrievedIdsIterator.next();
				 //System.out.println("deleting column: "+columnNameDelete);
				 db.delete(mailboxtable, keynameInbox+columnNameDelete);
				 db.delete(sizetable, keynameInbox+columnNameDelete);
			 }
			 if(deleteCount>0){
				//Update Counter
				 HashMap<String, ByteIterator> countUpdateValue=  new HashMap<String, ByteIterator>();
				 countUpdateValue.put(messagecountfieldkey, new StringByteIterator("-"+deleteCount));
				 countUpdateValue.put(mailboxsizefieldkey, new StringByteIterator("-"+deleteCount*100));
				 
				 //indicate use of counterColumns with incremetTag in from of tablename
			 db.update(incrementTag+countertable,keynameInbox,countUpdateValue);
			 }
		}
		
		long en=System.nanoTime();
		Measurements.getMeasurements().measure("POP_ReadDeleteAll-Transaction", (int)((en-st)/1000));
    	    	
    }
    */
    public void doTransactionPopEmpty(DB db){
    	printDebug("------------POP Empty Transaction-------------");
    	//just do a pop3 stat
    	//choose a random key
		int keynum = nextKeynum();
		
		String keynameInbox=buildKeyName(keynum, inboxSuffix);

		HashSet<String> fields=null;
		

		HashSet<String> counterColumnNames =new HashSet<String>();
		counterColumnNames.add(messagecountfieldkey);
		counterColumnNames.add(mailboxsizefieldkey);
		
		HashMap<String, ByteIterator> counterResult = new HashMap<String,ByteIterator>();
		HashMap<String, ByteIterator> uidlResult = new HashMap<String,ByteIterator>();

		long st=System.nanoTime();
		
		//read counter Table (POP3 STAT Command)
		db.read(incrementTag+countertable,keynameInbox,counterColumnNames,counterResult);
		int statCount = Integer.valueOf(counterResult.get(messagecountfieldkey).toString());
		if(statCount<=0){
			
			long en=System.nanoTime();
			printDebug("POP Empty: Inbox empty, closing...");
			Measurements.getMeasurements().measure("POP_EMPTY-Transaction", (int)((en-st)/1000));
			return;
		}
		//get UIDL/List-List
		printDebug("POP Empty: reading uidl-List");
		db.read(sizetable,keynameInbox,fields,uidlResult);
		
				
		long en=System.nanoTime();
		Measurements.getMeasurements().measure("POP_EMPTY-Transaction", (int)((en-st)/1000));
    	    	
    }
    
    public void doTransactionPop(DB db) throws WorkloadException{
    	printDebug("------------POP Read Transaction-------------");
    	//choose a random key
		int keynum = nextKeynum();
		
		String keynameInbox=buildKeyName(keynum, inboxSuffix);

		HashSet<String> fields=null;
		
		HashSet<String> counterColumnNames =new HashSet<String>();
		counterColumnNames.add(messagecountfieldkey);
		counterColumnNames.add(mailboxsizefieldkey);
		
		HashMap<String, ByteIterator> counterResult = new HashMap<String,ByteIterator>();
		HashMap<String, ByteIterator> uidlResult = new HashMap<String,ByteIterator>();
		HashMap<String, ByteIterator> result = new HashMap<String,ByteIterator>();
		
		HashSet<String> retrievedMessageIDList = new HashSet<String>();

		
		long st=System.nanoTime();
		
		//read counter Table (POP3 STAT Command)
		db.read(incrementTag+countertable,keynameInbox,counterColumnNames,counterResult);
		int statCount = Integer.valueOf(counterResult.get(messagecountfieldkey).toString());
		
		if(statCount<=0){
			
			long en=System.nanoTime();
			printDebug("POP Read: Inbox empty, closing...");
			Measurements.getMeasurements().measure("POP-Transaction", (int)((en-st)/1000));
			return;
		}
		//get UIDL/List-List
		printDebug("POP Read: reading uidl-List");
		db.read(sizetable,keynameInbox,fields,uidlResult);
		//Retr x Messages
		printDebug("POP Read: setting up retrieve count generator...");
		int retrieveCount = getMessageRetrieveCountGenerator(1, uidlResult.size()).nextInt();
		Iterator<Entry<String, ByteIterator>> uidlResultIterator = uidlResult.entrySet().iterator();
		printDebug("POP Read: reading "+retrieveCount+" messages. #messages in Inbox: "+uidlResult.size());
		for(int i=0;i<retrieveCount;i++){
			
			//request one message by key and single column name
			String columnName = uidlResultIterator.next().getKey();
			printDebug("POP Read: requesting column with name: "+columnName);
			HashSet<String> requestColumn = new HashSet<String>();
			requestColumn.add(columnName);
			db.read(mailboxtable, keynameInbox,requestColumn , result);
			printDebug("POP Read: retrieved: "+columnName+" row: "+keynameInbox+" result #columns: "+result.size());
			retrievedMessageIDList.add(columnName);
		}
		
		//---
		
		long en=System.nanoTime();
		Measurements.getMeasurements().measure("POP-Transaction", (int)((en-st)/1000));
    	    	
    }
    
    public void doTransactionPopDelete(DB db) throws WorkloadException{
    	printDebug("------------POP Delete Transaction-------------");
    	//choose a random key
		int keynum = nextKeynum();
		
		String keynameInbox=buildKeyName(keynum, inboxSuffix);

		HashSet<String> fields=null;
		
		HashSet<String> counterColumnNames =new HashSet<String>();
		counterColumnNames.add(messagecountfieldkey);
		counterColumnNames.add(mailboxsizefieldkey);
		
		HashMap<String, ByteIterator> counterResult = new HashMap<String,ByteIterator>();
		HashMap<String, ByteIterator> uidlResult = new HashMap<String,ByteIterator>();
		HashMap<String, ByteIterator> result = new HashMap<String,ByteIterator>();
		
		HashSet<String> uidlKeySet = new HashSet<String>();

		
		long st=System.nanoTime();
		
		//read counter Table (POP3 STAT Command)
		db.read(incrementTag+countertable,keynameInbox,counterColumnNames,counterResult);
		
		int statCount = Integer.valueOf(counterResult.get(messagecountfieldkey).toString());
		if(statCount<=0){
			long en=System.nanoTime();
			printDebug("POP Delete: Inbox empty, closing...");
			Measurements.getMeasurements().measure("POP_DELETE-Transaction", (int)((en-st)/1000));
			return;
		}
		//get UIDL/LIST-List
		printDebug("POP Delete: reading uidl-List");
		db.read(sizetable,keynameInbox,fields,uidlResult);
		
		//DELE d-times
		//delete Messages. DB.delete() only allows deletion of whole rows, so  columnNames are appended with delimiter
		//min. deletion: 1, max. deletion: messagecount in Mailbox
		
		uidlKeySet.addAll(uidlResult.keySet());
		printDebug("POP Read: setting up delete count generator...");
		int deleteCount= getMessageDeleteCountGenerator(0, uidlKeySet.size()).nextInt();
		printDebug("POP Read: deleting "+deleteCount+" messages from "+keynameInbox);
		if(deleteCount>0){
			
			Iterator<String> retrievedIdsIterator = uidlResult.keySet().iterator();
			 for(int i=0;i<deleteCount;i++){
				 
				 String columnNameDelete=hashAndRangeKeyDelimiter+retrievedIdsIterator.next();
				 //System.out.println("deleting column: "+columnNameDelete);
				 db.delete(mailboxtable, keynameInbox+columnNameDelete);
				 db.delete(sizetable, keynameInbox+columnNameDelete);
			 }
			 
				//Update Counter
				 HashMap<String, ByteIterator> countUpdateValue=  new HashMap<String, ByteIterator>();
				 countUpdateValue.put(messagecountfieldkey, new StringByteIterator("-"+deleteCount));
				 countUpdateValue.put(mailboxsizefieldkey, new StringByteIterator("-"+deleteCount*100));//100: dummy size
				 
				 //indicate use of counterColumns with incremetTag in from of tablename
			 db.update(incrementTag+countertable,keynameInbox,countUpdateValue);
			 
		}
		
		//---
		
		long en=System.nanoTime();
		Measurements.getMeasurements().measure("POP_DELETE-Transaction", (int)((en-st)/1000));
    	    	
    }
     

	public void doTransactionSMTP(DB db){
		
		printDebug("------------SMTP Transaction-------------");
		
    	//choose a random key
		int keynumInbox = nextKeynum();
		int keynumOutbox = nextKeynum();
		
		String keynameInbox=buildKeyName(keynumInbox, inboxSuffix);
		String keynameOutbox=buildKeyName(keynumOutbox, outboxSuffix);
		
		HashSet<String> counterColumnNames =new HashSet<String>();
		counterColumnNames.add(messagecountfieldkey);
		counterColumnNames.add(mailboxsizefieldkey);
		HashMap<String, ByteIterator> counterResult = new HashMap<String,ByteIterator>();
		HashMap<String, ByteIterator> result = new HashMap<String,ByteIterator>();
		
		
		//build Maps containing one Message
		String uuid = "fieldSMTP_"+UUID.randomUUID();
		ArrayList<HashMap<String, ByteIterator>> valueList = buildSingleMessageValuesArray(uuid);
		HashMap<String, ByteIterator> valuesInbox = valueList.get(0);
		HashMap<String, ByteIterator> valuesOutbox = valueList.get(1);

		ArrayList<HashMap<String, ByteIterator>> sizetable_valueList = sizetable_buildSingleMessageValuesArray(uuid);
		HashMap<String, ByteIterator> sizetable_valuesInbox = sizetable_valueList.get(0);
		HashMap<String, ByteIterator> sizetable_valuesOutbox = sizetable_valueList.get(1);

		
		long st=System.nanoTime();
		
		//read messagecount inbox to determine if mailbox is full (>maxMessages)
		//add incremet tag before tablename to indicate counter column read
		
		db.read(incrementTag+countertable,keynameInbox,counterColumnNames,counterResult);
		
		int statCount = Integer.valueOf(counterResult.get(messagecountfieldkey).toString());
		printDebug("SMTP: current mailcount inbox: "+keynameInbox+ " #: "+statCount);
		if(statCount<=0){
			//System.out.println("counter row empty. Treating like value 0.");
			counterResult.put(messagecountfieldkey, new StringByteIterator("0"));
			counterResult.put(mailboxsizefieldkey, new StringByteIterator("0"));
		}
		HashMap<String, ByteIterator> countUpdateValue;
		long currentMailcount = Long.valueOf(counterResult.get(messagecountfieldkey).toString());
		//System.out.println("current mailcount "+keynameInbox+" : "+currentMailcount);
		if(currentMailcount<maxMessageCount){
			

		//store Message in Inbox Mailboxtable
		printDebug("SMTP: storing Message Inbox");
		db.update(mailboxtable,keynameInbox,valuesInbox);
		//store Message in Inbox Sizetable
		printDebug("SMTP: storing Message Size Inbox");
		db.update(sizetable,keynameInbox,sizetable_valuesInbox);
		
		//update Inbox counters
		
		countUpdateValue= new HashMap<String, ByteIterator>();
		 countUpdateValue.put(messagecountfieldkey, new StringByteIterator("1"));
		 countUpdateValue.put(mailboxsizefieldkey, new StringByteIterator("100"));
		 
		//add incremet tag before tablename to indicate counter column 
		 printDebug("SMTP: updating counters "+keynameInbox);
		 db.update(incrementTag+countertable,keynameInbox,countUpdateValue);
		
		}else{
			//System.out.println("Mailbox of receiver is full. Doing nothing...");
		}
		
		
		//read messagecount outbox to determine if mailbox is full (>maxMessages)
		//add incremet tag before tablename to indicate counter column 
		db.read(incrementTag+countertable,keynameOutbox,counterColumnNames,counterResult);
		//System.out.println("current mailcount "+keynameOutbox+" : "+currentMailcount);
		
		statCount = Integer.valueOf(counterResult.get(messagecountfieldkey).toString());
		printDebug("SMTP: current mailcount outbox: "+keynameOutbox+ " #: "+statCount);
		if(statCount<=0){
			
			//store Message in Outbox
			printDebug("SMTP: storing Message Outbox");
			db.update(mailboxtable,keynameOutbox,valuesOutbox);
			//store Message in Inbox Sizetable
			printDebug("SMTP: storing Message Size Outbox");
			db.update(sizetable,keynameOutbox,sizetable_valuesOutbox);

			//update outbox counters
			countUpdateValue= new HashMap<String, ByteIterator>();
			 countUpdateValue.put(messagecountfieldkey, new StringByteIterator("1"));
			 countUpdateValue.put(mailboxsizefieldkey, new StringByteIterator("100"));
			 
			 printDebug("SMTP: updating counters "+keynameOutbox);	
			 db.update(incrementTag+countertable,keynameOutbox,countUpdateValue);
			
		}else{
			//System.out.println("Mailbox of receiver is full. Doing nothing...");
		}		

		
		long en=System.nanoTime();
		Measurements.getMeasurements().measure("SMTP-Transaction", (int)((en-st)/1000));
    }
   

    
    int nextKeynum() {
        int keynum;
        if(keychooser instanceof ExponentialGenerator) {
            do
                {
                    keynum=transactioninsertkeysequence.lastInt() - keychooser.nextInt();
                }
            while(keynum < 0);
        } else {
            do
                {
                    keynum=keychooser.nextInt();
                }
            while (keynum > transactioninsertkeysequence.lastInt());
        }
        return keynum;
    }
    
    public void printDebug(String s){
    	System.out.println("MailAppCassandra: id: "+this.hashCode()+":: "+s);
    }
	
}
