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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;

import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.measurements.Measurements;


/*DynamoDB MaiApp Workload
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



public class MailAppDynamoDBWorkload extends MailAppWorkload
{
	//range key support
	private String hashAndRangeKeyDelimiter;
	private String rangeKeyName;
	private String messageSizeAttribute;
	private String messageAttribute="message";
	
	public void init(Properties p) throws WorkloadException{
		super.init(p);
		hashAndRangeKeyDelimiter = p.getProperty("dynamodb.rangekeydelimiter","_rangeKeyFollows_");
		messageSizeAttribute = p.getProperty("dynamodb.sizeattributename","size");
		messageAttribute = p.getProperty("dynamodb.messageattributename","message");
		
        //range key support
		String rangeKey = p.getProperty("dynamodb.rangeKey","range");
        this.rangeKeyName = rangeKey;
		
	}
	public String buildHashKeyName(long keynum /* ~username */, String mailboxSuffix /* ~inbox or outbox */) {
 		//like joe@gmail.com#inbox
		return keynum+keyDelimiter+mailboxSuffix;
	}
	public String buildRangeKeyName() {
// like "1362592658903_b69e87e8-5a05-4487-b90e-6d9e067f15cd"
		return System.currentTimeMillis()+"_"+UUID.randomUUID();
	}
	
/*	
	HashMap<String, ByteIterator> buildNewMessage() {
 		HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

  		
 		int msgSize = messageSizegenerator.nextInt();
 			ByteIterator data= new RandomByteIterator(msgSize);
 			values.put(messageAttribute,data);
 			values.put(messageSizeAttribute, new StringByteIterator(""+msgSize));
		return values;
	}
	
	HashMap<String, ByteIterator> buildValues() {
 		HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

 		//build one Message with a length determined by the generator
 		

 		int msgSize = messageSizegenerator.nextInt();
 			ByteIterator data= new RandomByteIterator(msgSize);
 			
 			values.put(messageAttribute,data);
 			values.put(messageSizeAttribute, new StringByteIterator(""+msgSize));
 		
		return values;
	}*/
	ArrayList<HashMap<String, ByteIterator>> buildValuesArray() {
		//return equally sized copies in an array
		ArrayList<HashMap<String, ByteIterator>> valuesArray = new ArrayList<HashMap<String, ByteIterator>>();


 		HashMap<String,ByteIterator> values1=new HashMap<String,ByteIterator>();
 		HashMap<String,ByteIterator> values2=new HashMap<String,ByteIterator>();

 		//build one Message
 		//cannot request specific fields later, because they might not exist on all records
 		
 		

 			int msgSize = messageSizegenerator.nextInt();
 			ByteIterator data1= new RandomByteIterator(1+msgSize);
 			ByteIterator data2= new RandomByteIterator(1+msgSize);
 			
 			//System.out.println("messageAttribute : "+messageAttribute);
 			values1.put(messageAttribute,data1);

 			
 			values2.put(messageAttribute,data2);

 		
 		valuesArray.add(values1);
 		valuesArray.add(values2);
	
		return valuesArray;
	}
	ArrayList<HashMap<String, ByteIterator>> sizetable_buildValuesArray() {
		//return equally sized copies in an array
		ArrayList<HashMap<String, ByteIterator>> valuesArray = new ArrayList<HashMap<String, ByteIterator>>();


 		HashMap<String,ByteIterator> values1=new HashMap<String,ByteIterator>();
 		HashMap<String,ByteIterator> values2=new HashMap<String,ByteIterator>();

 		
 		

 			int msgSize = messageSizegenerator.nextInt();

 			
 			//System.out.println("messageAttribute : "+messageAttribute);

 			values1.put(messageSizeAttribute,new StringByteIterator(""+msgSize));
 			

 			values2.put(messageSizeAttribute,new StringByteIterator(""+msgSize));
 		
 		valuesArray.add(values1);
 		valuesArray.add(values2);
	
		return valuesArray;
	}
	
	//build with random values
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
	
	//build with real values
	HashMap<String, ByteIterator> buildCounterValues(long increment) {
		
 		HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();
			

			ByteIterator messageCountData;
			if(!(increment<0)){
 			messageCountData= new StringByteIterator(incrementTag+String.valueOf(increment)) ;
			}else{
			messageCountData= new StringByteIterator(decrementTag+String.valueOf(-1*increment)) ;	
			}
 			values.put(messagecountfieldkey,messageCountData);
 			
 			
 	
 			
 			ByteIterator messageSizeData;
 			if(!(increment<0)){
 			messageSizeData= new StringByteIterator(incrementTag+String.valueOf(increment));
 			}else{
 			messageSizeData= new StringByteIterator(decrementTag+String.valueOf(-1*increment));
 			}
 			values.put(mailboxsizefieldkey,messageSizeData);
 		
		return values;
	}
	
	/**
	 * Do one insert operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
	 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
	 * effects other than DB operations.
	 */

	public ByteIterator getByteIteratorFromArray(String[] arr){
		String s ="";
		for(int i=0;i<arr.length;i++){
			s=s+arr[i];
		}
		return new StringByteIterator(s);
	}
	public boolean doInsert(DB db, Object threadstate)
	//Befüllen der Tabelle in der Loadphase (je 1 record pro Aufruf)
	{
		
/*
			
			//Make inserts for a whole inbox and outbox mailbox for one user
		
		int keynum=keysequence.nextInt();
		String inboxdbkey = buildHashKeyName(keynum, inboxSuffix);
		String outboxdbkey = buildHashKeyName(keynum, outboxSuffix);
		boolean returnValue=true;
			
			int messageCount = messageCountGenerator.nextInt();	
			long st=System.nanoTime();
			for(int i = 0;i<messageCount;i++){

				//Get two similar ByteIterators with same length
			ArrayList<HashMap<String, ByteIterator>> valueList = buildValuesArray();
			HashMap<String, ByteIterator> valuesInbox = valueList.get(0);
			HashMap<String, ByteIterator> valuesOutbox = valueList.get(1);

			HashMap<String, ByteIterator> counterValuesInbox = buildCounterValues();
			HashMap<String, ByteIterator> counterValuesOutbox = buildCounterValues();
			
			
			
			
			String counterHashKey = inboxdbkey.split(keyDelimiter)[0];                                                     
					
			
			
			String rangeKey = buildRangeKeyName();
			
			boolean insertMessage = db.insert(mailboxtable, inboxdbkey+hashAndRangeKeyDelimiter+rangeKey, valuesInbox) == 0;
			boolean counterMessage = db.update(countertable, counterHashKey+hashAndRangeKeyDelimiter+inboxSuffix, new HashMap<String, ByteIterator>(counterValuesInbox)) == 0;
			
			boolean outboxInsertMessage = db.insert(mailboxtable, outboxdbkey+hashAndRangeKeyDelimiter+rangeKey, new HashMap<String, ByteIterator>(valuesOutbox)) == 0;
			boolean outboxCounterMessage = db.update(countertable, counterHashKey+hashAndRangeKeyDelimiter+outboxSuffix, new HashMap<String, ByteIterator>(counterValuesOutbox)) == 0;
			
				if(returnValue==true&&(insertMessage==false||counterMessage==false||outboxInsertMessage==false||outboxCounterMessage==false)){
					returnValue=false;
				}
			}
					
			long en=System.nanoTime();
			Measurements.getMeasurements().measure("loadMailbox", (int)((en-st)/1000));
			
			if (returnValue)
				return true;
			else
			*/
				return false;
		
		
			
			
		
	}



    public void doTransactionPopReadOnly(DB db) throws WorkloadException{
//retrieve some Messages
    	
    	//choose a random key
		int keynum = nextKeynum();
		
		//not obeyed in query
		int dummyInt =10;
		
		String keynameInbox=buildHashKeyName(keynum, inboxSuffix);

		HashSet<String> fields=null;
		
		Vector<HashMap<String, ByteIterator>> uidlResult = new Vector<HashMap<String, ByteIterator>>();
		HashMap<String,ByteIterator> counterReadResult = new HashMap<String,ByteIterator>();
		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String, ByteIterator>>();

		
		long st=System.nanoTime();
		//STAT: read counter Table (POP3 STAT Command)
		db.read(countertable,keynum+hashAndRangeKeyDelimiter+inboxSuffix,fields,counterReadResult);
		if(counterReadResult.isEmpty()){
			long en=System.nanoTime();
			Measurements.getMeasurements().measure("POP_ReadOnly-Transaction", (int)((en-st)/1000));
			return;
		}
		//UIDL/LIST
		fields=new HashSet<String>();
		fields.add(rangeKeyName);//timeuuid
		fields.add(messageSizeAttribute);//message size
		db.scan(sizetable, keynameInbox+hashAndRangeKeyDelimiter, dummyInt, fields, uidlResult);
		
		//RETR: get x Messages from Inbox. In this case scan is query
		 int retrieveCount = getMessageRetrieveCountGenerator(0, uidlResult.size()).nextInt();
		 fields=null;
		for(int i=0;i< retrieveCount;i++){
			db.read(mailboxtable,keynameInbox+hashAndRangeKeyDelimiter+rangeKeyName,fields,new HashMap<String, ByteIterator>());
		}
    	
    	long en=System.nanoTime();
		Measurements.getMeasurements().measure("POP_ReadOnly-Transaction", (int)((en-st)/1000));
    	    	
    }
    public void doTransactionPopReadDeleteAll(DB db){
   //retrieve all MEssages and delete all
    	
    	//choose a random key
		int keynum = nextKeynum();
		
		//not obeyed in query
		int dummyInt =10;
		
		String keynameInbox=buildHashKeyName(keynum, inboxSuffix);

		HashSet<String> fields=null;
		
		Vector<HashMap<String, ByteIterator>> uidlResult = new Vector<HashMap<String, ByteIterator>>();
		HashMap<String,ByteIterator> counterReadResult = new HashMap<String,ByteIterator>();
		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String, ByteIterator>>();

		
		long st=System.nanoTime();
		//STAT: read counter Table (POP3 STAT Command)
		db.read(countertable,keynum+hashAndRangeKeyDelimiter+inboxSuffix,fields,counterReadResult);
		if(counterReadResult.isEmpty()){
			long en=System.nanoTime();
			Measurements.getMeasurements().measure("POP_ReadDeleteAll-Transaction", (int)((en-st)/1000));
			return;
		}
		//UIDL/LIST
		fields=new HashSet<String>();
		fields.add(rangeKeyName);//timeuuid
		fields.add(messageSizeAttribute);//message size
		db.scan(sizetable, keynameInbox+hashAndRangeKeyDelimiter, dummyInt, fields, uidlResult);
		
		//RETR: get x Messages from Inbox.
		 int retrieveCount = uidlResult.size();
		 fields=null;
		for(int i=0;i< retrieveCount;i++){
			db.read(mailboxtable,keynameInbox+hashAndRangeKeyDelimiter+rangeKeyName,fields,new HashMap<String, ByteIterator>());
		}
		
		//DELE: delete d Messages
		if(uidlResult.size()>0){
			int deleteCount= retrieveCount;
			Iterator<HashMap<String, ByteIterator>> idIterator = uidlResult.iterator();
			for(int i=0;i<deleteCount;i++){
				 
				String timeuuid = idIterator.next().get(rangeKeyName).toString();
				 db.delete(mailboxtable, keynameInbox+hashAndRangeKeyDelimiter+timeuuid);
				 db.delete(sizetable, keynameInbox+hashAndRangeKeyDelimiter+timeuuid);
				 
			 }
			
		
					//Update Counter
					 HashMap<String, ByteIterator> countUpdateValue=  new HashMap<String, ByteIterator>();
					 countUpdateValue.put(messagecountfieldkey, new StringByteIterator(decrementTag+deleteCount));
					 countUpdateValue.put(mailboxsizefieldkey, new StringByteIterator(decrementTag+deleteCount*100));
					 
					 //indicate use of counterColumns with incremetTag in from of tablename
				 db.update(countertable,keynum+hashAndRangeKeyDelimiter+inboxSuffix,countUpdateValue);
		}	 
		
		long en=System.nanoTime();
		Measurements.getMeasurements().measure("POP_ReadDeleteAll-Transaction", (int)((en-st)/1000));
    	    	
    }
    
    public void doTransactionPopEmpty(DB db){
    	//just do a pop 3 stat
    	//choose a random key
		int keynum = nextKeynum();
		String keynameInbox=buildHashKeyName(keynum, inboxSuffix);
		HashSet<String> fields=null;
		

		HashMap<String,ByteIterator> counterReadResult = new HashMap<String,ByteIterator>();

		
		long st=System.nanoTime();
		//STAT: read counter Table (POP3 STAT Command)
		db.read(countertable,keynum+hashAndRangeKeyDelimiter+inboxSuffix,fields,counterReadResult);
		if(counterReadResult.isEmpty()){
			long en=System.nanoTime();
			Measurements.getMeasurements().measure("POP_EMPTY-Transaction", (int)((en-st)/1000));
			return;
		}
		//UIDL/LIST
		fields=new HashSet<String>();
		fields.add(rangeKeyName);//timeuuid
		fields.add(messageSizeAttribute);//message size
		db.scan(sizetable, keynameInbox+hashAndRangeKeyDelimiter, 10, fields, new Vector<HashMap<String,ByteIterator>>());

		
		long en=System.nanoTime();
		Measurements.getMeasurements().measure("POP_EMPTY-Transaction", (int)((en-st)/1000));
    	    	
    }
    

    public void doTransactionPop(DB db) throws WorkloadException{
    	//STAT UIDL/LIST RETR x DELE d QUIT
    	
    	//choose a random key
		int keynum = nextKeynum();
		
		//not obeyed in query
		int dummyInt =10;
		
		String keynameInbox=buildHashKeyName(keynum, inboxSuffix);

		HashSet<String> fields=null;
		
		Vector<HashMap<String, ByteIterator>> uidlResult = new Vector<HashMap<String, ByteIterator>>();
		HashMap<String,ByteIterator> counterReadResult = new HashMap<String,ByteIterator>();
		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String, ByteIterator>>();

		
		long st=System.nanoTime();
		//STAT: read counter Table (POP3 STAT Command)
		db.read(countertable,keynum+hashAndRangeKeyDelimiter+inboxSuffix,fields,counterReadResult);

		
		if(counterReadResult.isEmpty()){
			long en=System.nanoTime();
			Measurements.getMeasurements().measure("POP-Transaction", (int)((en-st)/1000));
			return;
		}
		//UIDL/LIST
		fields=new HashSet<String>();
		fields.add(rangeKeyName);//timeuuid
		fields.add(messageSizeAttribute);//message size
		db.scan(sizetable, keynameInbox+hashAndRangeKeyDelimiter, dummyInt, fields, uidlResult);

		//RETR: get x Messages from Inbox. In this case scan is query
		 int retrieveCount = getMessageRetrieveCountGenerator(1, uidlResult.size()).nextInt();
		 fields=new HashSet<String>();
		 fields.add(messageAttribute);
		 
		for(int i=0;i< retrieveCount;i++){
			db.read(mailboxtable,keynameInbox+hashAndRangeKeyDelimiter+uidlResult.get(i).get(rangeKeyName).toString(),fields,new HashMap<String, ByteIterator>());
		}		

		
				
		long en=System.nanoTime();
		Measurements.getMeasurements().measure("POP-Transaction", (int)((en-st)/1000));
    	    	
    }
    
    public void doTransactionPopDelete(DB db) throws WorkloadException{
    	
    	//choose a random key
		int keynum = nextKeynum();
		
		//not obeyed in query
		int dummyInt =10;
		
		String keynameInbox=buildHashKeyName(keynum, inboxSuffix);

		HashSet<String> fields=null;
		
		Vector<HashMap<String, ByteIterator>> uidlResult = new Vector<HashMap<String, ByteIterator>>();
		HashMap<String,ByteIterator> counterReadResult = new HashMap<String,ByteIterator>();
		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String, ByteIterator>>();

		
		long st=System.nanoTime();
		//STAT: read counter Table (POP3 STAT Command)
		db.read(countertable,keynum+hashAndRangeKeyDelimiter+inboxSuffix,fields,counterReadResult);

		
		if(counterReadResult.isEmpty()){
			long en=System.nanoTime();
			Measurements.getMeasurements().measure("POP_DELETE-Transaction", (int)((en-st)/1000));
			return;
		}
		//UIDL/LIST
		fields=new HashSet<String>();
		fields.add(rangeKeyName);//request timeuuid
		fields.add(messageSizeAttribute);//request message size
		db.scan(sizetable, keynameInbox+hashAndRangeKeyDelimiter, dummyInt, fields, uidlResult);

				
		//DELE: delete d Messages

		int deleteCount= getMessageDeleteCountGenerator(1, uidlResult.size()).nextInt();
		Iterator<HashMap<String, ByteIterator>> idIterator = uidlResult.iterator();
		for(int i=0;i<deleteCount;i++){
			String timeuuid = idIterator.next().get(rangeKeyName).toString();
			 db.delete(mailboxtable, keynameInbox+hashAndRangeKeyDelimiter+timeuuid);
			 db.delete(sizetable, keynameInbox+hashAndRangeKeyDelimiter+timeuuid);
			 
		 }
		
		 if(deleteCount>0){
				//Update Counter
				 HashMap<String, ByteIterator> countUpdateValue=  new HashMap<String, ByteIterator>();
				 countUpdateValue.put(messagecountfieldkey, new StringByteIterator(decrementTag+deleteCount));
				 countUpdateValue.put(mailboxsizefieldkey, new StringByteIterator(decrementTag+deleteCount*100));//dummy size
				 
				 //indicate use of counterColumns with incremetTag in front of tablename
				 db.update(countertable,keynum+hashAndRangeKeyDelimiter+inboxSuffix,countUpdateValue);
			 }
		
		
				
		long en=System.nanoTime();
		Measurements.getMeasurements().measure("POP_DELETE-Transaction", (int)((en-st)/1000));
    	    	
    }
    
    public void doTransactionSMTP(DB db){
    	//choose a random key
		int keynumInbox = nextKeynum();
		int keynumOutbox = nextKeynum();

		
		String keynameInbox=buildHashKeyName(keynumInbox, inboxSuffix);
		String keynameOutbox=buildHashKeyName(keynumOutbox, outboxSuffix);
		
		//Get two similar ByteIterators with same length
		ArrayList<HashMap<String, ByteIterator>> sizetable_valueList = sizetable_buildValuesArray();
		HashMap<String, ByteIterator> sizetable_valuesInbox = sizetable_valueList.get(0);
		HashMap<String, ByteIterator> sizetable_valuesOutbox = sizetable_valueList.get(1);
		
		ArrayList<HashMap<String, ByteIterator>> valueList = buildValuesArray();
		HashMap<String, ByteIterator> valuesInbox = valueList.get(0);
		HashMap<String, ByteIterator> valuesOutbox = valueList.get(1);
		
		HashMap<String, ByteIterator> counterResult = new HashMap<String,ByteIterator>();
		
		String rangeKey = buildRangeKeyName();
		long st=System.nanoTime();
		
		//read messagecount to determine if mailbox is full (>maxMessages)
		db.read(countertable,keynumInbox+hashAndRangeKeyDelimiter+inboxSuffix,null,counterResult);
		long currentMailcount=0;
		if(!counterResult.isEmpty()){
			currentMailcount=Long.valueOf(counterResult.get(messagecountfieldkey).toString());
		}
		
		//System.out.println("Current mailcount key: "+keynameInbox+" value: "+currentMailcount);
		if(currentMailcount<maxMessageCount){
		
		//store Message in Inbox (and in sizetable)

		db.update(mailboxtable,keynameInbox+hashAndRangeKeyDelimiter+rangeKey,valuesInbox);
		db.update(sizetable,keynameInbox+hashAndRangeKeyDelimiter+rangeKey,sizetable_valuesInbox);
		//increase Inbox counters
		db.update(countertable, keynumInbox+hashAndRangeKeyDelimiter+inboxSuffix, buildCounterValues(1));
		}else{
			//System.out.println("Mailbox of receiver is full. Doing nothing...");
		}
		
		
		//read messagecount to determine if mailbox is full (>maxMessages)
		counterResult = new HashMap<String,ByteIterator>();
		db.read(countertable,keynumOutbox+hashAndRangeKeyDelimiter+outboxSuffix,null,counterResult);
		currentMailcount=0;
		if(!counterResult.isEmpty()){
			currentMailcount=Long.valueOf(counterResult.get(messagecountfieldkey).toString());
		}
		if(currentMailcount<maxMessageCount){
		//store Message in Outbox
		db.update(mailboxtable,keynameOutbox+hashAndRangeKeyDelimiter+rangeKey,valuesOutbox);
		db.update(sizetable,keynameOutbox+hashAndRangeKeyDelimiter+rangeKey,sizetable_valuesOutbox);
		//increase Outbox counters
		db.update(countertable, keynumOutbox+hashAndRangeKeyDelimiter+outboxSuffix, buildCounterValues(1));		
		}else{
			//System.out.println("Mailbox of sender is full. Doing nothing...");
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
	
}
