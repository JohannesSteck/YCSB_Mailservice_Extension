/*

 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.yahoo.ycsb.db;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeAction;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ReturnValue;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * DynamoDB v1.3.14 client for YCSB
 */

/**
 * JS
 * Problems: 
 * No Querys (no hash & range key)
 * global primary (hash-)key Attribute name set in Properties -> can't use other table with different PK Attribute name (but doesn't matter on the other hand)
 * 
 * 
 */

public class MailAppDynamoDBClient extends DB {

    private static final int OK = 0;
    private static final int SERVER_ERROR = 1;
    private static final int CLIENT_ERROR = 2;
    private AmazonDynamoDBClient dynamoDB;
    private String primaryKeyName;
    //range key support
    private String hashAndRangeKeyDelimiter;
    private String rangeKeyName = "rangeKey";
    private boolean debug = false;
    
    private String incrementTag="#inc#";
    private String decrementTag="#dec#";
    
    private boolean consistentRead = false;
    private String endpoint = "http://dynamodb.us-east-1.amazonaws.com";
    private int maxConnects = 50;
    private static Logger logger = Logger.getLogger(MailAppDynamoDBClient.class);
    
    public MailAppDynamoDBClient() {}
    
    
    
    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        // initialize DynamoDb driver & table.
        String debug = getProperties().getProperty("dynamodb.debug",null);

        if (null != debug && "true".equalsIgnoreCase(debug)) {
            logger.setLevel(Level.DEBUG);
        }

        String endpoint = getProperties().getProperty("dynamodb.endpoint",null);
        String credentialsFile = getProperties().getProperty("dynamodb.awsCredentialsFile",null);
        String primaryKey = getProperties().getProperty("dynamodb.primaryKey",null);
        //range key support
        String rangeKey = getProperties().getProperty("dynamodb.rangeKey","range");
        this.rangeKeyName = rangeKey;
        String hashAndRangeKeyDelimiterProp = getProperties().getProperty("dynamodb.rangekeydelimiter","_rangeKeyFollows_");
        this.hashAndRangeKeyDelimiter = hashAndRangeKeyDelimiterProp;
        String consistentReads = getProperties().getProperty("dynamodb.consistentReads",null);
        String connectMax = getProperties().getProperty("dynamodb.connectMax",null);
        this.incrementTag = getProperties().getProperty("incrementtag","#inc#");
        this.decrementTag = getProperties().getProperty("decrementtag","#dec#");

        if (null != connectMax) {
            this.maxConnects = Integer.parseInt(connectMax);
        }

        if (null != consistentReads && "true".equalsIgnoreCase(consistentReads)) {
            this.consistentRead = true;
        }

        if (null != endpoint) {
            this.endpoint = endpoint;
        }

        if (null == primaryKey || primaryKey.length() < 1) {
            String errMsg = "Missing primary key attribute name, cannot continue";
            logger.error(errMsg);
        }
        
        
        

        
        	
         


        try {
            AWSCredentials credentials = new PropertiesCredentials(new File(credentialsFile));
            ClientConfiguration cconfig = new ClientConfiguration();
            cconfig.setMaxConnections(maxConnects);
            dynamoDB = new AmazonDynamoDBClient(credentials,cconfig);
            dynamoDB.setEndpoint(this.endpoint);
            primaryKeyName = primaryKey;
            logger.info("dynamodb connection created with " + this.endpoint);
        } catch (Exception e1) {
            String errMsg = "MailAppDynamoDBClient.init(): Could not initialize MailAppDynamoDB client: " + e1.getMessage();
            logger.error(errMsg);
        }
    }

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
if(key.contains(hashAndRangeKeyDelimiter)){
    		String[] keys = key.split(hashAndRangeKeyDelimiter);
    		String hashKey=keys[0];
    		String rangeKey=keys[1];
    		return readWithHashAndRangeKey(table, hashKey, rangeKey, fields,result);
    	}else{

    		logger.debug("readkey: " + key + " from table: " + table);
    		
    		 GetItemRequest req = new GetItemRequest(table, createPrimaryKey(key));
 	        req.setAttributesToGet(fields);
 	        req.setConsistentRead(consistentRead);
 	        GetItemResult res = null;
 	
 	        try {
 	            res = dynamoDB.getItem(req);
 	        }catch (AmazonServiceException ex) {
 	            logger.error(ex.getMessage());
 	            return SERVER_ERROR;
 	        }catch (AmazonClientException ex){
 	            logger.error(ex.getMessage());
 	            return CLIENT_ERROR;
 	        }
 	
 	        if (null != res.getItem())
 	        {
 	            result.putAll(extractResult(res.getItem()));
 	            logger.debug("Result: " + res.toString());
 	        }
    		
    		
    		/*
    		
    		Key lastKeyEvaluated = null;
    		
    		do {
    			
    		
    		QueryRequest queryRequest = new QueryRequest()
    		.withTableName(table)
    		.withHashKeyValue(new AttributeValue(key))
    		.withExclusiveStartKey(lastKeyEvaluated);
    		QueryResult res;
    		try{
    		res = dynamoDB.query(queryRequest);
    		 }catch (AmazonServiceException ex) {
 	            logger.error(ex.getMessage());
 	            return SERVER_ERROR;
 	        }catch (AmazonClientException ex){
 	            logger.error(ex.getMessage());
 	            return CLIENT_ERROR;
 	        }
 	
 	        if (res.getCount()>0)
 	        {
 	        	
 	            result.putAll(extractResult(res.getItems().get(0)));
 	            logger.debug("Result: " + res.toString());
 	        }
 	        
 	        lastKeyEvaluated = res.getLastEvaluatedKey();
    		
    	} while (lastKeyEvaluated != null);
    		    		
    	*/	
    		//-------
	      /*  GetItemRequest req = new GetItemRequest(table, createPrimaryKey(key));
	        req.setAttributesToGet(fields);
	        req.setConsistentRead(consistentRead);
	        GetItemResult res = null;
	
	        try {
	            res = dynamoDB.getItem(req);
	        }catch (AmazonServiceException ex) {
	            logger.error(ex.getMessage());
	            return SERVER_ERROR;
	        }catch (AmazonClientException ex){
	            logger.error(ex.getMessage());
	            return CLIENT_ERROR;
	        }
	
	        if (null != res.getItem())
	        {
	            result.putAll(extractResult(res.getItem()));
	            logger.debug("Result: " + res.toString());
	        }
	        */
	        //return OK;
	        return (int)Math.ceil(res.getConsumedCapacityUnits());
    		}
    	}
  
    public int readWithHashAndRangeKey(String table, String hashKey, String rangeKey, Set<String> fields,
            HashMap<String, ByteIterator> result) {

		
    	logger.debug("readkey: " + hashKey + "rangeKey: "+rangeKey+" from table: " + table);
        Key key = new Key().withHashKeyElement(new AttributeValue(hashKey)).withRangeKeyElement(new AttributeValue(rangeKey));
        GetItemRequest req = new GetItemRequest().withTableName(table).withKey(key);
        req.setAttributesToGet(fields);
        req.setConsistentRead(consistentRead);
        GetItemResult res = null;

        try {
            res = dynamoDB.getItem(req);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
            return CLIENT_ERROR;
        }

        if (null != res.getItem())
        {
            result.putAll(extractResult(res.getItem()));
            logger.debug("Result: " + res.toString());
        }
        //return OK;
        return (int)Math.ceil(res.getConsumedCapacityUnits());
        
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    	if(startkey.contains(hashAndRangeKeyDelimiter)){
        	String[] keys = startkey.split(hashAndRangeKeyDelimiter);
        		//do a query instead of scan, if rangeKyDelimiter is in key string
        		String hashKey=keys[0];
        		return query(table, hashKey, recordcount, fields, result);
        	}else{
        
    	
    	
    	logger.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
        /*
         * on DynamoDB's scan, startkey is *exclusive* so we need to
         * getItem(startKey) and then use scan for the res
         */
    	
        GetItemRequest greq = new GetItemRequest(table, createPrimaryKey(startkey));
        greq.setAttributesToGet(fields);

        GetItemResult gres = null;

        try {
            gres = dynamoDB.getItem(greq);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
           return CLIENT_ERROR;
        }

        if (null != gres.getItem()) {
            result.add(extractResult(gres.getItem()));
        }

        int count = 1; // startKey is done, rest to go.

        Key startKey = createPrimaryKey(startkey);
        ScanRequest req = new ScanRequest(table);
        req.setAttributesToGet(fields);
        while (count < recordcount) {
            req.setExclusiveStartKey(startKey);
            req.setLimit(recordcount - count);
            ScanResult res = null;
            try {
                res = dynamoDB.scan(req);
            }catch (AmazonServiceException ex) {
                logger.error(ex.getMessage());
              ex.printStackTrace();
             return SERVER_ERROR;
            }catch (AmazonClientException ex){
                logger.error(ex.getMessage());
               ex.printStackTrace();
             return CLIENT_ERROR;
            }

            count += res.getCount();
            for (Map<String, AttributeValue> items : res.getItems()) {
                result.add(extractResult(items));
            }
            startKey = res.getLastEvaluatedKey();

        } 

        return OK;
        
    }
    }
    
    public int query(String table, String hashKey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
            
        		logger.debug("query key: " + hashKey + " from table: " + table);
        			
        		
        		Key lastKeyEvaluated = null;
        		
        		do {
        			
        		
        		QueryRequest queryRequest = new QueryRequest()
        		.withTableName(table)
        		.withHashKeyValue(new AttributeValue(hashKey))
        		.withExclusiveStartKey(lastKeyEvaluated);
        		
        		if(fields!=null){
        			for(String attrib : fields){
        				queryRequest.withAttributesToGet(attrib);
        			}
        		}
        		
        		QueryResult res;
        		try{
        		res = dynamoDB.query(queryRequest);
        		 }catch (AmazonServiceException ex) {
     	            logger.error(ex.getMessage());
     	            return SERVER_ERROR;
     	        }catch (AmazonClientException ex){
     	            logger.error(ex.getMessage());
     	            return CLIENT_ERROR;
     	        }
     	
     	        if (res.getCount()>0)
     	        {
     	        	 for (Map<String, AttributeValue> items : res.getItems()) {
     	                result.add(extractResult(items));
     	            }
     	            logger.debug("Result: " + res.toString());
     	        }
     	        
     	        lastKeyEvaluated = res.getLastEvaluatedKey();
        		
        	} while (lastKeyEvaluated != null);
        		    		
        
    	        return OK;
       }



    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
    	if(key.contains(hashAndRangeKeyDelimiter)){
    	String[] keys = key.split(hashAndRangeKeyDelimiter);
    	
    		String hashKey=keys[0];
    		String rangeKey=keys[1];
    		return updateWithHashAndRangeKey(table, hashKey, rangeKey, values);
    	}else{
    	
        logger.debug("updatekey: " + key + " from table: " + table);

        Map<String, AttributeValueUpdate> attributes = new HashMap<String, AttributeValueUpdate>(
                values.size());
        for (Entry<String, ByteIterator> val : values.entrySet()) {
        	
        	
            AttributeValue v = new AttributeValue(val.getValue().toString());
            
            //Counter support
            String stringValue = v.getS();
            
            if(stringValue.startsWith(incrementTag)){
            	Long incrementValue = Long.valueOf(stringValue.replace(incrementTag, ""));
            	attributes.put(val.getKey(),  
            			new AttributeValueUpdate()
			    .withAction(AttributeAction.ADD)
			    .withValue(new AttributeValue().withN("+"+incrementValue)));
            }else if(stringValue.startsWith(decrementTag)){
            	Long decrementValue = Long.valueOf(stringValue.replace(decrementTag, ""));
            	attributes.put(val.getKey(),  
            			new AttributeValueUpdate()
			    .withAction(AttributeAction.ADD)
			    .withValue(new AttributeValue().withN("-"+decrementValue)));
            }else{
            
            attributes.put(val.getKey(), new AttributeValueUpdate()
                    .withValue(v).withAction("PUT"));
            }
        }

        UpdateItemRequest req = new UpdateItemRequest(table, createPrimaryKey(key), attributes);

        try {
            dynamoDB.updateItem(req);
        }catch (AmazonServiceException ex) {
        	if(debug)ex.printStackTrace();
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
        	if(debug)ex.printStackTrace();
            logger.error(ex.getMessage());
            return CLIENT_ERROR;
        }
        return OK;
    	}
    }
    public int updateWithHashAndRangeKey(String table, String hashKey, String rangeKey, HashMap<String, ByteIterator> values) {
       
    	 	
    	logger.debug("updatekey: " + hashKey + " rangeKey: "+rangeKey+" from table: " + table);

        Map<String, AttributeValueUpdate> attributes = new HashMap<String, AttributeValueUpdate>(
                values.size());
        for (Entry<String, ByteIterator> val : values.entrySet()) {
            AttributeValue v = new AttributeValue(val.getValue().toString());
         
            
            //Counter support
            
            String stringValue = v.getS();

            
            if(stringValue.startsWith(incrementTag)){
            	Long incrementValue = Long.valueOf(stringValue.replace(incrementTag, ""));
            	attributes.put(val.getKey(),  
            			new AttributeValueUpdate()
			    .withAction(AttributeAction.ADD)
			    .withValue(new AttributeValue().withN("+"+incrementValue)));
            	
            }else if(stringValue.startsWith(decrementTag)){
            	Long decrementValue = Long.valueOf(stringValue.replace(decrementTag, ""));
            	attributes.put(val.getKey(),  
            			new AttributeValueUpdate()
			    .withAction(AttributeAction.ADD)
			    .withValue(new AttributeValue().withN("-"+decrementValue)));
            }else{
            	
            attributes.put(val.getKey(), new AttributeValueUpdate()
                    .withValue(v).withAction("PUT"));
            }
        }

Key key = new Key().withHashKeyElement(new AttributeValue(hashKey)).withRangeKeyElement(new AttributeValue(rangeKey));
        UpdateItemRequest req = new UpdateItemRequest().withKey(key).withTableName(table).withAttributeUpdates(attributes);
       
            //System.out.println("updating table "+table+" h+r key: "+key+ "values "+attributes);
           
        try {
            dynamoDB.updateItem(req);
        }catch (AmazonServiceException ex) {
        	ex.printStackTrace();
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
        	ex.printStackTrace();
            logger.error(ex.getMessage());
           
            return CLIENT_ERROR;
        }
        return OK;
    }

    @Override
    public int insert(String table, String key,HashMap<String, ByteIterator> values) {
    	if(key.contains(hashAndRangeKeyDelimiter)){
    	String[] keys = key.split(hashAndRangeKeyDelimiter);
    
    		String hashKey=keys[0];
    		String rangeKey=keys[1];
    		return insertWithHashAndRangeKey(table, hashKey, rangeKey, values);
    	}else{
    	logger.debug("insertkey: " + primaryKeyName + "-" + key + " from table: " + table);
        Map<String, AttributeValue> attributes = createAttributes(values);
        // adding primary key
        attributes.put(primaryKeyName, new AttributeValue(key));

        PutItemRequest putItemRequest = new PutItemRequest(table, attributes);
        PutItemResult res = null;
        try {
            res = dynamoDB.putItem(putItemRequest);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
            return CLIENT_ERROR;
        }
        return res.getConsumedCapacityUnits().intValue()*-1;
    	}
    }
    
    public int insertWithHashAndRangeKey(String table, String hashKey, String rangeKey, HashMap<String, ByteIterator> values) {
        logger.debug("insertkey: " + primaryKeyName + "-" + hashKey + " from table: " + table);
        Map<String, AttributeValue> attributes = createAttributesForHashAndRange(values);
        
        // adding primary key
        attributes.put(primaryKeyName, new AttributeValue().withS(hashKey));
        attributes.put(rangeKeyName, new AttributeValue().withS(rangeKey));
       // System.out.println("attributes: "+attributes);
        PutItemRequest putItemRequest = new PutItemRequest().withTableName(table).withItem(attributes);
        PutItemResult res = null;
        try {
            res = dynamoDB.putItem(putItemRequest);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
            return CLIENT_ERROR;
        }
        return res.getConsumedCapacityUnits().intValue()*-1;
    }

    @Override
    public int delete(String table, String key) {
    	if(key.contains(hashAndRangeKeyDelimiter)){
        	String[] keys = key.split(hashAndRangeKeyDelimiter);
        
        		String hashKey=keys[0];
        		String rangeKey=keys[1];
        		return deleteWithHashAndRangeKey(table, hashKey, rangeKey);
        	}else{
    	logger.debug("deletekey: " + key + " from table: " + table);
        DeleteItemRequest req = new DeleteItemRequest(table, createPrimaryKey(key));
        DeleteItemResult res = null;

        try {
            res = dynamoDB.deleteItem(req);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
            return CLIENT_ERROR;
        }
        return res.getConsumedCapacityUnits().intValue()*-1;
        	}
    }
    public int deleteWithHashAndRangeKey(String table, String hashKey, String rangeKey) {
    	logger.debug("deletekey: " + hashKey + " rangeKey "+rangeKey+" from table: " + table);
    	ReturnValue returnValues = ReturnValue.ALL_OLD;
    	Key key = new Key().withHashKeyElement(new AttributeValue(hashKey)).withRangeKeyElement(new AttributeValue(rangeKey));
        DeleteItemRequest req = new DeleteItemRequest().withTableName(table).withKey(key).withReturnValues(returnValues);
        DeleteItemResult res = null;

        try {
        	res = dynamoDB.deleteItem(req);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
            return CLIENT_ERROR;
        }
        return res.getConsumedCapacityUnits().intValue()*-1;
    }

    private static Map<String, AttributeValue> createAttributes(
            HashMap<String, ByteIterator> values) {
        Map<String, AttributeValue> attributes = new HashMap<String, AttributeValue>(
                values.size() + 1); //leave space for the PrimaryKey
        for (Entry<String, ByteIterator> val : values.entrySet()) {
            attributes.put(val.getKey(), new AttributeValue(val.getValue()
                    .toString()));
        }
        return attributes;
    }

    private static Map<String, AttributeValue> createAttributesForHashAndRange(
            HashMap<String, ByteIterator> values) {
        Map<String, AttributeValue> attributes = new HashMap<String, AttributeValue>(
               ); //leave space for hash and range key
        for (Entry<String, ByteIterator> val : values.entrySet()) {
            attributes.put(val.getKey(), new AttributeValue(val.getValue()
                    .toString()));

        }
        return attributes;
    }
    
    private HashMap<String, ByteIterator> extractResult(Map<String, AttributeValue> item) {
        if(null == item)
            return null;
        HashMap<String, ByteIterator> rItems = new HashMap<String, ByteIterator>(item.size());

        for (Entry<String, AttributeValue> attr : item.entrySet()) {
            logger.debug(String.format("Result- key: %s, value: %s", attr.getKey(), attr.getValue()) );
            String sOrNValue=attr.getValue().getS();
            if(sOrNValue==null){
            	sOrNValue=attr.getValue().getN();
            }
            rItems.put(attr.getKey(), new StringByteIterator(sOrNValue));
        }
        return rItems;
    }

    private static Key createPrimaryKey(String key) {
        Key k = new Key().withHashKeyElement(new AttributeValue().withS(key));
        return k;
    }
}
