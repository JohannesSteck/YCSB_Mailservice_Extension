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

package com.yahoo.ycsb.db;


import com.yahoo.ycsb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Random;
import java.util.Properties;
import java.util.Map.Entry;
import java.nio.ByteBuffer;

import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.thrift.*;


//XXXX if we do replication, fix the consistency levels
/**
 * Cassandra 1.0.6 client for YCSB framework
 */
public class CassandraClient10 extends DB
{
  static Random random = new Random();
  public static final int Ok = 0;
  public static final int Error = -1;
  public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);

  public int ConnectionRetries;
  public int OperationRetries;
  public String column_family;

  public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
  public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
  public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
  public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";
 
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY = "cassandra.scanconsistencylevel";
  public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.deleteconsistencylevel";
  public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";


  
  TTransport tr;
  Cassandra.Client client;

  boolean _debug = false;

  String _table = "";
  Exception errorexception = null;

  List<Mutation> mutations = new ArrayList<Mutation>();
  Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
  Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

  ColumnParent parent;
 
  ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
  ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
  ConsistencyLevel scanConsistencyLevel = ConsistencyLevel.ONE;
  ConsistencyLevel deleteConsistencyLevel = ConsistencyLevel.ONE;
  
  private String incrementTag;
  private String decrementTag;
  
  private String hashAndRangeKeyDelimiter;

  //Hector---
  Cluster cluster;
  //---
  

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  public void init() throws DBException
  {
    String hosts = getProperties().getProperty("hosts");
    if (hosts == null)
    {
      throw new DBException("Required property \"hosts\" missing for CassandraClient");
    }
    

    column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
    parent = new ColumnParent(column_family);

    ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
        CONNECTION_RETRY_PROPERTY_DEFAULT));
    OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
        OPERATION_RETRY_PROPERTY_DEFAULT));

    String username = getProperties().getProperty(USERNAME_PROPERTY);
    String password = getProperties().getProperty(PASSWORD_PROPERTY);

    readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
    writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
    scanConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(SCAN_CONSISTENCY_LEVEL_PROPERTY, SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
    deleteConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(DELETE_CONSISTENCY_LEVEL_PROPERTY, DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));


    this.incrementTag = getProperties().getProperty("incrementtag","#inc#");
    this.decrementTag = getProperties().getProperty("decrementtag","#dec#");
    
    this.hashAndRangeKeyDelimiter=getProperties().getProperty("cassandra.rangekeydelimiter","_rangeKeyFollows_");
    
    _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

    String[] allhosts = hosts.split(",");
    String myhost = allhosts[random.nextInt(allhosts.length)];

    Exception connectexception = null;

    for (int retry = 0; retry < ConnectionRetries; retry++)
    {
      tr = new TFramedTransport(new TSocket(myhost, 9160));
      TProtocol proto = new TBinaryProtocol(tr);
      client = new Cassandra.Client(proto);
      try
      {
        tr.open();
        connectexception = null;
        break;
      } catch (Exception e)
      {
        connectexception = e;
      }
      try
      {
        Thread.sleep(1000);
      } catch (InterruptedException e)
      {
      }
    }
    if (connectexception != null)
    {
      System.err.println("Unable to connect to " + myhost + " after " + ConnectionRetries
          + " tries");
      throw new DBException(connectexception);
    }

    if (username != null && password != null)
    {
        Map<String,String> cred = new HashMap<String,String>();
        cred.put("username", username);
        cred.put("password", password);
        AuthenticationRequest req = new AuthenticationRequest(cred);
        try
        {
            client.login(req);
        }
        catch (Exception e)
        {
            throw new DBException(e);
        }
    }
    
  //Additionaly set up hector client for CounterColumn Support---
    cluster = HFactory.getOrCreateCluster("YCSBCluster",hosts);
    
    
    
    //---
    
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  public void cleanup() throws DBException
  {
    tr.close();
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  
  public int readCounterColumn(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
  {

	  
    if (!_table.equals(table)) {
      try
      {  	  
  
        _table = table;
      }
      catch (Exception e)
      {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Error;
      }
    }
    Keyspace ksp = HFactory.createKeyspace(table, cluster);
    for (int i = 0; i < OperationRetries; i++)
    {

      try
      {  
        if (fields == null)
        {
        System.out.println("ERROR: Column Names must be specified for reading CounterColumns!");
        return Error;
        } else {

          for (String field : fields)
          {
        	  
	        	  CounterQuery<String, String> counterQuery = new ThriftCounterColumnQuery<String, String>(
	  	                ksp, StringSerializer.get(), StringSerializer.get()).
	  				    setKey(key).setColumnFamily(column_family).setName(field);
	  			
	  			QueryResult<HCounterColumn<String>> qr = counterQuery.execute();
	  		HCounterColumn<String> hcc = qr.get();
	  		if(hcc!=null){
	  			
	  		    result.put(field,new StringByteIterator( hcc.getValue().toString()));
	  		}else {
	  			result.put(field,new StringByteIterator("0"));  		
	  		}	  
	        	  
  			
          }


        }
       
        if (_debug)
        {
          System.out.println("Reading key in CounterCF: " + key+ " retrieved columns: "+ result.size());
        
          System.out.println("ConsistencyLevel=" + readConsistencyLevel.toString());
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }

      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;

  }
  
  public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
  {
	
	  if(table.contains(incrementTag)){
		  table=table.replace(incrementTag, "");
		  
	        if (_debug)
	        {
	          System.out.println("Reading in counter column...");
	        }
	        return readCounterColumn(table, key, fields, result);
	  }
	  
    if (!_table.equals(table)) {
      try
      {
        client.set_keyspace(table);
        _table = table;
      }
      catch (Exception e)
      {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Error;
      }
    }

    for (int i = 0; i < OperationRetries; i++)
    {

      try
      {
        SlicePredicate predicate;
        
        if (_debug)
        {
        	
          System.out.println("Reading key: " + key);
          
        }
        
        if (fields == null)
        {
        	
          predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));

        } else {
        	
        	if (_debug)
            {
              System.out.println("Reading #columns specified: "+fields.size()+" sample specified column name: "+fields.iterator().next());
            }
        	
          ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
          for (String s : fields)
          {
            fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
          }

          predicate = new SlicePredicate().setColumn_names(fieldlist);
        }


        
        
        List<ColumnOrSuperColumn> results=null;
       
        results = client.get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, readConsistencyLevel);
                
        if (_debug)
        {
        	
          System.out.println("Reading key: " + key+ " retrived column count: "+results.size());
        }

        

        String name;
        ByteIterator value;
        
        

	        Column column;
	        for (ColumnOrSuperColumn oneresult : results)
	        {
	                	column=oneresult.column;	
		            name = new String(column.name.array(), column.name.position()+column.name.arrayOffset(), column.name.remaining());
		           
		            value = new ByteArrayByteIterator(column.value.array(), column.value.position()+column.value.arrayOffset(), column.value.remaining());
		         
	        	
	          result.put(name,value);
	
	          if (_debug)
	          {
	        	  System.out.println("read result column: "+name+" size: "+value.bytesLeft());
	        	  //TODO: Report Bug - value.toString() consumes Bytes -> stored results will be empty!
	           // System.out.print("(" + name + "=" + value + ")");
	          }
	        }
        
        if (_debug)
        {
          System.out.println();
          System.out.println("ConsistencyLevel=" + readConsistencyLevel.toString());
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }

      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public int scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result)
  {
    if (!_table.equals(table)) {
      try
      {
        client.set_keyspace(table);
        _table = table;
      }
      catch (Exception e)
      {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Error;
      }
    }

    for (int i = 0; i < OperationRetries; i++)
    {

      try
      {
        SlicePredicate predicate;
        if (fields == null)
        {
          predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));

        } else {
          ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
          for (String s : fields)
          {
              fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
          }

          predicate = new SlicePredicate().setColumn_names(fieldlist);
        }

        KeyRange kr = new KeyRange().setStart_key(startkey.getBytes("UTF-8")).setEnd_key(new byte[] {}).setCount(recordcount);

        List<KeySlice> results = client.get_range_slices(parent, predicate, kr, scanConsistencyLevel);

        if (_debug)
        {
          System.out.println("Scanning startkey: " + startkey);
        }

        HashMap<String, ByteIterator> tuple;
        for (KeySlice oneresult : results)
        {
          tuple = new HashMap<String, ByteIterator>();

          Column column;
          String name;
          ByteIterator value;
          for (ColumnOrSuperColumn onecol : oneresult.columns)
          {
              column = onecol.column;
              name = new String(column.name.array(), column.name.position()+column.name.arrayOffset(), column.name.remaining());
              value = new ByteArrayByteIterator(column.value.array(), column.value.position()+column.value.arrayOffset(), column.value.remaining());

              tuple.put(name, value);

            if (_debug)
            {
             // System.out.print("(" + name + "=" + value + ")");
            }
          }

          result.add(tuple);
          if (_debug)
          {
            System.out.println();
            System.out.println("ConsistencyLevel=" + scanConsistencyLevel.toString());
          }
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int update(String table, String key, HashMap<String, ByteIterator> values)
  {

	  if(table.contains(incrementTag)){
		  table=table.replace(incrementTag, "");
		  try{
		  Keyspace ksp = HFactory.createKeyspace(table, cluster);
		  Mutator<String> mutator = HFactory.createMutator(ksp, StringSerializer.get());
		  for(Entry<String, ByteIterator> entry : values.entrySet()){
			  long incremetValue = Long.valueOf(entry.getValue().toString());
		      if (_debug)
		      {
		        System.out.println("Updating counter column with key: " + key+" and value: "+incremetValue);
		      }

			  mutator.incrementCounter(key, column_family, entry.getKey(),incremetValue );
		  }
		  }catch (Exception e) {
			e.printStackTrace();
			return Error;
		}
		  return Ok;
		  
	  }else{
		  return insert(table, key, values);
	  }
	 
  }
  
  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int insert(String table, String key, HashMap<String, ByteIterator> values)
  {
	  
	  
    if (!_table.equals(table)) {
      try
      {
        client.set_keyspace(table);
        _table = table;
      }
      catch (Exception e)
      {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Error;
      }
    }

    for (int i = 0; i < OperationRetries; i++)
    {
      if (_debug)
      {
        System.out.println("Inserting key: " + key);
      }

      try
      {
        ByteBuffer wrappedKey = ByteBuffer.wrap(key.getBytes("UTF-8"));

        Column col;
        ColumnOrSuperColumn column;
        for (Map.Entry<String, ByteIterator> entry : values.entrySet())
        {
        	
        	if (_debug)
            {
              System.out.println("inserting new column in Row: " + key+" column name: "+ entry.getKey() +" value size: "+entry.getValue().bytesLeft());
            }	
        	
          col = new Column();
          col.setName(ByteBuffer.wrap(entry.getKey().getBytes("UTF-8")));
          col.setValue(ByteBuffer.wrap(entry.getValue().toArray()));
          col.setTimestamp(System.currentTimeMillis());

          column = new ColumnOrSuperColumn();
          column.setColumn(col);

          mutations.add(new Mutation().setColumn_or_supercolumn(column));//setColumn_or_supercolumn -> insert, setDeletion ->delete
        }

        mutationMap.put(column_family, mutations);
        record.put(wrappedKey, mutationMap);

        client.batch_mutate(record, writeConsistencyLevel);

        mutations.clear();
        mutationMap.clear();
        record.clear();
        
        if (_debug)
        {
           System.out.println("ConsistencyLevel=" + writeConsistencyLevel.toString());
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }

    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public int delete(String table, String key)  
  {
	  
	    if (!_table.equals(table)) {
	        try
	        {
	          client.set_keyspace(table);
	          _table = table;
	        }
	        catch (Exception e)
	        {
	          e.printStackTrace();
	          e.printStackTrace(System.out);
	          return Error;
	        }
	      }
	  
	  //TODO: Added Support for deletion of certain columns
	  if(key.contains(hashAndRangeKeyDelimiter)){
		  String[] split=key.split(hashAndRangeKeyDelimiter);
		  
		 return deleteSingleColumn(table, split[0], split[1]);
	  }else{
	  


    for (int i = 0; i < OperationRetries; i++)
    {
      try
      {
        client.remove(ByteBuffer.wrap(key.getBytes("UTF-8")),
                      new ColumnPath(column_family),
                      System.currentTimeMillis(),
                      deleteConsistencyLevel);

        if (_debug)
        {
          System.out.println("Delete key: " + key);
          System.out.println("ConsistencyLevel=" + deleteConsistencyLevel.toString());
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
	  }
  }
	  
  public int deleteSingleColumn(String table, String key, String columnName)  {
	    for (int i = 0; i < OperationRetries; i++)
	    {
	      try
	      {
	    	  ColumnPath path =  new ColumnPath();
	    	  path.column_family=column_family;
	    	  path.column=ByteBuffer.wrap(columnName.getBytes());
	    	  
	        client.remove(ByteBuffer.wrap(key.getBytes("UTF-8")),
	                      path,
	                      System.currentTimeMillis(),
	                      deleteConsistencyLevel);

	        if (_debug)
	        {
	          System.out.println("Delete Column "+columnName+" in Row with key: " + key);
	          System.out.println("ConsistencyLevel=" + deleteConsistencyLevel.toString());
	        }

	        return Ok;
	      } catch (Exception e)
	      {
	        errorexception = e;
	      }
	      try
	      {
	        Thread.sleep(500);
	      } catch (InterruptedException e)
	      {
	      }
	    }
	    errorexception.printStackTrace();
	    errorexception.printStackTrace(System.out);
	    return Error;
}
	  
	  

	  public static void main(String[] args)
	  {
	    CassandraClient10 cli = new CassandraClient10();

	    Properties props = new Properties();

	    props.setProperty("hosts", args[0]);
	    cli.setProperties(props);

	    try
	    {
	      cli.init();
	    } catch (Exception e)
	    {
	      e.printStackTrace();
	      System.exit(0);
	    }

	    HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
	    vals.put("age", new StringByteIterator("57"));
	    vals.put("middlename", new StringByteIterator("bradley"));
	    vals.put("favoritecolor", new StringByteIterator("blue"));
	    int res = cli.insert("usertable", "BrianFrankCooper", vals);
	    System.out.println("Result of insert: " + res);

	    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
	    HashSet<String> fields = new HashSet<String>();
	    fields.add("middlename");
	    fields.add("age");
	    fields.add("favoritecolor");
	    res = cli.read("usertable", "BrianFrankCooper", null, result);
	    System.out.println("Result of read: " + res);
	    for (String s : result.keySet())
	    {
	      System.out.println("[" + s + "]=[" + result.get(s) + "]");
	    }

	    res = cli.delete("usertable", "BrianFrankCooper");
	    System.out.println("Result of delete: " + res);
	  }
  
  

  /*
   * public static void main(String[] args) throws TException,
   * InvalidRequestException, UnavailableException,
   * UnsupportedEncodingException, NotFoundException {
   *
   *
   *
   * String key_user_id = "1";
   *
   *
   *
   *
   * client.insert("Keyspace1", key_user_id, new ColumnPath("Standard1", null,
   * "age".getBytes("UTF-8")), "24".getBytes("UTF-8"), timestamp,
   * ConsistencyLevel.ONE);
   *
   *
   * // read single column ColumnPath path = new ColumnPath("Standard1", null,
   * "name".getBytes("UTF-8"));
   *
   * System.out.println(client.get("Keyspace1", key_user_id, path,
   * ConsistencyLevel.ONE));
   *
   *
   * // read entire row SlicePredicate predicate = new SlicePredicate(null, new
   * SliceRange(new byte[0], new byte[0], false, 10));
   *
   * ColumnParent parent = new ColumnParent("Standard1", null);
   *
   * List<ColumnOrSuperColumn> results = client.get_slice("Keyspace1",
   * key_user_id, parent, predicate, ConsistencyLevel.ONE);
   *
   * for (ColumnOrSuperColumn result : results) {
   *
   * Column column = result.column;
   *
   * System.out.println(new String(column.name, "UTF-8") + " -> " + new
   * String(column.value, "UTF-8"));
   *
   * }
   *
   *
   *
   *
   * }
   */
}
