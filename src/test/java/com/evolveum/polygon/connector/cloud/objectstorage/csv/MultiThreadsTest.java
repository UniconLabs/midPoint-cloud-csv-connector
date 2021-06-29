/*
 * Copyright (c) 2010-2018 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.CsvTestUtil;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.ListResultHandler;

/**
 * @author skublik
 */
public class MultiThreadsTest extends BaseTest{
	
	private static final Log LOG = Log.getLog(CreateOpTest.class);

	private static final String NEW_UID = "uid=lukas,dc=example,dc=com";
    private static final String NEW_FIRST_NAME = "lukas";
    private static final String NEW_LAST_NAME = "skublik";
    private static final String NEW_PASSWORD = "password123";
    private static final String ATTR_PASSWORD_NAME = "__PASSWORD__";

    @Test
    public void create100AccountsAllAttributesNameEqualsUid() throws Exception {
    	testOp(RunnableCreateAccounts.CREATE_OP, 0, 25);
    }
    
    @Test
    public void update100Accounts() throws Exception {
    	testOp(RunnableCreateAccounts.UPDATE_OP, 0, 25);
    }
    
    @Test
    public void delete100Accounts() throws Exception {
        testOp(RunnableCreateAccounts.DELETE_OP, 0, 25);
    }
    
    private void testOp(String operation, int from, int by) throws Exception {
    	Set<Map<String, String>> setExpectedRecord = new HashSet<>();
        Map<Integer, Set<Attribute>> mapAttributes1 = getMapOfAttributesForThread(from+(0*by), from+(1*by), setExpectedRecord, false);
        Map<Integer, Set<Attribute>> mapAttributes2 = getMapOfAttributesForThread(from+(1*by), from+(2*by), setExpectedRecord, false);
        Map<Integer, Set<Attribute>> mapAttributes3 = getMapOfAttributesForThread(from+(2*by), from+(3*by), setExpectedRecord, false);
        Map<Integer, Set<Attribute>> mapAttributes4 = getMapOfAttributesForThread(from+(3*by), from+(4*by), setExpectedRecord, false);
        
        CloudCsvConfiguration config = createConfiguration();
        if(operation.equals(RunnableCreateAccounts.CREATE_OP)) {
        	copyDataFile("/create.csv", config);
        } else if(operation.equals(RunnableCreateAccounts.UPDATE_OP) || operation.equals(RunnableCreateAccounts.DELETE_OP)) {
        	copyDataFile("/multi-threads-update-delete.csv", config);
        }
        
        RunnableCreateAccounts t1 = new RunnableCreateAccounts(mapAttributes1, operation);
        RunnableCreateAccounts t2 = new RunnableCreateAccounts(mapAttributes2, operation);
        RunnableCreateAccounts t3 = new RunnableCreateAccounts(mapAttributes3, operation);
        RunnableCreateAccounts t4 = new RunnableCreateAccounts(mapAttributes4, operation);
        
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        
        t1.getThread().join();
        t2.getThread().join();
        t3.getThread().join();
        t4.getThread().join();
        
        if(operation.equals(RunnableCreateAccounts.DELETE_OP)) {
        	ConnectorFacade connector = createNewInstance(config);
            ListResultHandler handler = new ListResultHandler();
            connector.search(ObjectClass.ACCOUNT, null, handler, null);
            AssertJUnit.assertEquals(0, handler.getObjects().size());
        } else if(operation.equals(RunnableCreateAccounts.UPDATE_OP) || operation.equals(RunnableCreateAccounts.CREATE_OP)) {
        	for(Map<String, String> expectedRecord : setExpectedRecord) {
            	Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), expectedRecord.get(ATTR_UID));
            	assertEquals(expectedRecord, realRecord);
            }
        }
    }
    
    private Map<Integer, Set<Attribute>> getMapOfAttributesForThread(int from, int to, Set<Map<String, String>> setExpectedRecord, boolean update){
    	
    	Map<Integer, Set<Attribute>> mapAttributes = new HashMap<>();
    	for(int i = from; i < to; i++) {
    		Set<Attribute> attributes = new HashSet<>();
        	attributes.add(new Name(NEW_UID + i));
        	attributes.add(createAttribute(ATTR_UID, NEW_UID + i));
        	
        	attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME + i));
        	attributes.add(AttributeBuilder.buildPassword(new GuardedString((NEW_PASSWORD + i).toCharArray())));
        	
        	Map<String, String> expectedRecord = new HashMap<>();
            expectedRecord.put(ATTR_UID, NEW_UID + i);
            expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME + i);
            expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD + i);
            
            if(update) {
            	attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME + i + "update"));
            	expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME + i + "update");
            } else {
            	attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME + i));
            	expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME + i);
            }
            
            mapAttributes.put(i, attributes);
            setExpectedRecord.add(expectedRecord);
        }
    	return mapAttributes;
    }
    
    private class RunnableCreateAccounts implements Runnable {
    	private Thread t;
    	private Map<Integer, Set<Attribute>> setAttributes;
		private String operation;
    	
    	public static final String CREATE_OP = "create";
    	public static final String UPDATE_OP = "update";
    	public static final String DELETE_OP = "delete";
    	   
    	RunnableCreateAccounts(Map<Integer, Set<Attribute>> setAttributes, String operation) {
    		this.setAttributes = setAttributes;
    		this.operation = operation;
    	}
    	
    	public void run() {
    		
    		ConnectorFacade connector = createNewInstance(createConfiguration());
    		  
    		for(int i : setAttributes.keySet()) {
    			if(operation.equals(CREATE_OP) || operation.equals(UPDATE_OP)) {
    				Set<Attribute> attributes = setAttributes.get(i);
    				Uid uid = null;
    				if(operation.equals(CREATE_OP)) {
    					uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
    				} else {
    					uid = connector.update(ObjectClass.ACCOUNT, new Uid(NEW_UID+i), attributes, null);
    				}
    				assertNotNull(uid);
    				assertEquals(NEW_UID+i, uid.getUidValue());
    			} else if(operation.equals(DELETE_OP)) {
    				connector.delete(ObjectClass.ACCOUNT, new Uid(NEW_UID+i), null);
    			}
    		}
    	}
    	   
    	public void start () {
    	   if (t == null) {
    	      t = new Thread (this);
    	      t.start ();
    	   }
    	}
    	
    	public Thread getThread() {
			return t;
		}
    }
}
