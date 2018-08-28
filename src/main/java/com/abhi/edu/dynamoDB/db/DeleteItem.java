package com.abhi.edu.dynamoDB.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;

/**
 * 
 * @author Abhishek Khare
 *
 */
public class DeleteItem {

	public static void main(String[] args) {
		Map<String, Object> keyValueMap = new HashMap<String, Object>();
		keyValueMap.put("Artist", "Kishore Kumar");
		keyValueMap.put("SongTitle", "Neele Neele Ambar Par");
		GetItem.getItem();
		delete(keyValueMap);
		GetItem.getItem();
	}

	public static boolean delete(Map<String, Object> keyValueMap) {
		final AmazonDynamoDB ddb = Connection.getConnection();
		DeleteItemRequest deleteItemRequest = new DeleteItemRequest();
		deleteItemRequest.setTableName(Run.TABLE_NAME);
		Set<String> keySet = keyValueMap.keySet();
		for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext();) {
			String key = iterator.next();
			Object value = keyValueMap.get(key);
			if (value != null) {
				if (value instanceof String) {
					deleteItemRequest.addKeyEntry(key, new AttributeValue().withS(value.toString()));
				} else if (value instanceof Integer) {
					deleteItemRequest.addKeyEntry(key, new AttributeValue().withN(value.toString()));
				}
			}

		}
		ddb.deleteItem(deleteItemRequest);
		return true;
	}

}
