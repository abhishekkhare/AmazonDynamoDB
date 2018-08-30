package com.abhi.edu.dynamoDB.db;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

/**
 * 
 * @author Abhishek Khare
 *
 */
public class UpdateItem {
	public static void main(String[] args) throws Exception {
		Map<String, Object> keyValueMap = new HashMap<String, Object>();
		keyValueMap.put("Artist", "Kishore Kumar");
		keyValueMap.put("SongTitle", "Neele Neele Ambar Par");

		Map<String, Object> updatedvalues = new HashMap<String, Object>();
		updatedvalues.put("AlbumTitle", "Kalaakaar");
		updatedvalues.put("DateTime", new Date().toString());
		update(keyValueMap, updatedvalues);
		GetItem.getItem();
		Thread.sleep(3000);
		updateWithJSON(keyValueMap, "{\"AlbumTitle\":\"Kalaakaar\",\"DateTime\":\"" + new Date().toString() + "\"}");
		GetItem.getItem();
	}

	public static void update(Map<String, Object> keyValueMap, Map<String, Object> updatedvalues) throws Exception {
		final AmazonDynamoDB ddb = Connection.getConnection();
		Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
		Set<String> keySet = updatedvalues.keySet();
		for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext();) {
			String key = iterator.next();
			Object value = updatedvalues.get(key);
			AttributeValueUpdate attrValue = new AttributeValueUpdate();
			if (value != null) {
				if (value instanceof String) {
					attrValue.setValue(new AttributeValue().withS(value.toString()));
				} else if (value instanceof Integer) {
					attrValue.setValue(new AttributeValue().withN(value.toString()));
				}
			}
			attributeUpdates.put(key, attrValue);
		}
		UpdateItemRequest rq = new UpdateItemRequest();
		rq.setTableName(Run.TABLE_NAME);
		keySet = keyValueMap.keySet();
		for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext();) {
			String key = iterator.next();
			Object value = keyValueMap.get(key);
			if (value != null) {
				if (value instanceof String) {
					rq.addKeyEntry(key, new AttributeValue().withS(value.toString()));
				} else if (value instanceof Integer) {
					rq.addKeyEntry(key, new AttributeValue().withN(value.toString()));
				}
			}

		}
		rq.setAttributeUpdates(attributeUpdates);
		UpdateItemResult rs = ddb.updateItem(rq);
		System.out.println("DONE!!" + rs);
	}

	public static void updateWithJSON(Map<String, Object> keyValueMap, String json) throws Exception {
		final AmazonDynamoDB ddb = Connection.getConnection();
		Item item = Item.fromJSON(json);
		Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
		Map<String, AttributeValue> item_values = ItemUtils.toAttributeValues(item);
		Set<String> keys = item_values.keySet();
		for (Iterator<String> iterator = keys.iterator(); iterator.hasNext();) {
			String key = iterator.next();
			attributeUpdates.put(key, new AttributeValueUpdate(item_values.get(key), AttributeAction.PUT));
		}

		UpdateItemRequest rq = new UpdateItemRequest();
		rq.setTableName(Run.TABLE_NAME);
		Set<String> keySet = keyValueMap.keySet();
		for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext();) {
			String key = iterator.next();
			Object value = keyValueMap.get(key);
			if (value != null) {
				if (value instanceof String) {
					rq.addKeyEntry(key, new AttributeValue().withS(value.toString()));
				} else if (value instanceof Integer) {
					rq.addKeyEntry(key, new AttributeValue().withN(value.toString()));
				}
			}

		}
		rq.setAttributeUpdates(attributeUpdates);
		UpdateItemResult rs = ddb.updateItem(rq);
		System.out.println("DONE!!" + rs);
	}
}
