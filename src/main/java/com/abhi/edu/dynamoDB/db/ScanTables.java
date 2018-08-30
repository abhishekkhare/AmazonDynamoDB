package com.abhi.edu.dynamoDB.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * 
 * @author Abhishek Khare
 *
 */
public class ScanTables {
	public static void main(String[] args) {
		getALL();
		getWithFilter("Artist", "Kishore Kumar");
	}
	
	public static boolean getWithFilter(String tableKey, String columnValue) {
		try {
			final AmazonDynamoDB ddb = Connection.getConnection();
			Condition condition = new Condition();
			condition.withComparisonOperator(ComparisonOperator.CONTAINS)
					.withAttributeValueList(new AttributeValue().withS(columnValue));
			Map<String, Condition> scanFilter = new HashMap<String, Condition>();
			scanFilter.put(tableKey, condition);

			ScanRequest scanRequest = new ScanRequest().withTableName(Run.TABLE_NAME).withLimit(200).withScanFilter(scanFilter);
			ScanResult result = ddb.scan(scanRequest);
			System.out.println("result!" + result);
			/*for (Map<String, AttributeValue> item : result.getItems()) {
				Set<String> keySet = item.keySet();
				for (String key : keySet) {
					AttributeValue value = item.get(key);
					System.out.println("ITEM:::" + key + " ::" + item.get(key));
				}

			}*/
			List<Map<String, AttributeValue>> items = result.getItems();
			List<Item> itemList = ItemUtils.toItemList(items);
			final ArrayNode array = JsonNodeFactory.instance.arrayNode(); 
			for (Iterator <Item>iterator = itemList.iterator(); iterator.hasNext();) {
				Item item =  iterator.next();
				String json = item.toJSONPretty();
				ObjectMapper mapper = new ObjectMapper();
			    JsonNode actualObj = mapper.readTree(json);
			    array.add(actualObj);
			}
			System.out.println("JSON RESPONSE: \n" + array);
			return true;
		}catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}
	
	public static boolean getALL() {
		try {
			final AmazonDynamoDB ddb = Connection.getConnection();
			ScanRequest scanRequest = new ScanRequest().withTableName(Run.TABLE_NAME).withLimit(200);
			ScanResult result = ddb.scan(scanRequest);
			System.out.println("result!" + result);
			/*for (Map<String, AttributeValue> item : result.getItems()) {
				Set<String> keySet = item.keySet();
				for (String key : keySet) {
					AttributeValue value = item.get(key);
					System.out.println("ITEM:::" + key + " ::" + item.get(key));
				}

			}*/
			List<Map<String, AttributeValue>> items = result.getItems();
			List<Item> itemList = ItemUtils.toItemList(items);
			final ArrayNode array = JsonNodeFactory.instance.arrayNode(); 
			for (Iterator <Item>iterator = itemList.iterator(); iterator.hasNext();) {
				Item item =  iterator.next();
				String json = item.toJSONPretty();
				ObjectMapper mapper = new ObjectMapper();
			    JsonNode actualObj = mapper.readTree(json);
			    array.add(actualObj);
			}
			System.out.println("JSON RESPONSE: \n" + array);
			return true;
		}catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}

}
