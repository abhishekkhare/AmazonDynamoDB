package com.abhi.edu.dynamoDB.db;

import java.util.HashMap;
import java.util.Map;

import com.abhi.edu.dynamoDB.util.JacksonConverter;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.fasterxml.jackson.databind.JsonNode;

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
		JacksonConverter converter = new JacksonConverter();
		JsonNode node = converter.itemListToJsonArray(result.getItems());
		System.out.println("JSON RESPONSE: \n" + node);
		return true;
	}
	
	public static boolean getALL() {
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
		JacksonConverter converter = new JacksonConverter();
		JsonNode node = converter.itemListToJsonArray(result.getItems());
		System.out.println("JSON RESPONSE: \n" + node);
		return true;
	}

}
