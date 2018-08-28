package com.abhi.edu.dynamoDB.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.abhi.edu.dynamoDB.util.JacksonConverter;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;

/**
 * 
 * @author Abhishek Khare
 *
 */
public class GetItem {
	public static void main(String[] args) throws Exception {
		getItem();
	}
	
	public static boolean getItem() {
		String table_name = Run.TABLE_NAME;
		HashMap<String, AttributeValue> key_to_get = new HashMap<String, AttributeValue>();
		key_to_get.put("Artist", new AttributeValue().withS("Kishore Kumar"));
		key_to_get.put("SongTitle", new AttributeValue().withS("Neele Neele Ambar Par"));
		System.out.format("Retrieving item \"%s\" from \"%s\"\n", key_to_get, table_name);

		GetItemRequest request = new GetItemRequest().withKey(key_to_get).withTableName(table_name);
		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
		try {
			Map<String, AttributeValue> returned_item = ddb.getItem(request).getItem();
			if (returned_item != null) {
				Set<String> keys = returned_item.keySet();
				for (String key : keys) {
					System.out.format("%s: %s\n", key, returned_item.get(key).toString());
				}
			} else {
				System.out.format("No item found with the key %s!\n", key_to_get);
			}
			JacksonConverter converter = new JacksonConverter();
			System.out.println("JSON RESPONSE: \n" +converter.mapToJsonObject(returned_item));
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
		return true;
	}
}
