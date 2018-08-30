package com.abhi.edu.dynamoDB.db;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;

/**
 * 
 * @author Abhishek Khare
 *
 */
public class PutItem {
	public static void main(String[] args) {
		put();
		putJSON("{\"Artist\": \"Anuradha Paudwal\",\"SongTitle\": \"Tere Naam Liya\",\"AlbumTitle\": \"Ram Lakhan\",\"DateTime\": \""+new Date().toString()+"\"}");
	}

	public static void putJSON(String json) {
		final AmazonDynamoDB ddb = Connection.getConnection();
		String table_name = Run.TABLE_NAME;
		Map<String, AttributeValue> item_values;
		try {
			Item item = Item.fromJSON(json);
			item_values = ItemUtils.toAttributeValues(item);
			ddb.putItem(table_name, item_values);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static boolean put() {
		String table_name = Run.TABLE_NAME;
		System.out.format("Adding \"%s\" to \"%s\"", "name", table_name);
		add(table_name, "S.P. BalaSubramanyam", "Aate Jaate Haste Gaate", "Maine Pyar Kiya");
		add(table_name, "Udit Naraya", "Bin Tere Sanam", "Yaara Dildara");
		add(table_name, "Kishore Kumar", "Neele Neele Ambar Par", "Kalaakaar");
		add(table_name, "Shabbir Kumar", "Jab ham jawan honge", "Betaab");
		add(table_name, "Kishore Kumar", "Shayad Meri Shaadi Ka Khayal ", "Souten");
		return true;

	}

	private static void add(String table_name, String artist, String songTitle, String albumTitle) {
		HashMap<String, AttributeValue> item_values = new HashMap<String, AttributeValue>();

		item_values.put("Artist", new AttributeValue(artist));
		item_values.put("SongTitle", new AttributeValue(songTitle));
		item_values.put("AlbumTitle", new AttributeValue(albumTitle));
		item_values.put("DateTime", new AttributeValue(new Date().toString()));

		final AmazonDynamoDB ddb = Connection.getConnection();

		try {
			PutItemResult result = ddb.putItem(table_name, item_values);
			System.out.println("result!" + result);
		} catch (ResourceNotFoundException e) {
			System.err.format("Error: The table \"%s\" can't be found.\n", table_name);
			System.err.println("Be sure that it exists and that you've typed its name correctly!");
			System.exit(1);
		} catch (AmazonServiceException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
