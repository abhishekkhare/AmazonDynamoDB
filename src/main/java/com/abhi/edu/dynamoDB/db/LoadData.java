package com.abhi.edu.dynamoDB.db;

import java.io.File;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 
 * @author Abhishek Khare
 *
 */
public class LoadData {
	public static void main(String[] args) throws Exception {
		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();

		DynamoDB dynamoDB = new DynamoDB(ddb);
		Table table = dynamoDB.getTable("Movies");
		JsonParser parser = new JsonFactory()
				.createParser(new File("/Users/khaab05/palooza/dynamodb/moviedata.json"));

		JsonNode rootNode = new ObjectMapper().readTree(parser);
		Iterator<JsonNode> iter = rootNode.iterator();
		ObjectNode currentNode;

		while (iter.hasNext()) {
			currentNode = (ObjectNode) iter.next();
			int year = currentNode.path("year").asInt();
			String title = currentNode.path("title").asText();

			try {
				table.putItem(new Item()
						.withPrimaryKey("year", year, "title", title)
						.withJSON("info", currentNode.path("info").toString()));
				System.out.println("Successful load: " + year + " " + title);
			} catch (Exception e) {
				System.err.println("Cannot add product: " + year + " " + title);
				System.err.println(e.getMessage());
				break;
			}
		}
		parser.close();
		System.out.println("Done");
	}
} 
