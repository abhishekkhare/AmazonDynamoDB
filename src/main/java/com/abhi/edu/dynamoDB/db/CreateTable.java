package com.abhi.edu.dynamoDB.db;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * 
 * @author Abhishek Khare
 *
 */
public class CreateTable {

	public static void main(String[] args) {
		create();
	}

	public static boolean create() {
		AmazonDynamoDB ddb = Connection.getConnection();
		List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
		keySchema.add(new KeySchemaElement().withAttributeName("Artist").withKeyType(KeyType.HASH));
		keySchema.add(new KeySchemaElement().withAttributeName("SongTitle").withKeyType(KeyType.RANGE));

		List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions
				.add(new AttributeDefinition().withAttributeName("Artist").withAttributeType(ScalarAttributeType.S));
		attributeDefinitions
				.add(new AttributeDefinition().withAttributeName("SongTitle").withAttributeType(ScalarAttributeType.S));

		ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
		provisionedThroughput.setReadCapacityUnits(10L);
		provisionedThroughput.setWriteCapacityUnits(10L);

		CreateTableRequest createTableRequest = new CreateTableRequest(attributeDefinitions, Run.TABLE_NAME, keySchema,
				provisionedThroughput);
		CreateTableResult result = ddb.createTable(createTableRequest);

		
		System.out.println(result.getTableDescription().getTableName());
		TableDescription table = Connection.getConnection().describeTable(Run.TABLE_NAME).getTable();
		while (!table.getTableStatus().equals("ACTIVE")) {
			table = Connection.getConnection().describeTable(Run.TABLE_NAME).getTable();
			System.out.println("Current Status::" + table.getTableStatus() + " Waiting for " + Run.TABLE_NAME
					+ " to be created...this may take a while...");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("DONE");
		return true;
	}

}
