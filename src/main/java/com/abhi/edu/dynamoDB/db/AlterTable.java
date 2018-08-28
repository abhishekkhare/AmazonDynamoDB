package com.abhi.edu.dynamoDB.db;

import java.util.ArrayList;
import java.util.Collection;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;

public class AlterTable {

	public static void main(String[] args) {
		alter();
	}

	public static boolean alter() {
		AmazonDynamoDB ddb = Connection.getConnection();
		UpdateTableRequest updateTableRequest = new UpdateTableRequest();
		Collection<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(
				new AttributeDefinition().withAttributeName("AlbumTitle").withAttributeType(ScalarAttributeType.S));
		attributeDefinitions
				.add(new AttributeDefinition().withAttributeName("DateTime").withAttributeType(ScalarAttributeType.S));
		updateTableRequest.setAttributeDefinitions(attributeDefinitions);
		updateTableRequest.setTableName(Run.TABLE_NAME);

		ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
		provisionedThroughput.setReadCapacityUnits(11L);
		provisionedThroughput.setWriteCapacityUnits(11L);
		updateTableRequest.setProvisionedThroughput(provisionedThroughput);
		ddb.updateTable(updateTableRequest);
		TableDescription table = Connection.getConnection().describeTable(Run.TABLE_NAME).getTable();
		while (!table.getTableStatus().equals("ACTIVE")) {
			table = Connection.getConnection().describeTable(Run.TABLE_NAME).getTable();
			System.out.println("Current Status::" + table.getTableStatus() + " Waiting for "
					+ Run.TABLE_NAME + " to be altered...this may take a while...");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Done");

		return true;
	}

}
