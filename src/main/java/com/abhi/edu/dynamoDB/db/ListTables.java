package com.abhi.edu.dynamoDB.db;

import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

/**
 * 
 * @author Abhishek Khare
 *
 */

public class ListTables {

	public static void main(String[] args) {
		System.out.println("Your DynamoDB tables");
		AmazonDynamoDB ddb = Connection.getConnection();
		ListTablesRequest request;
		boolean more_tables = true;
		String last_name = null;
		while (more_tables) {
			try {
				if (last_name == null) {
					request = new ListTablesRequest().withLimit(10);
				} else {
					request = new ListTablesRequest().withLimit(10).withExclusiveStartTableName(last_name);
				}

				ListTablesResult table_list = ddb.listTables(request);
				List<String> table_names = table_list.getTableNames();
				if (table_names.size() > 0) {
					for (String cur_name : table_names) {

						DescribeTableResult tableResult = ddb.describeTable(cur_name);
						System.out.format("* %s\n", tableResult.getTable().getTableName());
						List<AttributeDefinition> attributes = tableResult.getTable().getAttributeDefinitions();
						for(AttributeDefinition arttribute: attributes){
							System.out.println("Attribute::" + arttribute.getAttributeName() + " -- " + arttribute.getAttributeType());
						}
						List<KeySchemaElement> keys = tableResult.getTable().getKeySchema();
						for(KeySchemaElement key:keys){
							System.out.println("Key:"+key.getAttributeName() + " -- " + key.getKeyType());
						}
						System.out.println("*********************");

					}
				} else {
					System.out.println("No tables found!");
					System.exit(0);
				}

				last_name = table_list.getLastEvaluatedTableName();
				if (last_name == null) {
					more_tables = false;
				}

			} catch (Exception e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}
		}
		System.out.println("\nDone!");
	}
}
