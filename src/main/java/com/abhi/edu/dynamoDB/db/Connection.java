package com.abhi.edu.dynamoDB.db;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

/**
 * 
 * @author Abhishek Khare
 *
 */

public class Connection {
	public static AmazonDynamoDB getConnection(String acessKey, String secretKey) {
		BasicAWSCredentials awsCredentials = new BasicAWSCredentials(acessKey, secretKey);
		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).build();
		return ddb;
	}

	public static AmazonDynamoDB getConnection() {
		BasicAWSCredentials awsCredentials = new BasicAWSCredentials("*****",
				"****");
		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).build();
		return ddb;
	}

}
