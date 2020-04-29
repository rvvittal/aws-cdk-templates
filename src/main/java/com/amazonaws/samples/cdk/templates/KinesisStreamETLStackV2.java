package com.amazonaws.samples.cdk.templates;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import software.amazon.awscdk.core.CfnParameter;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.Policy;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.kinesis.Stream;
import software.amazon.awscdk.services.kinesisanalytics.*;
import software.amazon.awscdk.services.kinesisanalytics.CfnApplicationV2.SqlApplicationConfigurationProperty;
import software.amazon.awscdk.services.s3.Bucket;


public class KinesisStreamETLStackV2 extends Stack {

	public KinesisStreamETLStackV2(final Construct parent, final String id) {
		this(parent, id, null);
	}

	public KinesisStreamETLStackV2(final Construct parent, final String id, final StackProps props) {
		super(parent, id, props);
		
		
		CfnParameter orderStreamName = CfnParameter.Builder.create(this, "orderStreamName")
		        .type("String")
		        .description("The name of the kinesis order stream").defaultValue("OrderStream")
		        .build();
		
		CfnParameter orderStreamShards = CfnParameter.Builder.create(this, "orderStreamShards")
		        .type("Number")
		        .description("Number of shards for kinesis order stream").defaultValue(2)
		        .build();
		
		CfnParameter orderEStreamName = CfnParameter.Builder.create(this, "orderEnrichedStreamName")
		        .type("String")
		        .description("The name of the kinesis order enriched stream").defaultValue("OrderEnrichedStream")
		        .build();
		
		CfnParameter orderEStreamShards = CfnParameter.Builder.create(this, "orderEnrichedStreamShards")
		        .type("Number")
		        .description("Number of shards for kinesis order enriched stream").defaultValue(2)
		        .build();
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-hh-mm-ss");
		 
		
		CfnParameter productsBucket = CfnParameter.Builder.create(this, "productsBucket")
		        .type("String")
		        .description("S3 Products Bucket Name").defaultValue("kinesis-stream-etl-" +LocalDateTime.now().format(formatter))
		        .build();
		
		
		
		Bucket.Builder bucketBldr = Bucket.Builder.create(this, "bucketId");
		Bucket bucket = bucketBldr.bucketName(productsBucket.getValueAsString()).build();
		

		
		//create kinesis streams

		Stream orderStream = Stream.Builder.create(this, "OrderStreamId").streamName(orderStreamName.getValueAsString()).shardCount(orderStreamShards.getValueAsNumber()).build();
		Stream orderEStream = Stream.Builder.create(this, "OrderEnrichedStreamId").streamName(orderEStreamName.getValueAsString()).shardCount(orderEStreamShards.getValueAsNumber()).build();
		
		
		
		//setup iam role and policies
		
		List<String> actionList = new ArrayList<String>();
		actionList.add("kinesis:DescribeStream");
		actionList.add("kinesis:GetShardIterator");
		actionList.add("kinesis:GetRecords");
		
		List<String> resourceList = new ArrayList<String>();
		resourceList.add(orderStream.getStreamArn());
		
		PolicyStatement.Builder policyStmt = PolicyStatement.Builder.create();
		policyStmt.sid("ReadKinesisInput");
		policyStmt.effect(Effect.ALLOW);
		policyStmt.actions(actionList);
		policyStmt.resources(resourceList);
		
		List<PolicyStatement> policyStmtList = new ArrayList<PolicyStatement>();
		
		policyStmtList.add(policyStmt.build());
		
		actionList = new ArrayList<String>();
		actionList.add("kinesis:DescribeStream");
		actionList.add("kinesis:PutRecord");
		actionList.add("kinesis:PutRecords");
		
		resourceList = new ArrayList<String>();
		resourceList.add(orderEStream.getStreamArn());
		
		policyStmt = PolicyStatement.Builder.create();
		policyStmt.sid("WriteOutputKinesis");
		policyStmt.effect(Effect.ALLOW);
		policyStmt.actions(actionList);
		policyStmt.resources(resourceList);
		
		policyStmtList.add(policyStmt.build());
		
		//build S3 bucket access
		
		actionList = new ArrayList<String>();
		actionList.add("s3:GetObject");
		
		resourceList = new ArrayList<String>();
		resourceList.add(bucket.getBucketArn() +"/products/products.json");
		
		policyStmt = PolicyStatement.Builder.create();
		policyStmt.sid("ReadS3ReferenceData");
		policyStmt.effect(Effect.ALLOW);
		policyStmt.actions(actionList);
		policyStmt.resources(resourceList);
		
		policyStmtList.add(policyStmt.build());
		
		
		
		PolicyDocument.Builder policyDoc = PolicyDocument.Builder.create();
		policyDoc.statements(policyStmtList);
		
		/*
		Policy.Builder policy = Policy.Builder.create(this,"kdaOrderETLPolicy");
		policy.policyName("KDAOrderETLPolicy");
		policy.statements(policyStmtList);
		*/
		
		HashMap<String, PolicyDocument> policyMap = new HashMap<String, PolicyDocument>();
		policyMap.put("kdaPolicy", policyDoc.build());
		
		//("kdaPolicy", policyDoc.build());
		
		Role kdaOrderRole =
		        Role.Builder.create(this, "kdaOrderRole")
		            .assumedBy(new ServicePrincipal("kinesisanalytics.amazonaws.com"))
		            .inlinePolicies(policyMap)
		            .build();
		
		
		//create kinesis data analytics app
		
		
		
		CfnApplicationV2.InputSchemaProperty.Builder  kdaInputSchema = new CfnApplicationV2.InputSchemaProperty.Builder();
		
		
		CfnApplicationV2.RecordColumnProperty rcpOrderId = new CfnApplicationV2.RecordColumnProperty.Builder().name("orderId").sqlType("INT").mapping("$.orderId").build();
		CfnApplicationV2.RecordColumnProperty rcpItemId = new CfnApplicationV2.RecordColumnProperty.Builder().name("itemId").sqlType("INT").mapping("$.orderId").build();
		CfnApplicationV2.RecordColumnProperty rcpItemQty = new CfnApplicationV2.RecordColumnProperty.Builder().name("itemQuantity").sqlType("INT").mapping("$.itemQuantity").build();
		CfnApplicationV2.RecordColumnProperty rcpItemAmt = new CfnApplicationV2.RecordColumnProperty.Builder().name("itemAmount").sqlType("REAL").mapping("$.itemAmount").build();
		CfnApplicationV2.RecordColumnProperty rcpItemStatus = new CfnApplicationV2.RecordColumnProperty.Builder().name("itemStatus").sqlType("VARCHAR(8)").mapping("$.itemStatus").build();
		CfnApplicationV2.RecordColumnProperty rcpOrderDtTm = new CfnApplicationV2.RecordColumnProperty.Builder().name("orderDateTime").sqlType("TIMESTAMP").mapping("$.orderDateTime").build();
		
		CfnApplicationV2.RecordColumnProperty rcpRecordType = new CfnApplicationV2.RecordColumnProperty.Builder().name("recordType").sqlType("VARCHAR(16)").mapping("$.recordType").build();
		CfnApplicationV2.RecordColumnProperty rcpOrderAmount = new CfnApplicationV2.RecordColumnProperty.Builder().name("orderAmount").sqlType("DOUBLE").mapping("$.orderAmount").build();
		CfnApplicationV2.RecordColumnProperty rcpOrderStatus = new CfnApplicationV2.RecordColumnProperty.Builder().name("orderStatus").sqlType("VARCHAR(8)").mapping("$.orderStatus").build();
		CfnApplicationV2.RecordColumnProperty rcpShipToName = new CfnApplicationV2.RecordColumnProperty.Builder().name("shipToName").sqlType("VARCHAR(32)").mapping("$.shipToName").build();
		
		CfnApplicationV2.RecordColumnProperty rcpShipToAddress = new CfnApplicationV2.RecordColumnProperty.Builder().name("shipToAddress").sqlType("VARCHAR(32)").mapping("$.shipToAddress").build();
		CfnApplicationV2.RecordColumnProperty rcpShipToCity = new CfnApplicationV2.RecordColumnProperty.Builder().name("shipToCity").sqlType("VARCHAR(32)").mapping("$.shipToCity").build();
		CfnApplicationV2.RecordColumnProperty rcpShipToState = new CfnApplicationV2.RecordColumnProperty.Builder().name("shipToState").sqlType("VARCHAR(16)").mapping("$.shipToState").build();
		CfnApplicationV2.RecordColumnProperty rcpShipToZip = new CfnApplicationV2.RecordColumnProperty.Builder().name("shipToZip").sqlType("VARCHAR(16)").mapping("$.shipToZip").build();
		
		
		List<Object> rcpList = new ArrayList<Object>();
		
		rcpList.add(rcpOrderId);
		rcpList.add(rcpItemId);
		rcpList.add(rcpItemQty);
		rcpList.add(rcpItemAmt);
		rcpList.add(rcpItemStatus);
		rcpList.add(rcpOrderDtTm);
		rcpList.add(rcpRecordType);
		rcpList.add(rcpOrderStatus);
		rcpList.add(rcpShipToName);
		rcpList.add(rcpShipToAddress);
		rcpList.add(rcpShipToCity);
		rcpList.add(rcpShipToState);
		rcpList.add(rcpShipToZip);
		
		
		kdaInputSchema.recordColumns(rcpList);
		
		CfnApplicationV2.KinesisStreamsInputProperty.Builder ksiBuilder = new CfnApplicationV2.KinesisStreamsInputProperty.Builder();
		//ksiBuilder.roleArn("arn:aws:iam::716664005094:role/service-role/kinesis-analytics-KDA-OrderProcess-us-east-1");
		//ksiBuilder.roleArn(kdaOrderRole.getRoleArn());
		ksiBuilder.resourceArn(orderStream.getStreamArn());
		
		CfnApplicationV2.RecordFormatProperty.Builder rfBuilder = new CfnApplicationV2.RecordFormatProperty.Builder();
		rfBuilder.recordFormatType("JSON");
		
		CfnApplicationV2.MappingParametersProperty.Builder mpBuilder = new CfnApplicationV2.MappingParametersProperty.Builder();
		//mpBuilder.
		
		CfnApplicationV2.JSONMappingParametersProperty.Builder jmpBuilder = new CfnApplicationV2.JSONMappingParametersProperty.Builder();
		jmpBuilder.recordRowPath("$");
		
		mpBuilder.jsonMappingParameters(jmpBuilder.build());
		
		kdaInputSchema.recordFormat(rfBuilder.build());
		
		
		
			

		CfnApplicationV2.InputProperty.Builder inputBuilder = new CfnApplicationV2.InputProperty.Builder();
		
		inputBuilder.inputSchema(kdaInputSchema.build());
		inputBuilder.namePrefix("ord");
		inputBuilder.kinesisStreamsInput(ksiBuilder.build());
		
		List<Object> inputs = new ArrayList<Object>();
		inputs.add(inputBuilder.build());
		
		
		
		CfnApplicationV2.CodeContentProperty.Builder ccBldr = new CfnApplicationV2.CodeContentProperty.Builder();	
		ccBldr.textContent("CREATE OR REPLACE STREAM \"ORDERSTREAM\"(\n" + 
				"   \"orderId\" INTEGER, \n" + 
				"   \"orderAmount\" DECIMAL(5,2),\n" + 
				"   \"orderStatus\" VARCHAR(8),\n" + 
				"   \"orderDateTime\" TIMESTAMP,\n" + 
				"   \"shipToName\" VARCHAR(32),\n" + 
				"   \"shipToAddress\" VARCHAR(32),\n" + 
				"   \"shipToCity\" VARCHAR(32),\n" + 
				"   \"shipToState\" VARCHAR(16),\n" + 
				"   \"shipToZip\" VARCHAR(16),\n" + 
				"   \"recordType\" VARCHAR(16));");
		
		CfnApplicationV2.ApplicationCodeConfigurationProperty.Builder appcodeBldr = new CfnApplicationV2.ApplicationCodeConfigurationProperty.Builder();
		appcodeBldr.codeContent(ccBldr.build());
		appcodeBldr.codeContentType("PLAINTEXT");
		
		CfnApplicationV2.SqlApplicationConfigurationProperty.Builder sqlBldr = new CfnApplicationV2.SqlApplicationConfigurationProperty.Builder();
		sqlBldr.inputs(inputs);
		
		CfnApplicationV2.ApplicationConfigurationProperty.Builder appConfBldr = new CfnApplicationV2.ApplicationConfigurationProperty.Builder();
		appConfBldr.sqlApplicationConfiguration(sqlBldr.build());
		appConfBldr.applicationCodeConfiguration(appcodeBldr.build());
		
		
		
		CfnApplicationV2.Builder.create(this, "KDA-OrderETLAppId")
		.applicationName("KDA-OrderETL")
		.applicationDescription("ETL for orders")
		.applicationConfiguration(appConfBldr.build())
		.serviceExecutionRole(kdaOrderRole.getRoleArn())
		.runtimeEnvironment("SQL-1_0")
		.build();
		
	}

}
