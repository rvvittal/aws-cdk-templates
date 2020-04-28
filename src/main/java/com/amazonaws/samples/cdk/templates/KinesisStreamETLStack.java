package com.amazonaws.samples.cdk.templates;

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

public class KinesisStreamETLStack extends Stack {

	public KinesisStreamETLStack(final Construct parent, final String id) {
		this(parent, id, null);
	}

	public KinesisStreamETLStack(final Construct parent, final String id, final StackProps props) {
		super(parent, id, props);
		
		
		CfnParameter orderStreamName = CfnParameter.Builder.create(this, "orderStreamName")
		        .type("String")
		        .description("The name of the kinesis order stream")
		        .build();
		
		CfnParameter orderStreamShards = CfnParameter.Builder.create(this, "orderStreamShards")
		        .type("Number")
		        .description("Number of shards for kinesis order stream")
		        .build();
		
		CfnParameter orderEStreamName = CfnParameter.Builder.create(this, "orderEnrichedStreamName")
		        .type("String")
		        .description("The name of the kinesis order enriched stream")
		        .build();
		
		CfnParameter orderEStreamShards = CfnParameter.Builder.create(this, "orderEnrichedStreamShards")
		        .type("Number")
		        .description("Number of shards for kinesis order enriched stream")
		        .build();
		
		
		
		
		//policy.
		
		//create kinesis streams

		Stream orderStream = Stream.Builder.create(this, "OrderStreamId").streamName(orderStreamName.getValueAsString()).shardCount(orderStreamShards.getValueAsNumber()).build();
		Stream.Builder.create(this, "OrderEnrichedStreamId").streamName(orderEStreamName.getValueAsString()).shardCount(orderStreamShards.getValueAsNumber()).build();
		
		
		
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
		
		
		
		CfnApplication.InputSchemaProperty.Builder  kdaInputSchema = new CfnApplication.InputSchemaProperty.Builder();
		
		
		CfnApplication.RecordColumnProperty rcpOrderId = new CfnApplication.RecordColumnProperty.Builder().name("orderId").sqlType("INT").mapping("$.orderId").build();
		CfnApplication.RecordColumnProperty rcpItemId = new CfnApplication.RecordColumnProperty.Builder().name("itemId").sqlType("INT").mapping("$.orderId").build();
		CfnApplication.RecordColumnProperty rcpItemQty = new CfnApplication.RecordColumnProperty.Builder().name("itemQuantity").sqlType("INT").mapping("$.itemQuantity").build();
		CfnApplication.RecordColumnProperty rcpItemAmt = new CfnApplication.RecordColumnProperty.Builder().name("itemAmount").sqlType("REAL").mapping("$.itemAmount").build();
		CfnApplication.RecordColumnProperty rcpItemStatus = new CfnApplication.RecordColumnProperty.Builder().name("itemStatus").sqlType("VARCHAR(8)").mapping("$.itemStatus").build();
		CfnApplication.RecordColumnProperty rcpOrderDtTm = new CfnApplication.RecordColumnProperty.Builder().name("orderDateTime").sqlType("TIMESTAMP").mapping("$.orderDateTime").build();
		
		CfnApplication.RecordColumnProperty rcpRecordType = new CfnApplication.RecordColumnProperty.Builder().name("recordType").sqlType("VARCHAR(16)").mapping("$.recordType").build();
		CfnApplication.RecordColumnProperty rcpOrderAmount = new CfnApplication.RecordColumnProperty.Builder().name("orderAmount").sqlType("DOUBLE").mapping("$.orderAmount").build();
		CfnApplication.RecordColumnProperty rcpOrderStatus = new CfnApplication.RecordColumnProperty.Builder().name("orderStatus").sqlType("VARCHAR(8)").mapping("$.orderStatus").build();
		CfnApplication.RecordColumnProperty rcpShipToName = new CfnApplication.RecordColumnProperty.Builder().name("shipToName").sqlType("VARCHAR(32)").mapping("$.shipToName").build();
		
		CfnApplication.RecordColumnProperty rcpShipToAddress = new CfnApplication.RecordColumnProperty.Builder().name("shipToAddress").sqlType("VARCHAR(32)").mapping("$.shipToAddress").build();
		CfnApplication.RecordColumnProperty rcpShipToCity = new CfnApplication.RecordColumnProperty.Builder().name("shipToCity").sqlType("VARCHAR(32)").mapping("$.shipToCity").build();
		CfnApplication.RecordColumnProperty rcpShipToState = new CfnApplication.RecordColumnProperty.Builder().name("shipToState").sqlType("VARCHAR(16)").mapping("$.shipToState").build();
		CfnApplication.RecordColumnProperty rcpShipToZip = new CfnApplication.RecordColumnProperty.Builder().name("shipToZip").sqlType("VARCHAR(16)").mapping("$.shipToZip").build();
		
		
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
		
		CfnApplication.KinesisStreamsInputProperty.Builder ksiBuilder = new CfnApplication.KinesisStreamsInputProperty.Builder();
		//ksiBuilder.roleArn("arn:aws:iam::716664005094:role/service-role/kinesis-analytics-KDA-OrderProcess-us-east-1");
		ksiBuilder.roleArn(kdaOrderRole.getRoleArn());
		ksiBuilder.resourceArn(orderStream.getStreamArn());
		
		CfnApplication.RecordFormatProperty.Builder rfBuilder = new CfnApplication.RecordFormatProperty.Builder();
		rfBuilder.recordFormatType("JSON");
		
		CfnApplication.MappingParametersProperty.Builder mpBuilder = new CfnApplication.MappingParametersProperty.Builder();
		//mpBuilder.
		
		CfnApplication.JSONMappingParametersProperty.Builder jmpBuilder = new CfnApplication.JSONMappingParametersProperty.Builder();
		jmpBuilder.recordRowPath("$");
		
		mpBuilder.jsonMappingParameters(jmpBuilder.build());
		
		kdaInputSchema.recordFormat(rfBuilder.build());

		

		CfnApplication.InputProperty.Builder inputBuilder = new CfnApplication.InputProperty.Builder();
		
		inputBuilder.inputSchema(kdaInputSchema.build());
		inputBuilder.namePrefix("ord");
		inputBuilder.kinesisStreamsInput(ksiBuilder.build());
		
		List<Object> inputs = new ArrayList<Object>();
		inputs.add(inputBuilder.build());
		
		
		CfnApplication.Builder.create(this, "KDA-OrderETLAppId")
		.applicationName("KDA-OrderETL")
		.applicationDescription("ETL for orders")
		.inputs(inputs)
		.build();
		
	}

}
