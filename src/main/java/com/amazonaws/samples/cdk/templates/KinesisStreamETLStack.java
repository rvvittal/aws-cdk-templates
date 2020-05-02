package com.amazonaws.samples.cdk.templates;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awscdk.core.CfnParameter;
import software.amazon.awscdk.core.ConcreteDependable;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Duration;
import software.amazon.awscdk.core.RemovalPolicy;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.dynamodb.CfnTable;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.Policy;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.kinesis.Stream;
import software.amazon.awscdk.services.kinesisanalytics.*;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.FunctionProps;
import software.amazon.awscdk.services.lambda.IEventSource;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.StartingPosition;
import software.amazon.awscdk.services.lambda.eventsources.KinesisEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.dynamodb.TableProps;




public class KinesisStreamETLStack extends Stack {

	public KinesisStreamETLStack(final Construct parent, final String id) {
		this(parent, id, null);
	}

	public KinesisStreamETLStack(final Construct parent, final String id, final StackProps props) {
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
		
		
		
		Bucket.Builder bucketBldr = Bucket.Builder.create(this, "bucketId").removalPolicy(RemovalPolicy.DESTROY);
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
		
		
		
		HashMap<String, PolicyDocument> policyMap = new HashMap<String, PolicyDocument>();
		policyMap.put("kdaPolicy", policyDoc.build());
		
		
		Role kdaOrderRole =
		        Role.Builder.create(this, "kdaOrderRole")
		            .assumedBy(new ServicePrincipal("kinesisanalytics.amazonaws.com"))
		            .inlinePolicies(policyMap)
		            .build();
		
		
		//create kinesis data analytics app
		
		
		
		CfnApplication.InputSchemaProperty.Builder  kdaInputSchema = new CfnApplication.InputSchemaProperty.Builder();
		
		
		CfnApplication.RecordColumnProperty rcpOrderId = new CfnApplication.RecordColumnProperty.Builder().name("orderId").sqlType("INT").mapping("$.orderId").build();
		CfnApplication.RecordColumnProperty rcpItemId = new CfnApplication.RecordColumnProperty.Builder().name("itemId").sqlType("INT").mapping("$.itemId").build();
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
		rcpList.add(rcpOrderAmount);
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
		inputBuilder.namePrefix("SOURCE_SQL_STREAM");
		inputBuilder.kinesisStreamsInput(ksiBuilder.build());
		
		List<Object> inputs = new ArrayList<Object>();
		inputs.add(inputBuilder.build());
		
		CfnApplicationOutput.KinesisStreamsOutputProperty.Builder ksoBuilder = new CfnApplicationOutput.KinesisStreamsOutputProperty.Builder();
		ksoBuilder.roleArn(kdaOrderRole.getRoleArn());
		ksoBuilder.resourceArn(orderEStream.getStreamArn());
		
		
		CfnApplicationOutput.DestinationSchemaProperty.Builder dsp = CfnApplicationOutput.DestinationSchemaProperty.builder();
		dsp.recordFormatType("JSON");
		
		
		CfnApplicationOutput.OutputProperty.Builder outputBuilder = new CfnApplicationOutput.OutputProperty.Builder();
		outputBuilder.kinesisStreamsOutput(ksoBuilder.build());
		outputBuilder.destinationSchema(dsp.build());
		
		
		
		
		CfnApplication appConstruct = CfnApplication.Builder.create(this, "KDA-OrderETLAppId")
		.applicationName("KDA-OrderETL")
		.applicationDescription("ETL for orders")
		.inputs(inputs)
		.build();
		
		
		
		CfnApplicationOutput appOutConstruct = CfnApplicationOutput.Builder.create(this, "KDA-OrderETLAppId2")
		 .applicationName("KDA-OrderETL")
		 .output(outputBuilder.build())
		 .build();
		
		appOutConstruct.addDependsOn(appConstruct);
		
		CfnApplicationReferenceDataSource.S3ReferenceDataSourceProperty.Builder	s3Ref = new CfnApplicationReferenceDataSource.S3ReferenceDataSourceProperty.Builder();	
		s3Ref.bucketArn(bucket.getBucketArn());
		s3Ref.referenceRoleArn(kdaOrderRole.getRoleArn());
		s3Ref.fileKey("products/products.json");
		
		
		CfnApplicationReferenceDataSource.RecordColumnProperty rcpPrdId = new CfnApplicationReferenceDataSource.RecordColumnProperty.Builder().name("productId").sqlType("INT").mapping("$.productId").build();
		CfnApplicationReferenceDataSource.RecordColumnProperty rcpPrdNm = new CfnApplicationReferenceDataSource.RecordColumnProperty.Builder().name("productName").sqlType("VARCHAR(32)").mapping("$.productName").build();
		CfnApplicationReferenceDataSource.RecordColumnProperty rcpPrdPrice = new CfnApplicationReferenceDataSource.RecordColumnProperty.Builder().name("productPrice").sqlType("REAL").mapping("$.productPrice").build();
		
		List<Object> rcpPrdList = new ArrayList<Object>();
		rcpPrdList.add(rcpPrdId);
		rcpPrdList.add(rcpPrdNm);
		rcpPrdList.add(rcpPrdPrice);
		
		CfnApplicationReferenceDataSource.RecordFormatProperty.Builder rfpB = new CfnApplicationReferenceDataSource.RecordFormatProperty.Builder();
		rfpB.recordFormatType("JSON");
		
		CfnApplicationReferenceDataSource.ReferenceSchemaProperty.Builder rfsB = new CfnApplicationReferenceDataSource.ReferenceSchemaProperty.Builder();
		rfsB.recordColumns(rcpPrdList);
		rfsB.recordFormat(rfpB.build());
		
		CfnApplicationReferenceDataSource.ReferenceDataSourceProperty.Builder refDs = new CfnApplicationReferenceDataSource.ReferenceDataSourceProperty.Builder();
		refDs.s3ReferenceDataSource(s3Ref.build());
		refDs.referenceSchema(rfsB.build());
		refDs.tableName("products");
		
		
		CfnApplicationReferenceDataSource.Builder appRefDsb = CfnApplicationReferenceDataSource.Builder.create(this, "s3RefDs");
		appRefDsb.applicationName("KDA-OrderETL");
		appRefDsb.referenceDataSource(refDs.build());
		
		
		CfnApplicationReferenceDataSource appDs = appRefDsb.build();
		appDs.addDependsOn(appConstruct);
		
		
		
		
		
		 // final Bucket bucket2 = new Bucket(this, "OrderEnrichmentSinkStore");
		  Bucket bucket2 = Bucket.Builder.create(this, "OrderEnrichmentSinkStore").removalPolicy(RemovalPolicy.DESTROY).build();

		  
		 
	      
	      TableProps tableProps;
	        Attribute partitionKey = Attribute.builder()
	                .name("orderId")
	                .type(AttributeType.NUMBER)
	                .build();
	        Attribute sortKey = Attribute.builder()
	                .name("itemId")
	                .type(AttributeType.NUMBER)
	                .build();
	        tableProps = TableProps.builder()
	                .tableName("OrderEnriched")
	                .partitionKey(partitionKey)
	                .sortKey(sortKey)
	                // The default removal policy is RETAIN, which means that cdk destroy will not attempt to delete
	                // the new table, and it will remain in your account until manually deleted. By setting the policy to
	                // DESTROY, cdk destroy will delete the table (even if it has data in it)
	                .removalPolicy(RemovalPolicy.DESTROY)
	                .build();
	        
	        Table dynamodbTable = new Table(this, "OrderEnriched", tableProps);
	        
	        
	        Map<String, String> lambdaEnvMap = new HashMap<String, String>();
			  lambdaEnvMap.put("BUCKET", bucket.getBucketName());
			  lambdaEnvMap.put("TABLE_NAME", dynamodbTable.getTableName());
		      lambdaEnvMap.put("PRIMARY_KEY","orderId");
		      lambdaEnvMap.put("SORT_KEY","itemId");
		      
		    
	      
		  List<IEventSource> events= new ArrayList<>();
		  
		  
		  
		  KinesisEventSource.Builder keb =  KinesisEventSource.Builder.create(orderEStream);
		  keb.parallelizationFactor(5);
		  keb.batchSize(500);
		  keb.startingPosition(StartingPosition.LATEST);

		  events.add(keb.build());
		  
		  Function lambdaFunction =
			        Function.Builder.create(this, "OrderEnrichmentSinkHandler")
			            .code(Code.fromAsset("resources"))
			            .handler("order_enrichment_sink.main")
			            .timeout(Duration.seconds(300))
			            .memorySize(512)
			            .events(events)
			            .runtime(Runtime.NODEJS_12_X)
			            .environment(lambdaEnvMap)
			            .build();


	        bucket2.grantReadWrite(lambdaFunction);
	        dynamodbTable.grantReadWriteData(lambdaFunction);
	        
	        
	       
		 
		
	}
	
	
}
