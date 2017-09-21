package com.FinalJoins.Join1;

import com.Essential.JobOptions;
import com.Essential.Joins;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class A6 {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	private static class ExtractFromA5 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			
			String key1 = (String) element.get("A_campaignId");
			String key2 = (String) element.get("A_prospectCallId");
			String key3 = (String) element.get("A_prospectSessionInteractionId");
			String key4 = (String) element.get("A_dispositionStatus");
			String finalKey = key1 + key2 + key3 + key4;
			
			context.output(KV.of(finalKey, element));
		}
	}
	
	private static class ExtractFromB4 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key1 = (String) element.get("D_code");
			context.output(KV.of(key1, element));
		}
	}
	
	private static class ExtractFromTemp1PCollection extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key1;
			if( element.get("A_status") != null) {
				key1 = (String) element.get("A_status");
				context.output(KV.of(key1, element));
			}
			
		}
	}
	
	private static class ExtractFromTemp3PCollection extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key1;
			if( element.get("A_dispositionStatus") != null) {
				key1 = (String) element.get("A_dispositionStatus");
				context.output(KV.of(key1, element));
			}
			
		}
	}
	
	private static class ExtractFromTemp5PCollection extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key1;
			if(element.get("A_subStatus") != null) {
				key1 = (String) element.get("A_subStatus");
				context.output(KV.of(key1, element));
			}
			
		}
	}
	
	private static class ExtractFromG1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			
			String key1 = (String) element.get("campaignId");
			String key2 = (String) element.get("prospectCallId");
			String key3 = (String) element.get("prospectSessionInteractionId");
			String key4 = (String) element.get("dispositionStatus");
			String finalKey = key1 + key2 + key3 + key4;
			
			context.output(KV.of(finalKey, element));
		}
	}
	
	private static class ExtractFromMasterStatus extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("code");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromMasterDispositionStatus extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("code");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromMasterSubStatus extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("code");
			context.output(KV.of(key, element));
		}
	}
	
	private static class FormatMainQuery extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			TableRow freshRow = new TableRow();
			
			freshRow.set("campaignId", element.get("A_campaignId"));
			freshRow.set("campaign", element.get("A_campaign"));
			
		}
	}
	
	public PCollection<TableRow> runIt(Init init){
		Joins joins = new Joins();
		A5 a5 = new A5();
		G1 g1 = new G1();
		
		PCollection<KV<String, TableRow>> a5PCollection = a5.runIt(init)
				.apply(ParDo.of(new ExtractFromA5()));
		
		PCollection<KV<String, TableRow>> g1PCollection = g1.runIt(init)
				.apply(ParDo.of(new ExtractFromG1()));
		
		PCollection<TableRow> temp1PCollection = joins.leftOuterJoin2(a5PCollection, g1PCollection,"C_",
				"JoiningJoins");
		
		PCollection<KV<String, TableRow>> masterStatusPCollection = init.getMaster_status()
				.apply(ParDo.of(new ExtractFromMasterStatus()));
		
		PCollection<KV<String, TableRow>> masterDispositionStatusPCollection = init.getMaster_dispositionstatus()
				.apply(ParDo.of(new ExtractFromMasterDispositionStatus()));
		
		PCollection<KV<String, TableRow>> masterSubStatusPCollection = init.getMaster_substatus()
				.apply(ParDo.of(new ExtractFromMasterSubStatus()));
		
		PCollection<KV<String, TableRow>> temp2PCollection = temp1PCollection.apply(ParDo.of(new ExtractFromTemp1PCollection()));
		
		PCollection<TableRow> temp3PCollection = joins.leftOuterJoin2(temp2PCollection, masterStatusPCollection, "D_",
				"JoiningMasterStatus");
		
		PCollection<KV<String, TableRow>> temp4PCollection = temp3PCollection.apply(ParDo.of(new ExtractFromTemp3PCollection()));
		
		PCollection<TableRow> temp5PCollection = joins.leftOuterJoin2(temp4PCollection, masterDispositionStatusPCollection, "E_",
				"JoiningMasterDispositionStatus");
		
		PCollection<KV<String, TableRow>> temp6PCollection = temp5PCollection.apply(ParDo.of(new ExtractFromTemp5PCollection()));
		
		PCollection<TableRow> resultPCollection = joins.leftOuterJoin2(temp6PCollection, masterSubStatusPCollection, "F",
				"JoiningMasterSubStatus");
		
		return resultPCollection;
	}
	
	public static void main(String[] args) {


		JobOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(JobOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		Joins joins = new Joins();

		Init init = new Init();
		init.initTables(pipeline);
		init.initJoins(init);

		A5 a5 = new A5();
		G1 g1 = new G1();

		PCollection<KV<String, TableRow>> a5PCollection = a5.runIt(init)
				.apply(ParDo.of(new ExtractFromA5()));

		PCollection<KV<String, TableRow>> g1PCollection = g1.runIt(init)
				.apply(ParDo.of(new ExtractFromG1()));

		PCollection<TableRow> temp1PCollection = joins.leftOuterJoin2(a5PCollection, g1PCollection,"C_",
				"JoiningJoins");

//		PCollection<KV<String, TableRow>> masterStatusPCollection = init.getMaster_status()
//				.apply(ParDo.of(new ExtractFromMasterStatus()));
//
//		PCollection<KV<String, TableRow>> masterDispositionStatusPCollection = init.getMaster_dispositionstatus()
//				.apply(ParDo.of(new ExtractFromMasterDispositionStatus()));
//
//		PCollection<KV<String, TableRow>> masterSubStatusPCollection = init.getMaster_substatus()
//				.apply(ParDo.of(new ExtractFromMasterSubStatus()));
//
//		PCollection<KV<String, TableRow>> temp2PCollection = temp1PCollection.apply(ParDo.of(new ExtractFromTemp1PCollection()));
//
//		PCollection<TableRow> temp3PCollection = joins.leftOuterJoin2(temp2PCollection, masterStatusPCollection, "D_",
//				"JoiningMasterStatus");
//
//		PCollection<KV<String, TableRow>> temp4PCollection = temp3PCollection.apply(ParDo.of(new ExtractFromTemp3PCollection()));
//
//		PCollection<TableRow> temp5PCollection = joins.leftOuterJoin2(temp4PCollection, masterDispositionStatusPCollection, "E_",
//				"JoiningMasterDispositionStatus");
//
//		PCollection<KV<String, TableRow>> temp6PCollection = temp5PCollection.apply(ParDo.of(new ExtractFromTemp5PCollection()));
//
//		PCollection<TableRow> resultPCollection = joins.leftOuterJoin2(temp6PCollection, masterSubStatusPCollection, "F",
//				"JoiningMasterSubStatus");
		
		temp1PCollection.apply(ParDo.of(new ConvertToString()))
				.apply(TextIO.Write.named("Writer").to(options.getOutput()));

		pipeline.run();
	}

}
