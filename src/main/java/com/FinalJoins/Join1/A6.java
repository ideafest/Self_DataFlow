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
			String key1 = (String) element.get("A_status");
			context.output(KV.of(key1, element));
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

	
	
	public static void main(String[] args) {
		
		
		JobOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(JobOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		Joins joins = new Joins();
		
		Init init = new Init();
		init.initTables(pipeline);
		
		A5 a5 = new A5();
		G1 g1 = new G1();
		
		PCollection<KV<String, TableRow>> a5PCollection = a5.runIt(init)
				.apply(ParDo.of(new ExtractFromA5()));
		
		PCollection<KV<String, TableRow>> g1PCollection = g1.runIt(init)
				.apply(ParDo.of(new ExtractFromG1()));
		
		PCollection<TableRow> temp1PCollection = joins.leftOuterJoin2(a5PCollection, g1PCollection,"C_");
		
		
		
		B4 b4 = new B4();
		PCollection<KV<String, TableRow>> b4PCollection = b4.runIt(init)
				.apply(ParDo.of(new ExtractFromB4()));
		
		PCollection<KV<String, TableRow>> temp2PCollection = temp1PCollection.apply(ParDo.of(new ExtractFromTemp1PCollection()));
		
		PCollection<TableRow> resultPCollection = joins.leftOuterJoin(temp2PCollection, b4PCollection);
		
		resultPCollection.apply(ParDo.of(new ConvertToString()))
				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
	}
	
}
