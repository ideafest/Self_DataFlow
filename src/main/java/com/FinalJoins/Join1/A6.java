package com.FinalJoins.Join1;

import com.Essential.Joins;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class A6 {
	
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
	
	private static class ExtractFromF1 extends DoFn<TableRow, KV<String, TableRow>>{
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
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		Joins joins = new Joins();
		
		A5 a5 = new A5();
		F1 f1 = new F1();
		
		PCollection<KV<String, TableRow>> a5PCollection = a5.runIt(pipeline)
				.apply(ParDo.of(new ExtractFromA5()));
		
		PCollection<KV<String, TableRow>> f1PCollection = f1.runIt(pipeline)
				.apply(ParDo.of(new ExtractFromF1()));
		
//		PCollection<TableRow> resultPCollection = joins.leftOuterJoin(a5PCollection, f1PCollection,)
		
	}
	
}
