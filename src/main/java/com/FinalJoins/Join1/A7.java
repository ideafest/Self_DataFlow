package com.FinalJoins.Join1;

import com.Essential.JobOptions;
import com.Essential.Joins;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class A7 {
	
	private static class ExtractFromEntityAgent extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("_id");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromEntityQa extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("_id");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromEntityProspect extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("prospectcallid");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromStateMapping extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("statecode");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromNewEntityCampaign extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("campaignid");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromCampaignAssets extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("assetid");
			context.output(KV.of(key, element));
		}
	}
	
	public static void main(String[] args) {
		
		JobOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(JobOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		Joins joins = new Joins();

		Init init = new Init();
		init.initTables(pipeline);
		init.initJoins(init);
		
		A6 a6 = new A6();
		
//		PCollection<
		
	}
	
}
