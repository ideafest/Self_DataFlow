package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;

public class TimeStampTest {
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	private static class GenerateKV extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String key = (String) context.element().get("campaignid");
			String key1 = (String) context.element().get("prospectcallid");
			String key2 = (String) context.element().get("updateddate");
			context.output(KV.of(key+key1+key2, context.element()));
		}
	}
	
	
	public static void main(String[] args) {
		
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		pipeline.apply(BigQueryIO.Read.from("vantage-167009:Learning.PCI_Temp"))
				.apply(ParDo.of(new GenerateKV()))
				.apply(GroupByKey.create())
				.apply(ParDo.of(new DoFn<KV<String, Iterable<TableRow>>, String>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
					
					}
				}))
				.apply(TextIO.Write.to(options.getOutput()));
		
		
		pipeline.run();
		
	}
	
}
