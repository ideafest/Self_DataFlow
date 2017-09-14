package com.example;

import com.Practice.Basic.Queries;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;

public class ExportAsCSV {
	
	private interface Options extends PipelineOptions{
		@Description("Destination")
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}
	
	public static void main(String[] args) {
		Queries queries = new Queries();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);

		
		pipeline.apply(BigQueryIO.Read.named("Reading table").from(queries.dncList))
				.apply(ParDo.named("Filtering").of(new DoFn<TableRow, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						String campaignid = (String) context.element().get("campaignId");
						if(!(campaignid == null)){
							context.output(context.element());
						}
					}
				}))
				.apply(ParDo.named("Formatter").of(new DoFn<TableRow, KV<String, TableRow>>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						context.output(KV.of((String)context.element().get("campaignId"), context.element()));
					}
				}))
				.apply(ParDo.named("Converter").of(new DoFn<KV<String, TableRow>, String>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						context.output(context.element().getKey() +", "+context.element().getValue().toPrettyString());
					}
				}))
				.apply(TextIO.Write.named("Writing to GCS").to(options.getOutput()).withSuffix(".csv"));
		
		pipeline.run();
	}
}
