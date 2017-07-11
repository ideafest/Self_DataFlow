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
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class BQJoin {
	
	// A 1000-row sample of the GDELT data here: gdelt-bq:full.events.
	private static final String source_1 = "clouddataflow-readonly:samples.gdelt_sample";
	// A table that maps country codes to country names.
	private static final String source_2 = "gdelt-bq:full.crosswalk_geocountrycodetohuman";
	
	static class ExtractFromSource_1 extends DoFn<TableRow, KV<String, String>>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String countryCode = (String) row.get("ActionGeo_CountryCode");
			String sqlDate = (String) row.get("SQLDATE");
			String actor1Name = (String) row.get("Actor1Name");
			String sourceURL = (String) row.get("SOURCEURL");
			String eventInfo = "Date: "+sqlDate+", Actor1: "+actor1Name+", url: "+sourceURL;
			context.output(KV.of(countryCode, eventInfo));
		}
	}
	
	static class ExtractFromSource_2 extends DoFn<TableRow, KV<String, String>>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String countryCode = (String) row.get("FIPSCC");
			String countryName = (String) row.get("HumanName");
			context.output(KV.of(countryCode, countryName));
		}
	}
	
	static PCollection<String> joinTables(PCollection<TableRow> source1, PCollection<TableRow> source2){
		final TupleTag<String> source1Tag = new TupleTag<>();
		final TupleTag<String> source2Tag = new TupleTag<>();
		
		PCollection<KV<String, String>> source1Info = source1.apply(ParDo.of(new ExtractFromSource_1()));
		PCollection<KV<String, String>> source2Info = source2.apply(ParDo.of(new ExtractFromSource_2()));
		
		PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple
				.of(source1Tag, source1Info)
				.and(source2Tag, source2Info)
				.apply(CoGroupByKey.<String>create());
		
		PCollection<KV<String, String>> finalResult = kvpCollection
				.apply(ParDo.named("Process").of(
						new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
							@Override
							public void processElement(ProcessContext context) throws Exception {
								KV<String, CoGbkResult> element = context.element();
								String countryCode = element.getKey();
								String countryName = "none";
								countryName = element.getValue().getOnly(source2Tag);
								for(String source1info : context.element().getValue().getAll(source1Tag)){
									context.output(KV.of(countryCode, "Country name: " +countryName +", Event info: "+source1info));
								}
							}
						}
				));
		
		PCollection<String> formattedResult = finalResult
				.apply(ParDo.named("Format").of(new DoFn<KV<String,String>, String>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						String output = "Country code: "+context.element().getKey() + ", "+context.element().getValue();
						context.output(output);
					}
				}));
		
		return formattedResult;
	}
	
	private interface Options extends PipelineOptions{
		@Description("Path of file to write to.")
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}
	
	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<TableRow> table1 = pipeline.apply(BigQueryIO.Read.from(source_1));
		PCollection<TableRow> table2 = pipeline.apply(BigQueryIO.Read.from(source_2));
		PCollection<String> result = joinTables(table1, table2);
		
		result.apply(TextIO.Write.to(options.getOutput()));
		
		pipeline.run();
	}
	
}
