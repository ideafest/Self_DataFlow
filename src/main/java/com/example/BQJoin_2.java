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

public class BQJoin_2 {

	/*
	DON'T RUN THIS ONE
	DATA SIZE IS TOO BIG
	CAN CAUSE UNNECESSARY BILLING
	DON'T DO IT
	 */
	
	private static final String table1 = "zimetrics:Learning.weather_stations_copy";
	private static final String table2 = "zimetrics:Learning.gsod_copy";
	
	static class ExtractFromTable1 extends DoFn<TableRow, KV<String, String>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String station_number = (String) row.get("station_number");
			String month = (String) row.get("month");
			context.output(KV.of(station_number, month));
		}
	}
	
	static class ExtractFromTable2 extends DoFn<TableRow, KV<String, String>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row =context.element();
			String station_number = (String) row.get("station_number");
			String day = (String) row.get("day");
			String year = (String) row.get("year");
			String event2 = day + ", " + year;
			context.output(KV.of(station_number, event2));
		}
	}
	
	static PCollection<String> joinTables(PCollection<TableRow> table1, PCollection<TableRow> table2){
		final TupleTag<String> tupleTag1 = new TupleTag<>();
		final TupleTag<String> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, String>> collection1 = table1.apply(ParDo.of(new ExtractFromTable1()));
		PCollection<KV<String, String>> collection2 = table2.apply(ParDo.of(new ExtractFromTable2()));
		
		PCollection<KV<String, CoGbkResult>> groupKey = KeyedPCollectionTuple
				.of(tupleTag1, collection1)
				.and(tupleTag2, collection2)
				.apply(CoGroupByKey.<String>create());
		
		PCollection<KV<String, String>> resCollection = groupKey
				.apply(ParDo.named("Process").of(new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						String key = element.getKey();
						String value = element.getValue().getOnly(tupleTag1);
						for(String stuff : context.element().getValue().getAll(tupleTag2)){
							context.output(KV.of(key, ", " + value + ", " + stuff));
						}
					}
				}));
		
		PCollection<String> finalResult = resCollection
				.apply(ParDo.of(new DoFn<KV<String, String>, String>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						context.output("A : " +context.element().getKey() + ", B :" + context.element().getValue());
					}
				}));
		
		return finalResult;
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
		
		PCollection<TableRow> t1 = pipeline.apply(BigQueryIO.Read.named("Reader1").from(table1));
		PCollection<TableRow> t2 = pipeline.apply(BigQueryIO.Read.named("Reader2").from(table2));
		
		PCollection<String> result = joinTables(t1, t2);
		result.apply(TextIO.Write.to(options.getOutput()));
		
		pipeline.run();
	}
	
	
}
