package com.Practice.GottaTestThis;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.dataflow.sdk.Pipeline;
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

public class NumeroUno {
	
	private static class ExtractFromColl extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("_id");
			context.output(KV.of(key, element));
		}
	}
	
	private static PCollection<String> joinOperation(PCollection<KV<String, TableRow>> kvpCollection1, PCollection<KV<String, TableRow>> kvpCollection2){
		
		TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<String> resultPCollection = pCollection
				.apply(ParDo.named("Result2").of(new DoFn<KV<String, CoGbkResult>, String>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						
						TableRow freshRow;
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								freshRow = new TableRow();
								for(String fieldName : tableRow1.keySet()){
									freshRow.set(fieldName, tableRow1.get(fieldName));
								}
								
								for(String fieldName : tableRow2.keySet()){
									freshRow.set(fieldName, tableRow2.get(fieldName));
								}
								
								context.output(freshRow.toPrettyString());
							}
						}
					}
				}));
		
		return resultPCollection;
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
		
		ForJoin6 join6 = new ForJoin6();
		ForJoin7 join7 = new ForJoin7();
		
		PCollection<KV<String, TableRow>> pCollection1 = join6.runIt(pipeline).apply(ParDo.of(new ExtractFromColl()));
		PCollection<KV<String, TableRow>> pCollection2 = join7.runIt(pipeline).apply(ParDo.of(new ExtractFromColl()));
		
		PCollection<String> finalResultPCollection = joinOperation(pCollection1, pCollection2);
		
		finalResultPCollection.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
		
	}

}
