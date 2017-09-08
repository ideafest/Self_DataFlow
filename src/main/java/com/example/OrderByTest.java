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
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Iterators;

public class OrderByTest {
	
	private static class GenerateKV extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String key = (String) context.element().get("corpus");
			String key1 = (String) context.element().get("word");
			context.output(KV.of(key+key1, context.element()));
		}
	}
	
	private static PCollection<String> operations(PCollection<KV<String, TableRow>> kvpCollection){
		
		PCollection<KV<String, Iterable<TableRow>>> iterablePCollection = kvpCollection.apply(GroupByKey.create());
		
		PCollection<String> pCollection = iterablePCollection
				.apply(ParDo.of(new DoFn<KV<String, Iterable<TableRow>>, String>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						
						KV<String, Iterable<TableRow>> element = context.element();
						Iterable<TableRow> rowIterable = element.getValue();
						TableRow freshRow = new TableRow();
						
						freshRow.set("word", rowIterable.iterator().next().get("word"));
						freshRow.set("corpus", rowIterable.iterator().next().get("corpus"));
						
						int minWordCount = 999999;
						for(TableRow tableRow : rowIterable){
							if(Integer.parseInt((String) tableRow.get("word_count")) < minWordCount){
								minWordCount = Integer.parseInt((String) tableRow.get("word_count"));
							}
						}
						
						freshRow.set("min", minWordCount);
						context.output(freshRow.toPrettyString());
					}
				}));
	
		return pCollection;
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
		
		PCollection< KV<String, TableRow>> rowPCollection = pipeline.apply(BigQueryIO.Read.from("vantage-167009:Learning.shakespeare_copy"))
				.apply(ParDo.of(new GenerateKV()));
		
		PCollection<String> stringPCollection = operations(rowPCollection);
		
		stringPCollection.apply(TextIO.Write.to(options.getOutput()));
		pipeline.run();
		
	}
	
}
