package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

public class SideViewTrial {

	private static final String tableName = "zimetrics:Learning.shakespeare_copy";
	
	static class ExtractWords extends DoFn<TableRow, String>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			context.output((String) row.get("word"));
		}
	}
	
	static class ExtractWordLen extends DoFn<TableRow, Integer>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			context.output(Integer.parseInt((String)  row.get("word_count")));
		}
	}
	
	static class PerformProcessing extends PTransform<PCollection<TableRow>, PCollection<String>> {
		@Override
		public PCollection<String> apply(PCollection<TableRow> rowPCollection){
			PCollection<String> wordCollection = rowPCollection.apply(ParDo.of(new ExtractWords()));
			PCollection<Integer> wordLenCollection = rowPCollection.apply(ParDo.of(new ExtractWordLen()));
			
			final PCollectionView<Integer> wordIntegerPCollectionView =
					wordLenCollection.apply(Combine.globally(new Max.MaxIntegerFn()).asSingletonView());
			
			
			PCollection<String> wordsBelowCtOff =
					wordCollection.apply(ParDo.withSideInputs(wordIntegerPCollectionView)
							.of(new DoFn<String, String>() {
								@Override
								public void processElement(ProcessContext context) throws Exception {
									String word = context.element();
									int cutOffLength = context.sideInput(wordIntegerPCollectionView);
									if(word.length() > cutOffLength){
										context.output(word);
									}
								}
							}));
			
			return wordsBelowCtOff;
		}
	
	}
	
	private interface Options extends PipelineOptions {
		@Description("Source")
		@Default.String(tableName)
		String getInput();
		void setInput(String value);
		
	}
	
	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		pipeline.apply(BigQueryIO.Read.named("Reader").from(options.getInput()))
				.apply(new PerformProcessing())
				.apply(TextIO.Write.named("Writer").to("gs://learning001/output/sideView/views"));
		
		pipeline.run();
	}
	
}
