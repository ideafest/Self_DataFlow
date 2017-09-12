package com.FinalJoins.Join1;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class B2 {
	
	private static class ExtractFromE1_F1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("_id");
			context.output(KV.of(key, element));
		}
	}
	
	static PCollection<TableRow> joinOperation1(PCollection<KV<String, TableRow>> stringPCollection1, PCollection<KV<String, TableRow>> stringPCollection2
			, String table1Prefix, String table2Prefix){
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, stringPCollection1)
				.and(tupleTag2, stringPCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named("Result1").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						
						TableRow tableRow;
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								
								tableRow = new TableRow();
								
								for(String field: tableRow1.keySet()){
									tableRow.set(table1Prefix + field, tableRow1.get(field));
								}
								
								for(String field : tableRow2.keySet()){
									tableRow.set(table2Prefix + field, tableRow2.get(field));
								}
								context.output(tableRow);
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
	
	public PCollection<TableRow> runIt(Pipeline pipeline){
		
		E1 e1 = new E1();
		F1 f1 = new F1();
		
		PCollection<KV<String, TableRow>> pCollection1 = e1.runIt(pipeline).apply(ParDo.of(new ExtractFromE1_F1()));
		PCollection<KV<String, TableRow>> pCollection2 = f1.runIt(pipeline).apply(ParDo.of(new ExtractFromE1_F1()));
		
		PCollection<TableRow> finalResultPCollection = joinOperation1(pCollection1, pCollection2, "A_", "B_");
		
		return finalResultPCollection;
	}
	
//	public static void main(String[] args) {
//
//		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//		Pipeline pipeline = Pipeline.create(options);
//
//		E1 e1 = new E1();
//		F1 f1 = new F1();
//
//		PCollection<KV<String, TableRow>> pCollection1 = e1.runIt(pipeline).apply(ParDo.of(new ExtractFromE1_F1()));
//		PCollection<KV<String, TableRow>> pCollection2 = f1.runIt(pipeline).apply(ParDo.of(new ExtractFromE1_F1()));
//
//		PCollection<TableRow> finalResultPCollection = joinOperation(pCollection1, pCollection2);
//
//		finalResultPCollection.apply(TextIO.Write.named("Writer").to(options.getOutput()));
//
//		pipeline.run();
//
//	}

}
