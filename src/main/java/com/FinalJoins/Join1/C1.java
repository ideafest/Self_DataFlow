package com.FinalJoins.Join1;

import com.Practice.Basic.Queries;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
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

import java.util.StringTokenizer;

public class C1 {
	private static class ReadFromTable1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromTable2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	static PCollection<TableRow> combineTableDetails(PCollection<KV<String, TableRow>> stringPCollection1, PCollection<KV<String, TableRow>> stringPCollection2
			, String table1Prefix, String table2Prefix) {
		
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
						for (TableRow tableRow1 : rowIterable1) {
							
							for (TableRow tableRow2 : rowIterable2) {
								
								tableRow = new TableRow();
								
								for (String field : tableRow1.keySet()) {
									tableRow.set(table1Prefix + field, tableRow1.get(field));
								}
								
								for (String field : tableRow2.keySet()) {
									tableRow.set(table2Prefix + field, tableRow2.get(field));
								}
								context.output(tableRow);
							}
						}
					}
				}));
		
		
		return resultPCollection;
	}
	private static String getDate(String str){
		StringTokenizer stringTokenizer = new StringTokenizer(str);
		return stringTokenizer.nextToken();
	}
	
	private static class Filter extends DoFn<TableRow, TableRow> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			TableRow freshRow = new TableRow();
			
			freshRow.set("campaignID", element.get("B_campaignid"));
			freshRow.set("prospectCallID", element.get("B_prospectcallid"));
			if(element.get("A_createddate") == null){
				freshRow.set("batch_date", "null");
			}
			else{
				freshRow.set("batch_date", getDate((String) element.get("A_createddate")));
			}
			
//			boolean aIsDirty = (boolean) element.get("A_isdirty");
//			boolean aIsDeleted = (boolean) element.get("A_isdeleted");
//			boolean bIsDirty = (boolean) element.get("B_isdirty");
//			boolean bIsDeleted = (boolean) element.get("B_isdeleted");
//			if (!aIsDirty && !aIsDeleted && !bIsDirty && !bIsDeleted){
//			}
			
			context.output(freshRow);
		}
	}
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public PCollection<TableRow> runIt(Pipeline pipeline) {
		Queries queries = new Queries();
		PCollection<KV<String, TableRow>> prospectCallLogPCollection = pipeline
				.apply(BigQueryIO.Read.named("Source1Reader").fromQuery(queries.prospectCallLog))
				.apply(ParDo.of(new ReadFromTable1()));
		PCollection<KV<String, TableRow>> prospectCallPCollection = pipeline
				.apply(BigQueryIO.Read.named("Source2Reader").fromQuery(queries.prospectCall))
				.apply(ParDo.of(new ReadFromTable2()));
		
		PCollection<TableRow> rowPCollection = combineTableDetails(prospectCallLogPCollection, prospectCallPCollection,
				"A_", "B_");
		
		return rowPCollection.apply(ParDo.of(new Filter()));
	}
	
}
