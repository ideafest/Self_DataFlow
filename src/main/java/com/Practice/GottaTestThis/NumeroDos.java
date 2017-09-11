package com.Practice.GottaTestThis;

import com.Practice.Basic.Queries;
import com.google.api.services.bigquery.model.Table;
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
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.junit.Test;

import java.util.StringTokenizer;

public class NumeroDos {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	private static class Extract1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			
			String campaignId = (String) element.get("campaignId");
			String prospectCallId = (String) element.get("prospectCallId");
			String prospectInteractionSessionId = (String) element.get("prospectInteractionSessionId");
			String status_seq = String.valueOf(element.get("status_seq"));
			
			String finalKey = campaignId + prospectCallId + prospectInteractionSessionId + status_seq;
			
			context.output(KV.of(finalKey, element));
		}
	}
	
	private static class ExtractForJoin1 extends DoFn<TableRow, KV<String, TableRow>>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(KV.of((String)context.element().get("campaignID"), context.element()));
		}
	}
	
	private static class ExtractForJoin2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key1 = (String) element.get("A_campaignID");
			String key2 = (String) element.get("A_prospectCallId");
			context.output(KV.of(key1+key2, context.element()));
		}
	}
	
	private static class ExtractFromJoin3 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key1 = (String) element.get("campaignID");
			String key2 = (String) element.get("prospectCallID");
			context.output(KV.of(key1+key2, context.element()));
		}
	}
	
	private static class ExtractForCMPGN extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(KV.of((String)context.element().get("_id"), context.element()));
		}
	}
	
	private static class GenerateKV extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String key1 = (String) context.element().get("B_campaignId");
			String key2 = (String) context.element().get("B_prospectCallId");
			String key3 = (String) context.element().get("B_prospectInteractionSessionId");
			context.output(KV.of(key1 + key2 + key3, context.element()));
		}
	}
	
	private static class Select1 extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			TableRow element = context.element();
			TableRow freshRow = new TableRow();
			
			freshRow.set("campaignID", element.get("B_campaignId"));
			freshRow.set("agentId", element.get("B_agentId"));
			freshRow.set("prospectCallId", element.get("B_prospectCallId"));
			freshRow.set("prospectInteractionSessionId", element.get("B_prospectInteractionSessionId"));
			freshRow.set("createdDate", element.get("B_createdDate"));
			freshRow.set("updatedDate", element.get("B_updatedDate"));
			freshRow.set("prospectHandleDuration", element.get("B_prospectHandleDuration"));
			freshRow.set("DNI", element.get("B_DNI"));
			freshRow.set("callStartTime", element.get("B_callStartTime"));
			freshRow.set("callStartDate", element.get("B_callStartDate"));
			freshRow.set("prospectHandleDurationFormatted", element.get("B_prospectHandleDurationFormatted"));
			freshRow.set("voiceDurationFormatted", element.get("B_voiceDurationFormatted"));
			freshRow.set("voiceDuration", element.get("B_voiceDuration"));
			freshRow.set("callDuration", element.get("B_callDuration"));
			freshRow.set("status", element.get("B_status"));
			freshRow.set("dispositionStatus", element.get("B_dispositionStatus"));
			freshRow.set("subStatus", element.get("B_subStatus"));
			freshRow.set("recordingURL", element.get("B_recordingURL"));
			freshRow.set("status_seq", element.get("B_status_seq"));
			freshRow.set("twilioCallsId", element.get("B_twilioCallsId"));
			freshRow.set("deliveredAssetId", element.get("B_deliveredAssetID"));
			freshRow.set("callbackDate", element.get("B_callbackDate"));
			freshRow.set("outboundNumber", element.get("B_outboundNumber"));
			freshRow.set("latestProspectIdentityChangeLogId", element.get("B_latestProspectIdentityChangeLogId"));
			freshRow.set("callRetryCount", element.get("B_callRetryCount"));
			
			context.output(freshRow);
		}
	}
	
	static PCollection<TableRow> combineTableDetails(PCollection<KV<String, TableRow>> stringPCollection1, PCollection<KV<String, TableRow>> stringPCollection2
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
	
	static PCollection<TableRow> combineTableDetails2(PCollection<KV<String, TableRow>> stringPCollection1, PCollection<KV<String, TableRow>> stringPCollection2
			, String table2Prefix){
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, stringPCollection1)
				.and(tupleTag2, stringPCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named("Result2").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								
								for(String field : tableRow2.keySet()){
									tableRow1.set(table2Prefix + field, tableRow2.get(field));
								}
								context.output(tableRow1);
							}
						}
					}
				}));
		
		
		return resultPCollection;
	}
	
	private static PCollection<TableRow> joinOperation(PCollection<KV<String, TableRow>> kvpCollection1, PCollection<KV<String, TableRow>> kvpCollection2
							, String table1Prefix, String table2Prefix){
		
		TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named("Result2").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
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
									freshRow.set(table1Prefix + fieldName, tableRow1.get(fieldName));
								}
								
								for(String fieldName : tableRow2.keySet()){
									freshRow.set(table2Prefix + fieldName, tableRow2.get(fieldName));
								}
								
								context.output(freshRow);
							}
						}
					}
				}));
		
		PCollection<KV<String, TableRow>> kvpCollection = resultPCollection.apply(ParDo.of(new GenerateKV()));
		
		PCollection<KV<String, Iterable<TableRow>>> grouped1 = kvpCollection.apply(GroupByKey.create());
		
		PCollection<TableRow> stringPCollection = grouped1
				.apply(ParDo.of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						
						KV<String, Iterable<TableRow>> element = context.element();
						Iterable<TableRow> rowIterable = element.getValue();
						
						String maxUpdatedDate = "";
						
						for(TableRow tableRow : rowIterable){
							String updatedDate = (String)tableRow.get("B_updatedDate");
							if(updatedDate.compareTo(maxUpdatedDate) > 0){
								maxUpdatedDate = updatedDate;
							}
						}
						for(TableRow tableRow : rowIterable){
							String updatedDate = (String)tableRow.get("B_updatedDate");
							if(maxUpdatedDate.equals(updatedDate)){
								context.output(tableRow);
							}
						}
						
					}
				}));
		
		return stringPCollection;
	}
	
	
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		
		Queries queries = new Queries();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		ForJoin1 join1 = new ForJoin1();
		ForJoin2 join2 = new ForJoin2();
		ForJoin3 join3 = new ForJoin3();
		
		PCollection<KV<String, TableRow>> kvpCollection1 = join1.runIt(pipeline).apply(ParDo.of(new Extract1()));
		PCollection<KV<String, TableRow>> kvpCollection2 = join2.runIt(pipeline).apply(ParDo.of(new Extract1()));
		
		PCollection<TableRow> finalResultPCollection = joinOperation(kvpCollection1, kvpCollection2
				, "A_", "B_");
		
		PCollection<TableRow> newTableForJoins = finalResultPCollection.apply(ParDo.of(new Select1()));
//
		
		PCollection<TableRow> cmpgnTable = pipeline.apply(BigQueryIO.Read.named("CMPGN").fromQuery(queries.CMPGN));
		
		PCollection<KV<String, TableRow>> kvpCollection3 = newTableForJoins.apply(ParDo.of(new ExtractForJoin1()));
		PCollection<KV<String, TableRow>> kvpCollection4 = cmpgnTable.apply(ParDo.of(new ExtractForCMPGN()));
		
		PCollection<TableRow> rowPCollection1 = combineTableDetails(kvpCollection3, kvpCollection4, "A_", "B_");
		
		PCollection<KV<String, TableRow>> kvpCollection5 = rowPCollection1.apply(ParDo.of(new ExtractForJoin2()));
		PCollection<KV<String, TableRow>> kvpCollection6 = join3.runIt(pipeline).apply(ParDo.of(new ExtractFromJoin3()));
		
		PCollection<TableRow> rowPCollection2 = combineTableDetails2(kvpCollection5, kvpCollection6, "C_");
		
		rowPCollection2.apply(ParDo.of(new ConvertToString()))
				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		
		
		pipeline.run();
		
	}

	
}
