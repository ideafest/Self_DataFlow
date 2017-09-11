package com.FinalJoins.Join1;

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
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class D1 {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	private static class ReadFromDncList extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("campaignId");
			if (!(id == null)) {
				context.output(KV.of(id, tableRow));
			}
		}
	}
	
	private static class ReadFromCMPGN extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class SelectFromTempColl extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			TableRow freshRow = new TableRow();
			
			freshRow.set("rownum", 1);
			freshRow.set("campaignId", element.get("A_campaignid"));
			freshRow.set("agentId", element.get("A_agentid"));
			freshRow.set("prospectCallId", element.get("A_prospectcallid"));
			freshRow.set("prospectInteractionSessionId", "null");
			freshRow.set("DNI", element.get("A_phonenumber"));
			freshRow.set("callStartTime", "null");
			freshRow.set("callStartDate", "null");
			freshRow.set("prospectHandleDurationFormatted", "null");
			freshRow.set("voiceDurationFormatted", "null");
			freshRow.set("prospectHandleDuration", 0);
			freshRow.set("voiceDuration", 0);
			freshRow.set("callDuration", 0);
			freshRow.set("status", "null");
			freshRow.set("dispositionStatus", "null");
			freshRow.set("subStatus", "DNCL");
			freshRow.set("recordingURL", element.get("A_recordingurl"));
			freshRow.set("createdDate", element.get("A_createddate"));
			freshRow.set("updatedDate", element.get("A_updateddate"));
			freshRow.set("status_seq", "null");
			freshRow.set("twilioCallsId", "null");
			freshRow.set("deliveredAssetId", "null");
			freshRow.set("callBackDate", "null");
			freshRow.set("outboundNumber", "null");
			freshRow.set("latestProspectIdentityChangeLogId", "null");
			freshRow.set("callRetryCount", 0);
			freshRow.set("campaign", element.get("B_name"));
			freshRow.set("batch_date", "null");
			freshRow.set("dnc_note", element.get("A_note"));
			freshRow.set("dnc_trigger", element.get("A_trigger"));
			
//			boolean aIsDirty = (boolean) element.get("A_isdirty");
//			boolean aIsDeleted = (boolean) element.get("A_isdeleted");
//			boolean bIsDirty = (boolean) element.get("B_isdirty");
//			boolean bIsDeleted = (boolean) element.get("B_isdeleted");
//			if (!aIsDirty && !aIsD  eleted && !bIsDirty && !bIsDeleted){
//			}
			
			String trigger = (String) element.get("A_trigger");
			if(trigger != null){
				if(trigger.equals("DIRECT")){
					context.output(freshRow);
				}
			}
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
		
		Queries queries = new Queries();
		
		PCollection<KV<String, TableRow>> dncListPCollection = pipeline
				.apply(BigQueryIO.Read.named("Source1Reader").from(queries.dncList))
				.apply(ParDo.of(new ReadFromDncList()));
		
		PCollection<KV<String, TableRow>> cmpgnPCollection = pipeline
				.apply(BigQueryIO.Read.named("Source2Reader").fromQuery(queries.CMPGN))
				.apply(ParDo.of(new ReadFromCMPGN()));
		
		PCollection<TableRow> tempPCollection = joinOperation1(dncListPCollection, cmpgnPCollection, "A_", "B_");
		
		PCollection<TableRow> resultPCollection = tempPCollection.apply(ParDo.of(new SelectFromTempColl()));
	
		return resultPCollection;
	}
	
//	public static void main(String[] args) {
//		Queries queries = new Queries();
//		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//		Pipeline pipeline = Pipeline.create(options);
//
//		PCollection<KV<String, TableRow>> dncListPCollection = pipeline
//				.apply(BigQueryIO.Read.named("Source1Reader").from(queries.dncList))
//				.apply(ParDo.of(new ReadFromDncList()));
//
//		PCollection<KV<String, TableRow>> cmpgnPCollection = pipeline
//				.apply(BigQueryIO.Read.named("Source2Reader").fromQuery(queries.CMPGN))
//				.apply(ParDo.of(new ReadFromCMPGN()));
//
//		PCollection<TableRow> tempPCollection = joinOperation1(dncListPCollection, cmpgnPCollection, "A_", "B_");
//
//		PCollection<TableRow> resultPCollection = tempPCollection.apply(ParDo.of(new SelectFromTempColl()));
//
//		resultPCollection.apply(ParDo.of(new ConvertToString()))
//				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
//
//		pipeline.run();
//	}

}
