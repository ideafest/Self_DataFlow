package com.Practice.GottaTestThis;

import com.google.api.services.bigquery.model.TableRow;
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

public class NumeroDos {
	
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
	
	private static class Select1 extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			TableRow element = context.element();
			
			String campaignID;
			String agentId;
			String prospectCallId;
			String prospectIntractionSessionId;
			String createdDate;
			String updatedDate;
			String prospectHandleDuration;
			String dni;
			String callStartTime;
			String callStartDate;
			String prospectHandleDurationFormatted;
			String voiceDurationFormatted;
			String voiceDuration;
			String callDuration;
			String status;
			String dispositionStatus;
			String subStatus;
			String recordingUrl;
			String twilioCallsId;
			String deliveredAssetId;
			String callbackDate;
			String outboundNumber;
			String latestProspectIdentityChangeLogId;
			String callRetyCount;
			
		}
	}
	
	private static PCollection<String> joinOperation(PCollection<KV<String, TableRow>> kvpCollection1, PCollection<KV<String, TableRow>> kvpCollection2
							, String table1Prefix, String table2Prefix){
		
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
									freshRow.set(table1Prefix + fieldName, tableRow1.get(fieldName));
								}
								
								for(String fieldName : tableRow2.keySet()){
									freshRow.set(table2Prefix + fieldName, tableRow2.get(fieldName));
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
		
		ForJoin1 join1 = new ForJoin1();
		ForJoin2 join2 = new ForJoin2();
		
		PCollection<KV<String, TableRow>> kvpCollection1 = join1.runIt(pipeline).apply(ParDo.of(new Extract1()));
		PCollection<KV<String, TableRow>> kvpCollection2 = join2.runIt(pipeline).apply(ParDo.of(new Extract1()));
		
		PCollection<String> finalResultPCollection = joinOperation(kvpCollection1, kvpCollection2
				, "A_", "B_");
		
		finalResultPCollection.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
		
	}
	
}
