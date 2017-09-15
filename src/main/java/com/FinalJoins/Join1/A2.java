package com.FinalJoins.Join1;

import com.Essential.Joins;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class A2 {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	private static class ExtractFromA1_B1 extends DoFn<TableRow, KV<String, TableRow>> {
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
	
	private static class GenerateKV extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String key1 = (String) context.element().get("B_campaignId");
			String key2 = (String) context.element().get("B_prospectCallId");
			String key3 = (String) context.element().get("B_prospectInteractionSessionId");
			context.output(KV.of(key1 + key2 + key3, context.element()));
		}
	}
	
	private static class SelectA2 extends DoFn<TableRow, TableRow>{
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
	
	private static PCollection<TableRow> postOperations(PCollection<TableRow> afterJoin){
		PCollection<KV<String, TableRow>> kvpCollection = afterJoin.apply(ParDo.of(new GenerateKV()));
		
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
		
		PCollection<TableRow> resutPCollection = stringPCollection.apply(ParDo.of(new SelectA2()));
		return resutPCollection;
	}
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public PCollection<TableRow> runIt(Pipeline pipeline){
		Joins joins = new Joins();
		A1 a1 = new A1();
		B1 b1 = new B1();
		
		PCollection<KV<String, TableRow>> a1PCollection = a1.runIt(pipeline).apply(ParDo.of(new ExtractFromA1_B1()));
		PCollection<KV<String, TableRow>> b1PCollection = b1.runIt(pipeline).apply(ParDo.of(new ExtractFromA1_B1()));
		
		PCollection<TableRow> tempPCollection = joins.innerJoin1(a1PCollection, b1PCollection, "A_", "B_");
		PCollection<TableRow> a2PCollection = postOperations(tempPCollection);
		return a2PCollection;
	}
	
//	public static void main(String[] args) {
//
//		Queries queries = new Queries();
//		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//		Pipeline pipeline = Pipeline.create(options);
//
//		A1 a1 = new A1();
//		B1 b1 = new B1();
//
//		PCollection<KV<String, TableRow>> a1PCollection = a1.runIt(pipeline).apply(ParDo.of(new ExtractFromA1_B1()));
//		PCollection<KV<String, TableRow>> b1PCollection = b1.runIt(pipeline).apply(ParDo.of(new ExtractFromA1_B1()));
//
//		PCollection<TableRow> tempPCollection = joinOperation1(a1PCollection, b1PCollection, "A_", "B_");
//		PCollection<TableRow> a2PCollection = postOperations(tempPCollection);
//
//		a2PCollection.apply(ParDo.of(new ConvertToString()))
//				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
//
//		pipeline.run();
//	}

}
