package com.FinalJoins.Join1;

import com.Essential.Joins;
import com.Essential.Queries;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class A3 {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	private static class ExtractFromA2 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(KV.of((String)context.element().get("campaignID"), context.element()));
		}
	}
	
	private static class ExtractFromCMPGN extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(KV.of((String)context.element().get("_id"), context.element()));
		}
	}
	
	private static class ExtractFromTempColl extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key1 = (String) element.get("A_campaignID");
			String key2 = (String) element.get("A_prospectCallId");
			context.output(KV.of(key1+key2, context.element()));
		}
	}
	
	private static class ExtractFromC1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key1 = (String) element.get("campaignID");
			String key2 = (String) element.get("prospectCallID");
			context.output(KV.of(key1+key2, context.element()));
		}
	}
	
	private static class SelectFromTempColl2 extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			TableRow freshRow = new TableRow();
			
			freshRow.set("campaignId", element.get("A_campaignID"));
			freshRow.set("agentId", element.get("A_agentId"));
			freshRow.set("prospectCallId", element.get("A_prospectCallId"));
			freshRow.set("prospectSessionInteractionId", element.get("A_prospectInteractionSessionId"));
			freshRow.set("DNI", element.get("A_DNI"));
			freshRow.set("callStartTime", element.get("A_callStartTime"));
			freshRow.set("callStartDate", element.get("A_callStartDate"));
			freshRow.set("prospectHandleDurationFormatted", element.get("A_prospectHandleDurationFormatted"));
			freshRow.set("voiceDurationFormatted", element.get("A_voiceDurationFormatted"));
			freshRow.set("prospectHandleDuration", element.get("A_prospectHandleDuration"));
			freshRow.set("voiceDuration", element.get("A_voiceDuration"));
			freshRow.set("callDuration", element.get("A_callDuration"));
			freshRow.set("status", element.get("A_status"));
			freshRow.set("dispositionStatus", element.get("A_dispositionStatus"));
			freshRow.set("subStatus", element.get("A_subStatus"));
			freshRow.set("recordingURL", element.get("A_recordingURL"));
			freshRow.set("createdDate", element.get("A_createdDate"));
			freshRow.set("updatedDate", element.get("A_updatedDate"));
			freshRow.set("status_seq", element.get("A_status_seq"));
			freshRow.set("twilioCallsId", element.get("A_twilioCallsId"));
			freshRow.set("deliveredAssetId", element.get("A_deliveredAssetId"));
			freshRow.set("callBackDate", element.get("A_callbackDate"));
			freshRow.set("outboundNumber", element.get("A_outboundNumber"));
			freshRow.set("latestProspectIdentityChangeLogId", element.get("A_latestProspectIdentityChangeLogId"));
			freshRow.set("callRetryCount", element.get("A_callRetryCount"));
			freshRow.set("campaign", element.get("B_name"));
			freshRow.set("batch_date", element.get("C_batch_date"));
			
			String subStatus = (String) element.get("A_subStatus");
			if(subStatus == null){
				freshRow.set("dnc_note", "null");
				freshRow.set("dnc_trigger", "null");
			}else{
				if(subStatus.equals("DNCL")){
					freshRow.set("dnc_note", "DNCL requested by prospect while on the call");
					freshRow.set("dnc_trigger", "CALL");
				}
			}
			
			context.output(freshRow);
		}
	}
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public PCollection<TableRow> runIt(Pipeline pipeline){
		
		Queries queries = new Queries();
		Joins joins = new Joins();
		
		A2 a2 = new A2();
		
		PCollection<KV<String, TableRow>> a2PCollection = a2.runIt(pipeline).apply(ParDo.of(new ExtractFromA2()));
		PCollection<KV<String, TableRow>> cmpgnTable = pipeline.apply(BigQueryIO.Read.named("CMPGN").fromQuery(queries.CMPGN))
				.apply(ParDo.of(new ExtractFromCMPGN()));
		
		PCollection<TableRow> tempPCollection1 = joins.innerJoin1(a2PCollection, cmpgnTable, "A_", "B_");
		
		C1 c1 = new C1();
		
		PCollection<KV<String, TableRow>> a3PCollection = tempPCollection1.apply(ParDo.of(new ExtractFromTempColl()));
		PCollection<KV<String, TableRow>> c1PCollection = c1.runIt(pipeline).apply(ParDo.of(new ExtractFromC1()));
		
		PCollection<TableRow> tempPCollection2 = joins.innerJoin2(a3PCollection, c1PCollection, "C_");
		
		PCollection<TableRow> resultPCollection = tempPCollection2.apply(ParDo.of(new SelectFromTempColl2()));
		return resultPCollection;
	}
	
//	public static void main(String[] args) {
//		Queries queries = new Queries();
//		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//		Pipeline pipeline = Pipeline.create(options);
//
//		A2 a2 = new A2();
//
//		PCollection<KV<String, TableRow>> a2PCollection = a2.runIt(pipeline).apply(ParDo.of(new ExtractFromA2()));
//		PCollection<KV<String, TableRow>> cmpgnTable = pipeline.apply(BigQueryIO.Read.named("CMPGN").fromQuery(queries.CMPGN))
//				.apply(ParDo.of(new ExtractFromCMPGN()));
//
//		PCollection<TableRow> tempPCollection1 = joinOperation1(a2PCollection, cmpgnTable, "A_", "B_");
//
//		C1 c1 = new C1();
//
//		PCollection<KV<String, TableRow>> a3PCollection = tempPCollection1.apply(ParDo.of(new ExtractFromTempColl()));
//		PCollection<KV<String, TableRow>> c1PCollection = c1.runIt(pipeline).apply(ParDo.of(new ExtractFromC1()));
//
//		PCollection<TableRow> tempPCollection2 = joinOperation2(a3PCollection, c1PCollection, "C_");
//
//		PCollection<TableRow> resultPCollection = tempPCollection2.apply(ParDo.of(new SelectFromTempColl2()));
//
//		resultPCollection.apply(ParDo.of(new ConvertToString()))
//				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
//
//		pipeline.run();
//
//	}
	
}
