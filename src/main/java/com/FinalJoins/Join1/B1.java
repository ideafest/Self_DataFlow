package com.FinalJoins.Join1;

import com.Essential.Joins;
import com.Essential.Queries;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.StringTokenizer;

public class B1 {
	
	static class Extract1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, context.element()));
		}
	}
	
	static class Extract2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("A_status");
			context.output(KV.of(id, context.element()));
		}
	}
	
	static class Extract3 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("code");
			context.output(KV.of(id, context.element()));
		}
	}
	
	/*
	 concat(LPAD((string(integer(P.prospecthandleduration / 60))),2,'0'),':'
	 ,LPAD(string(P.prospecthandleduration % 60),2,'0')) AS prospecthandledurationformatted,

	 */
	private static String getDurationFormatted(int val){
		String s = String.valueOf(val / 60) + "0";
		String s2 = String.valueOf(val % 60);
		return s+":"+s2;
	}
	
	static class Filter extends DoFn<TableRow, TableRow> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			boolean isDeleted = (boolean) element.get("B_isdeleted");
			boolean isDirty = (boolean) element.get("B_isdirty");
			
			if(!isDeleted && !isDirty){
				context.output(element);
			}
		}
	}
	
	
	private static String getDate(String str){
		StringTokenizer stringTokenizer = new StringTokenizer(str);
		return stringTokenizer.nextToken();
	}
	
	private static String nvl(String prospectCallId, String updatedDate, String code){
		StringTokenizer stringTokenizer = new StringTokenizer(updatedDate);
		String date = stringTokenizer.nextToken() + " " + stringTokenizer.nextToken();
		
		String result = prospectCallId + "-" + date + "-" + code;
		return result;
	}
	
	private static class FinalFieldTableRow extends DoFn<TableRow, TableRow> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			TableRow element = context.element();
			TableRow tableRow = new TableRow();
			
			tableRow.set("campaignId", element.get("A_campaignid"));
			tableRow.set("agentId", element.get("A_agentid"));
			tableRow.set("prospectCallId", element.get("A_prospectcallid"));
			
			String prospectCallId = (String) element.get("A_prospectcallid");
			String prospectInteractionSessionId = (String) element.get("A_prospectinteractionsessionid");
			String updatedDate = (String) element.get("A_updateddate");
			String code = (String) element.get("C_code");
			
			if(prospectInteractionSessionId == null){
				tableRow.set("prospectInteractionSessionId", nvl(prospectCallId, updatedDate, code));
			}
			else{
				tableRow.set("prospectInteractionSessionId", prospectInteractionSessionId);
			}
			
			tableRow.set("createdDate", element.get("A_createddate"));
			tableRow.set("updatedDate", element.get("A_updateddate"));
			tableRow.set("prospectHandleDuration", element.get("A_prospecthandleduration"));
			tableRow.set("DNI", element.get("B_phone"));
			tableRow.set("callStartTime", element.get("A_callstarttime"));
			if(element.get("A_callstarttime") == null){
				tableRow.set("callStartDate", "null");
			}
			else{
				tableRow.set("callStartDate", getDate((String) element.get("A_callstarttime")));
			}
			
			if(element.get("A_prospecthandleduration") == null){
				tableRow.set("prospectHandleDurationFormatted", "null");
			}else {
				tableRow.set("prospectHandleDurationFormatted", getDurationFormatted((Integer.parseInt((String) element.get("A_prospecthandleduration")))));
			}
			if( element.get("A_telcoduration") == null){
				tableRow.set("voiceDurationFormatted", "null");
			}else{
				tableRow.set("voiceDurationFormatted", getDurationFormatted(Integer.parseInt((String) element.get("A_telcoduration"))));
			}
			
			tableRow.set("voiceDuration", element.get("A_telcoduration"));
			tableRow.set("callDuration", element.get("A_callduration"));
			tableRow.set("status", element.get("A_status"));
			tableRow.set("dispositionStatus", element.get("A_dispositionstatus"));
			tableRow.set("subStatus", element.get("A_substatus"));
			tableRow.set("recordingURL", element.get("A_recordingurl"));
			tableRow.set("status_seq", element.get("C_status_seq"));
			tableRow.set("twilioCallsId", element.get("A_twiliocallsid"));
			tableRow.set("deliveredAssetID", element.get("A_deliveredassetid"));
			tableRow.set("callbackDate", element.get("A_callbackdate"));
			tableRow.set("outboundNumber", element.get("A_outboundnumber"));
			tableRow.set("latestProspectIdentityChangeLogId", element.get("A_latestprospectidentitychangelogid"));
			tableRow.set("callRetryCount", element.get("A_callretrycount"));
			
			context.output(tableRow);
		}
	}
	
	public PCollection<TableRow> runIt(Pipeline pipeline){
		
		Joins joins= new Joins();
		Queries queries = new Queries();
		
		PCollection<KV<String, TableRow>> source1Table = pipeline
				.apply(BigQueryIO.Read.named("Reader1").fromQuery(queries.PCI_Time))
				.apply(ParDo.named("FormatData1").of(new Extract1()));
		
		PCollection<KV<String, TableRow>> source2Table = pipeline
				.apply(BigQueryIO.Read.named("Reader2").from(queries.pciProspect))
				.apply(ParDo.named("FormatData2").of(new Extract1()));
		
		
		PCollection<TableRow> rowPCollection = joins.innerJoin1(source1Table, source2Table,
				"A_", "B_");
		
		PCollection<KV<String, TableRow>> joinResult1 = rowPCollection
				.apply(ParDo.named("FormatData3").of(new Extract2()));
		
		PCollection<KV<String, TableRow>> source3Table = pipeline
				.apply(BigQueryIO.Read.named("Reader4").from(queries.master_status))
				.apply(ParDo.named("FormatData4").of(new Extract3()));
		
		PCollection<TableRow> rowPCollection2 = joins.innerJoin2(joinResult1, source3Table,
				"C_");
		
		
		return rowPCollection2.apply(ParDo.of(new Filter()))
				.apply(ParDo.of(new FinalFieldTableRow()));
	}
	
}
