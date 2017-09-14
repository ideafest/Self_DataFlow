package com.Practice.Basic;

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
import org.junit.Test;

import java.util.StringTokenizer;

public class BasicTest11 {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			context.output(context.element().toPrettyString());
			
		}
	}
	
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
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
		
		@Description("Start time for query")
		@Validation.Required
		String getStartTime();
		void setStartTime(String startTme);
		
		@Description("End time for query")
		@Validation.Required
		String getEndTime();
		void setEndTime(String endTime);
	}
	
	public static void main(String[] args) {
		
		Queries queries = new Queries();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		String PC_PCI = "select * from [vantage-167009:Xtaas.PC_PCI] " +
				"where updateddate > '" +options.getStartTime()+"'  and updateddate < '" +options.getEndTime()+"'";
		
		
		PCollection<KV<String, TableRow>> source1Table = pipeline
				.apply(BigQueryIO.Read.named("PC_PCI").fromQuery(PC_PCI))
				.apply(ParDo.named("FormattedPC_PCI").of(new Extract1()));
		
		PCollection<KV<String, TableRow>> source2Table = pipeline
				.apply(BigQueryIO.Read.named("PC_Prospect").from(queries.pciProspect))
				.apply(ParDo.named("FormattedPC_Prospect").of(new Extract1()));
		
		
		PCollection<TableRow> rowPCollection = combineTableDetails(source1Table, source2Table,
				"A_", "B_");
		
		PCollection<KV<String, TableRow>> joinResult1 = rowPCollection
				.apply(ParDo.named("FormattedJoin").of(new Extract2()));
		
		PCollection<KV<String, TableRow>> source3Table = pipeline
				.apply(BigQueryIO.Read.named("Master_Status").from(queries.master_status))
				.apply(ParDo.named("FormattedMaster_Status").of(new Extract3()));
		
		PCollection<TableRow> rowPCollection2 = combineTableDetails2(joinResult1, source3Table,
				 "C_");
		
		rowPCollection2.apply(ParDo.of(new ConvertToString()))
				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		

		pipeline.run();
	}

	@Test
	public void test1(){
		
		String prospectHandleDurationRes = "";
		int prospectHandleDuration = 43;
		String s = String.valueOf(prospectHandleDuration / 60) + "0";
		String s2 = String.valueOf(prospectHandleDuration % 60);
		System.out.println(s+":"+s2);
		
	}
	
}
