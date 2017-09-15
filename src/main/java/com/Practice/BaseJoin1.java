package com.Practice;

import com.Essential.Queries;
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

import java.util.StringTokenizer;

public class BaseJoin1 {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	private static class FormatPC_PCI extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("status");
			context.output(KV.of(id, context.element()));
		}
	}
	
	static class FormatMaster_Status extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("code");
			context.output(KV.of(id, context.element()));
		}
	}
	
	static class FormatPCPCI_PCIProspect extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, context.element()));
		}
	}
	
	static class FormatPCPCI_PCIProspectJoin extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("A_status");
			context.output(KV.of(id, context.element()));
		}
	}
	
	private static class FormatCMPGN extends DoFn<TableRow, KV<String, TableRow>>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(KV.of((String)context.element().get("_id"), context.element()));
		}
	}
	
	private static class FormatPCollA_B extends DoFn<TableRow, KV<String, TableRow>>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(KV.of((String)context.element().get("campaignID"), context.element()));
		}
	}
	
	private static class FormatForProspectCall_ProspectCallLog extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class FormatJoinAB_CMPGN extends DoFn<TableRow, KV<String, TableRow>>{
		
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
	
	
	private static class FormatForJoins extends DoFn<TableRow, KV<String, TableRow>> {
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
	
	static class SelectDataForGroupBy extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String campaingId = (String) element.get("A_campaignid");
			String prospectCallId = (String) element.get("A_prospectcallid");
			String prospectInteractionSessionId = (String) element.get("A_prospectinteractionsessionid");
			
			String key = campaingId + prospectCallId + prospectInteractionSessionId;
			context.output(KV.of(key, element));
		}
	}
	
	private static class FormatJoinOfPCI_Prospect_Status extends DoFn<TableRow, TableRow> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			TableRow element = context.element();
			TableRow tableRow = new TableRow();
			
			boolean isDeleted = (boolean) element.get("B_isdeleted");
			boolean isDirty = (boolean) element.get("B_isdirty");
			
			if(!isDeleted && !isDirty){
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
			context.output(freshRow);
		}
	}
	
	private static String getDurationFormatted(int val){
		String s = String.valueOf(val / 60) + "0";
		String s2 = String.valueOf(val % 60);
		return s+":"+s2;
	}
	
	private static String nvl(String prospectCallId, String updatedDate, String code){
		StringTokenizer stringTokenizer = new StringTokenizer(updatedDate);
		String date = stringTokenizer.nextToken() + " " + stringTokenizer.nextToken();
		String result = prospectCallId + "-" + date + "-" + code;
		return result;
	}
	
	private static String getDate(String str){
		StringTokenizer stringTokenizer = new StringTokenizer(str);
		return stringTokenizer.nextToken();
	}
	
	static PCollection<TableRow> formatJoinOfPCPCI_MasterStatus(PCollection<TableRow> rowPCollection){
		
		PCollection<KV<String, TableRow>> kvpCollection = rowPCollection.apply(ParDo.of(new SelectDataForGroupBy()));
		PCollection<KV<String, Iterable<TableRow>>> resultOfGroupPCPCI_MasterStatus = kvpCollection.apply(GroupByKey.create());
		PCollection<TableRow> formattedData = resultOfGroupPCPCI_MasterStatus
				.apply(ParDo.named("Formatting1").of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						
						KV<String, Iterable<TableRow>> element = context.element();
						Iterable<TableRow> rowIterable = element.getValue();
						TableRow currentRow = rowIterable.iterator().next();
						
						TableRow tableRow = new TableRow();
						
						tableRow.set("campaignId", currentRow.get("A_campaignid"));
						tableRow.set("prospectCallId", currentRow.get("A_prospectcallid"));
						
						String prospectCallId = (String) currentRow.get("A_prospectcallid");
						String prospectInteractionSessionId = (String) currentRow.get("A_prospectinteractionsessionid");
						String updatedDate = (String) currentRow.get("A_updateddate");
						String code = (String) currentRow.get("B_code");
						
						if(prospectInteractionSessionId == null){
							tableRow.set("prospectInteractionSessionId", nvl(prospectCallId, updatedDate, code));
						}
						else{
							tableRow.set("prospectInteractionSessionId", prospectInteractionSessionId);
						}
						
						int maxStatusSeq = 0;
						
						for(TableRow currTableRow : rowIterable){
							if( Integer.valueOf((String) currTableRow.get("B_status_seq")) > maxStatusSeq){
								maxStatusSeq = Integer.valueOf((String) currTableRow.get("B_status_seq"));
							}
						}
						
						tableRow.set("status_seq", maxStatusSeq);
						context.output(tableRow);
					}
				}));
		return formattedData;
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
	
	static PCollection<TableRow> joinOperation2(PCollection<KV<String, TableRow>> stringPCollection1, PCollection<KV<String, TableRow>> stringPCollection2
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
	
	private static PCollection<TableRow> formatDataForJoinA_B(PCollection<TableRow> rowPCollection){
		
		PCollection<KV<String, TableRow>> kvpCollection = rowPCollection.apply(ParDo.of(new GenerateKV()));
		
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
		
		PCollection<KV<String, TableRow>> pcpciTable1 = pipeline
				.apply(BigQueryIO.Read.named("PC_PCI-1").from(queries.temp_PCPCI))
				.apply(ParDo.named("Format-PC_PCI-1").of(new FormatPC_PCI()));
		
		PCollection<KV<String, TableRow>> masterStatusTable1 = pipeline
				.apply(BigQueryIO.Read.named("master_status-1").from(queries.master_status))
				.apply(ParDo.named("Format-master_status-1").of(new FormatMaster_Status()));
		
		PCollection<TableRow> joinOfPCPCI_MasterStatus = joinOperation1(pcpciTable1, masterStatusTable1, "A_", "B_");
		PCollection<TableRow> formattedDataOfPCI_Status = formatJoinOfPCPCI_MasterStatus(joinOfPCPCI_MasterStatus);
		
		PCollection<KV<String, TableRow>> pcpciTable2 = pipeline
				.apply(BigQueryIO.Read.named("PC_PCI-2").from(queries.temp_PCPCI))
				.apply(ParDo.named("Format-PC_PCI-2").of(new FormatPCPCI_PCIProspect()));
		
		PCollection<KV<String, TableRow>> pciProspectTable = pipeline
				.apply(BigQueryIO.Read.named("pci_prospect").from(queries.pciProspect))
				.apply(ParDo.named("Format-pci_prospect").of(new FormatPCPCI_PCIProspect()));
		
		PCollection<TableRow> joinOfPCPCI_PCIProspect = joinOperation1(pcpciTable2, pciProspectTable, "A_", "B_");;
		
		PCollection<KV<String, TableRow>> masterStatusTable2 = pipeline
				.apply(BigQueryIO.Read.named("master_status-2").from(queries.master_status))
				.apply(ParDo.named("Format-master_status-2").of(new FormatMaster_Status()));

		PCollection<KV<String, TableRow>> formattedDataofPCI_Prospect = joinOfPCPCI_PCIProspect
				.apply(ParDo.named("FormatPCPCI_PCIProspectJoin").of(new FormatPCPCI_PCIProspectJoin()));
		
		
		PCollection<TableRow> joinOfPCI_Prospect__Status = joinOperation2(formattedDataofPCI_Prospect, masterStatusTable2,
				"C_");
		
		PCollection<TableRow> formattedDataOfPCI_Prospect__Status = joinOfPCI_Prospect__Status
				.apply(ParDo.of(new FormatJoinOfPCI_Prospect_Status()));
		
		
		PCollection<KV<String, TableRow>> pCollectionA = formattedDataOfPCI_Status.apply(ParDo.of(new FormatForJoins()));
		PCollection<KV<String, TableRow>> pCollectionB = formattedDataOfPCI_Prospect__Status.apply(ParDo.of(new FormatForJoins()));
		
		PCollection<TableRow> joinOfA_B = joinOperation1(pCollectionA, pCollectionB
				, "A_", "B_");
		
		PCollection<TableRow> formattedA_B = formatDataForJoinA_B(joinOfA_B);
		
		PCollection<KV<String, TableRow>> cmpgnTable = pipeline.apply(BigQueryIO.Read.named("CMPGN").fromQuery(queries.CMPGN))
				.apply(ParDo.of(new FormatCMPGN()));
		
		PCollection<KV<String, TableRow>> pCollectionA_B = formattedA_B.apply(ParDo.of(new FormatPCollA_B()));
		
		PCollection<TableRow> joinOfAB_CMPGN = joinOperation1(pCollectionA_B, cmpgnTable, "A_", "B_");
		
		PCollection<KV<String, TableRow>> formattedAB_CMPGN = joinOfAB_CMPGN.apply(ParDo.of(new FormatJoinAB_CMPGN()));
		
		PCollection<KV<String, TableRow>> prospectCallTable = pipeline
				.apply(BigQueryIO.Read.named("ProspectCall").fromQuery(queries.prospectCallLog))
				.apply(ParDo.of(new FormatForProspectCall_ProspectCallLog()));
		
		PCollection<KV<String, TableRow>> prospectCallLogTable = pipeline
				.apply(BigQueryIO.Read.named("ProspectCallLog").fromQuery(queries.prospectCall))
				.apply(ParDo.of(new FormatForProspectCall_ProspectCallLog()));
		
		PCollection<TableRow> joinOfPC_PCL = joinOperation1(prospectCallTable, prospectCallLogTable, "A_", "B_");
		PCollection<TableRow> tempPCollection = joinOfPC_PCL.apply(ParDo.of(new Filter()));
		
		PCollection<KV<String, TableRow>> formattedTempPCollection = tempPCollection.apply(ParDo.of(new ExtractFromJoin3()));
		
		PCollection<TableRow> rowPCollection2 = joinOperation2(formattedAB_CMPGN, formattedTempPCollection, "C_");
		rowPCollection2.apply(ParDo.of(new ConvertToString()))
				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
	}
}
