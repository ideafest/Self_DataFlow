package com.FinalJoins.Join1;

import com.Essential.*;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import jdk.nashorn.internal.scripts.JO;

import java.util.StringTokenizer;

public class A1 //extends Job {
{
	static class Extract1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("status");
			context.output(KV.of(id, context.element()));
		}
	}
	
	static class Extract2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("code");
			context.output(KV.of(id, context.element()));
		}
	}
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	
	private static String nvl(String prospectCallId, String updatedDate, String code){
		StringTokenizer stringTokenizer = new StringTokenizer(updatedDate);
		String date = stringTokenizer.nextToken() + " " + stringTokenizer.nextToken();
		
		String result = prospectCallId + "-" + date + "-" + code;
		return result;
	}
	
	private static class GroupByKeys1 extends DoFn<TableRow, KV<String, TableRow>> {
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
	
	
	private static PCollection<TableRow> postOperations(PCollection<TableRow> rowPCollection){
		
		PCollection<KV<String, TableRow>> kvpCollection = rowPCollection.apply(ParDo.of(new GroupByKeys1()));
		PCollection<KV<String, Iterable<TableRow>>> grouped1 = kvpCollection.apply(GroupByKey.create());
		
		PCollection<TableRow> resultPCollection = grouped1
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
		
		return resultPCollection;
		
	}
	
	public PCollection<TableRow> runIt(Init init){
//
		Joins joins = new Joins();

		PCollection<KV<String, TableRow>> pcpci = init.getPC_PCI()
				.apply(ParDo.named("FormatData1").of(new Extract1()));

		PCollection<KV<String, TableRow>> masterstatus = init.getMaster_status()
				.apply(ParDo.named("FormatData2").of(new Extract2()));

		PCollection<TableRow> resultPCollection = joins.innerJoin1(pcpci, masterstatus, "A_", "B_");

		PCollection<TableRow> result = postOperations(resultPCollection);

		return result;
	}
	
//	public static void main(String[] args) {
//		Queries queries = new Queries();
//		Joins joins = new Joins();
//		JobOptions jobOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(JobOptions.class);
//		Pipeline pipeline = Pipeline.create(jobOptions);
//
//		String PC_PCI = "SELECT * FROM [vantage-167009:Xtaas.PC_PCI]\n" +
//				"\tWHERE  updateddate > '"+ jobOptions.getStartTime()
//				+"' AND updateddate < '"+ jobOptions.getEndTime() +"'";
//
//		PCollection<KV<String, TableRow>> kvpCollection1 = pipeline.apply(BigQueryIO.Read.named("Reader1").fromQuery(PC_PCI))
//				.apply(ParDo.named("FormatData1").of(new Extract1()));
//
//		PCollection<KV<String, TableRow>> kvpCollection2 = pipeline.apply(BigQueryIO.Read.named("Reader2").from(queries.master_status))
//				.apply(ParDo.named("FormatData2").of(new Extract2()));
//
//		PCollection<TableRow> resultPCollection = joins.innerJoin1(kvpCollection1, kvpCollection2, "A_", "B_");
//
//		PCollection<TableRow> result = postOperations(resultPCollection);
//
//		result.apply(ParDo.of(new ConvertToString()))
//				.apply(TextIO.Write.to(jobOptions.getOutput()));
//
//		pipeline.run();
//	}
	
}
