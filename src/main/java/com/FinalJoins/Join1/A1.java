package com.FinalJoins.Join1;

import com.Practice.Basic.Queries;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
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

public class A1 {
	
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
	
	
	
	
	private static PCollection<TableRow> joinOperation1(PCollection<TableRow> stringPCollection1, PCollection<TableRow> stringPCollection2
			, String table1Prefix, String table2Prefix){
		
		PCollection<KV<String, TableRow>> kvpCollection1 = stringPCollection1.apply(ParDo.named("FormatData1").of(new Extract1()));
		PCollection<KV<String, TableRow>> kvpCollection2 = stringPCollection2.apply(ParDo.named("FormatData2").of(new Extract2()));
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.create());
		
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named("Result").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
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
	
	private static PCollection<TableRow> operations(PCollection<TableRow> rowPCollection){
		
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
	
	public PCollection<TableRow> runIt(Pipeline pipeline){
		
		Queries queries = new Queries();
		
		PCollection<TableRow> sourceTable1 = pipeline.apply(BigQueryIO.Read.named("Reader1").from("vantage-167009:Learning.PCI_Temp"));
		PCollection<TableRow> sourceTable2 = pipeline.apply(BigQueryIO.Read.named("Reader2").from(queries.master_status));
		
		PCollection<TableRow> resultPCollection = joinOperation1(sourceTable1, sourceTable2, "A_", "B_");
		
		PCollection<TableRow> result = operations(resultPCollection);
		
		return result;
	}

}
