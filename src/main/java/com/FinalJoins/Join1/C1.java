package com.FinalJoins.Join1;

import com.Essential.Joins;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.StringTokenizer;

public class C1 {
	private static class ReadFromTable1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromTable2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static String getDate(String str){
		StringTokenizer stringTokenizer = new StringTokenizer(str);
		return stringTokenizer.nextToken();
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
			
//			boolean aIsDirty = (boolean) element.get("A_isdirty");
//			boolean aIsDeleted = (boolean) element.get("A_isdeleted");
//			boolean bIsDirty = (boolean) element.get("B_isdirty");
//			boolean bIsDeleted = (boolean) element.get("B_isdeleted");
//			if (!aIsDirty && !aIsDeleted && !bIsDirty && !bIsDeleted){
//			}
			
			context.output(freshRow);
		}
	}
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public PCollection<TableRow> runIt(Init init) {

		Joins joins = new Joins();
//
		PCollection<KV<String, TableRow>> prospectCallLogPCollection = init.getProspectCallLog()
				.apply(ParDo.of(new ReadFromTable1()));
		PCollection<KV<String, TableRow>> prospectCallPCollection = init.getProspectCall()
				.apply(ParDo.of(new ReadFromTable2()));

		PCollection<TableRow> rowPCollection = joins.innerJoin1(prospectCallLogPCollection, prospectCallPCollection,
				"A_", "B_", "JoinOfProspectCallLog_ProspectCall");
		
		return rowPCollection.apply(ParDo.of(new Filter()));
	}
	
}
