package com.FinalJoins.Join1;

import com.Essential.Joins;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class B4 {
	
	private static class ExtractFromMasterStatus extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("code");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromMasterDispositionStatus extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("code");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromTemp1PCollection extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("D_code");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromMasterSubStatus extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("code");
			context.output(KV.of(key, element));
		}
	}
	
	public PCollection<TableRow> runIt(Init init) {
		Joins joins = new Joins();
		
		PCollection<KV<String, TableRow>> masterStatusPCollection = init.getMaster_status()
				.apply(ParDo.of(new ExtractFromMasterStatus()));
		
		PCollection<KV<String, TableRow>> masterDispositionStatusPCollection = init.getMaster_dispositionstatus()
				.apply(ParDo.of(new ExtractFromMasterDispositionStatus()));
		
		PCollection<KV<String, TableRow>> masterSubStatusPCollection = init.getMaster_substatus()
				.apply(ParDo.of(new ExtractFromMasterSubStatus()));
		
		PCollection<TableRow> temp1Result = joins.leftOuterJoin1(masterStatusPCollection, masterDispositionStatusPCollection,
				"D_", "E_");
		
		PCollection<KV<String, TableRow>> temp2PCollection = temp1Result.apply(ParDo.of(new ExtractFromTemp1PCollection()));
		
		PCollection<TableRow> resultPCollection = joins.leftOuterJoin2(temp2PCollection, masterSubStatusPCollection, "F");
		
		return resultPCollection;
		
	}
	
}
