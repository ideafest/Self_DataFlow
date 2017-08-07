package com.Practice;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class PJoin_v1 {

	private static final String table1 = "vantage-167009:Xtaas.PC_PCI";
	private static final String table2 = "vantage-167009:Xtaas.pci_prospect";
	private static final String table3 = "vantage-167009:Xtaas.master_status";

	static class ExtractFromTable_1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("_id");
			KV<String, TableRow> outputRowKV = KV.of(id, row);
			context.output(outputRowKV);
		}
	}
	static class ExtractFromTable_2 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("_id");
			KV<String, TableRow> outputRowKV = KV.of(id, row);
			context.output(outputRowKV);
		}
	}
	
	static PCollection<TableRow> joinPCollection(PCollection<TableRow> rowPCollection1, PCollection<TableRow> rowPCollection2){
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, TableRow>> kvpCollection1 = rowPCollection1.apply(ParDo.of(new ExtractFromTable_1()));
		PCollection<KV<String, TableRow>> kvpCollection2 = rowPCollection1.apply(ParDo.of(new ExtractFromTable_2()));
		
		PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.<String>create());
		
		
		return null;
	}
	
}
