package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class BQJoin3 {

/*
FROM [vantage-167009:Xtaas.prospectcalllog] A
            INNER JOIN [vantage-167009:Xtaas.prospectcall] B ON A._id = B._id
            INNER JOIN [vantage-167009:Xtaas.prospect] C ON B._id = C._id
            INNER JOIN [vantage-167009:Xtaas.answers] D ON C._id = D._id
            INNER JOIN [vantage-167009:Learning.CMPGN] E on B.campaignid = E._id
 */

	private static class ExtractFromTable_1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("_id");
			context.output(KV.of(id, row));
		}
	}
	
	private static class ExtractFromTable_2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("_id");
			context.output(KV.of(id, row));
		}
	}
	
	static PCollection<TableRow> joinOperation(PCollection<TableRow> pCollection1, PCollection<TableRow> pCollection2){
		PCollection<KV<String, TableRow>> table1 = pCollection1.apply(ParDo.of(new ExtractFromTable_1()));
		PCollection<KV<String, TableRow>> table2 = pCollection2.apply(ParDo.of(new ExtractFromTable_2()));
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple
				.of(tupleTag1, table1)
				.and(tupleTag2, table2)
				.apply(CoGroupByKey.create());
		
		return null;
	}

}
