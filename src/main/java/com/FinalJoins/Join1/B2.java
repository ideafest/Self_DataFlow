package com.FinalJoins.Join1;

import com.Essential.Joins;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class B2 {
	
	private static class ExtractFromE1_F1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("_id");
			context.output(KV.of(key, element));
		}
	}
	
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public PCollection<TableRow> runIt(Init init){
		Joins joins = new Joins();
		
		E1 e1 = new E1();
		F1 f1 = new F1();
		
		PCollection<KV<String, TableRow>> pCollection1 = e1.runIt(init).apply(ParDo.of(new ExtractFromE1_F1()));
		PCollection<KV<String, TableRow>> pCollection2 = f1.runIt(init).apply(ParDo.of(new ExtractFromE1_F1()));
		
		PCollection<TableRow> finalResultPCollection = joins.innerJoin1(pCollection1, pCollection2, "A_", "B_",
				"JoiningJoins");
		
		return finalResultPCollection;
	}
	
}
