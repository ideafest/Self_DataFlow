package com.FinalJoins.Join1;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

public class A4 {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public PCollection<TableRow> runIt(Init init){
		
		A3 a3 = new A3();
		D1 d1 = new D1();
		
		PCollection<TableRow> a3PCollection = a3.runIt(init);
		PCollection<TableRow> d1PCollection = d1.runIt(init);
		
		PCollection<TableRow> resultPCollection = PCollectionList.of(a3PCollection).and(d1PCollection).apply(Flatten.pCollections());
		
		return resultPCollection;
		
	}
	
}
