package com.Practice.Basic;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class CombineFnTest {
	
	private static class ExtractIntegerFields extends DoFn<TableRow, TableRow> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
		
			TableRow element = context.element();

			TableRow tableRow = new TableRow();
			tableRow.set("year", element.get("year"));
			tableRow.set("month", element.get("month"));
			tableRow.set("day", element.get("day"));

			context.output(tableRow);
		}
	}
	
	
	private static PCollection<Integer> getData(PCollection<TableRow> rowPCollection, String key){
		PCollection<Integer> integerPCollection = rowPCollection
				.apply(ParDo.named("GetData").of(new DoFn<TableRow, Integer>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						int field = (int) context.element().get(key);
						context.output(field);
					}
				}));
		
		return integerPCollection;
	}
	
//	private static PCollection<>
	
//	private static PCollection<TableRow> maxData(PCollection<TableRow> rowPCollection){
//
//		PCollection<List<TableRow>> finalPCollection = rowPCollection
//				.apply(ParDo.named("Meh").of(new DoFn<TableRow, List<TableRow>>() {
//					@Override
//					public void processElement(ProcessContext context) throws Exception {
//
//						TableRow element = context.element();
//						int maxYear = 0, maxMonth = 0, maxDay = 0;
//
////						List<TableRow> rowList = new ArrayList<>();
////						rowList.add((TableRow) element.get("year"));
//
//
//
//
//					}
//				}));
//			return null;
//		}

	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	
	public static void main(String[] args){
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		PCollection<TableRow> rowPCollection = pipeline.apply(BigQueryIO.Read.from("vantage-167009:Learning.gsod_copy"))
				.apply(ParDo.named("Meh").of(new ExtractIntegerFields()));
	
	}
	
}
