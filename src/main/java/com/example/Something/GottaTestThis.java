package com.example.Something;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

public class GottaTestThis {
	
	private final static String mainTable = "zimetrics:Learning.weather_filter";
	private final static String sideInputTable = "zimetrics:Learning.weather_filter";
	
	private static class ExtractDataFromMainTable extends  DoFn<TableRow, TableRow>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element());
		}
	}
	
	private static class ExtractDataFromSideInputTable extends  DoFn<TableRow, TableRow>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element());
		}
	}
	
	public static PCollection<String> performFiltering(PCollection<TableRow> mainTable, PCollection<TableRow> sideInputTable){
		
		PCollection<SchemaDetails> schemaDetailsPCollectionView = PerformOperations.translateToSchema(sideInputTable);
		PCollectionView<FieldArrayList> arrayListPCollectionView = schemaDetailsPCollectionView.apply(Combine.globally(new CreateSchema()).asSingletonView());
		PCollection<String> resultPCollection = PerformOperations.extractData(mainTable, arrayListPCollectionView);
		
		return resultPCollection;
	}
	
	private interface Options extends PipelineOptions{
//		@Description("Main table input")
//		@Default.String(mainTable);
//		String getInput();
//		void setInput(String input);
//
//		@Description("Side Table Input")
//		@Default.String(sideInputTable);
//		String getSideInput();
//		void setSideInput(String sideInput);
//
		@Description("Output destination")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
	}
}
