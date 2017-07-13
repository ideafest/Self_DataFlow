package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.List;

public class SideView_v2 {
	
	private static final String tableID = "clouddataflow-readonly:samples.weather_stations";
	static final int MONTH_TO_FILTEr = 7;
	
	static class ProjectionFunction extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			Integer year = Integer.parseInt((String) row.get("year"));
			Integer month = Integer.parseInt((String) row.get("month"));
			Integer day = Integer.parseInt((String) row.get("day"));
			Double mean_temp = Double.parseDouble( row.get("mean_temp").toString());
			
			TableRow outRow = new TableRow().set("year", year).set("month", month).set("day", day).set("mean_temp", mean_temp);
			context.output(outRow);
		}
	}
	
	static class FilterSingleMonth extends DoFn<TableRow, TableRow>{
		
		Integer monthFilter;
		
		public FilterSingleMonth(Integer monthFilter) {
			this.monthFilter = monthFilter;
		}
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
		
			TableRow row = context.element();
			Integer month = (Integer) row.get("month");
			if(month.equals(monthFilter)){
				context.output(row);
			}
		}
	}
	
	static class ExtractTemp extends DoFn<TableRow, Double>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			Double meanTemp = Double.parseDouble(row.get("mean_temp").toString());
			context.output(meanTemp);
		}
	}
	
	static class BelowTheMean extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
		static Integer monthFilter;
		
		public BelowTheMean(Integer monthFilter) {
			this.monthFilter = monthFilter;
		}
		
		@Override
		public PCollection<TableRow> apply(PCollection<TableRow> rowPCollection){
			PCollection<Double> meanTemps = rowPCollection.apply(ParDo.of(new ExtractTemp()));
			
			final PCollectionView<Double> meanTempGlobal = meanTemps.apply(Mean.<Double>globally()).apply(View.<Double>asSingleton());
			PCollection<TableRow> monthFilteredRows = rowPCollection.apply(ParDo.of(new FilterSingleMonth(monthFilter)));
			PCollection<TableRow> filteredRows = monthFilteredRows
					.apply(ParDo.named("ParseAndFilter").withSideInputs(meanTempGlobal).of(new DoFn<TableRow, TableRow>() {
						@Override
						public void processElement(ProcessContext context) throws Exception {
							Double meanTemp = Double.parseDouble(context.element().get("mean_temp").toString());
							Double gTemp = context.sideInput(meanTempGlobal);
							if(meanTemp < gTemp){
								context.output(context.element());
							}
						};
					}));
			return filteredRows;
		}
	}
	
	private interface Options extends PipelineOptions{
		
		@Description("Table to read from, specified as "
				+ "<project_id>:<dataset_id>.<table_id>")
		@Default.String(tableID)
		String getInput();
		void setInput(String value);
		
		@Description("Table to write to, specified as "
				+ "<project_id>:<dataset_id>.<table_id>. "
				+ "The dataset_id must already exist")
		@Validation.Required
		String getOutput();
		void setOutput(String value);
		
		@Description("Numeric value of month to filter on")
		@Default.Integer(MONTH_TO_FILTEr)
		Integer getMonthFilter();
		void setMonthFilter(Integer value);
	}
	
	public static void main(String[] args) {
		List<TableFieldSchema> fieldSchemas = new ArrayList<>();
		fieldSchemas.add(new TableFieldSchema().setName("year").setType("INTEGER"));
		fieldSchemas.add(new TableFieldSchema().setName("month").setType("INTEGER"));
		fieldSchemas.add(new TableFieldSchema().setName("day").setType("INTEGER"));
		fieldSchemas.add(new TableFieldSchema().setName("mean_temp").setType("FLOAT"));
		TableSchema schema = new TableSchema().setFields(fieldSchemas);
		
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline= Pipeline.create(options);
		
		pipeline.apply(BigQueryIO.Read.named("Reader").from(options.getInput()))
				.apply(ParDo.of(new ProjectionFunction()))
				.apply(new BelowTheMean(options.getMonthFilter()))
				.apply(BigQueryIO.Write.named("Writer")
				.to(options.getOutput())
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition((BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)));
		
		pipeline.run();
	}
	
}
