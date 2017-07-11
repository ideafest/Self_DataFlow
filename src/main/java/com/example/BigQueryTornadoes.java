package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

public class BigQueryTornadoes {
	
	private static final String WEATHER_SAMPLES_TABLE =	"clouddataflow-readonly:samples.weather_stations";
	
	static class ExtractTornadoesFn extends DoFn<TableRow, Integer> {
		@Override
		public void processElement(ProcessContext c){
			TableRow row = c.element();
			if ((Boolean) row.get("tornado")) {
				c.output(Integer.parseInt((String) row.get("month")));
			}
		}
	}
	
	static class FormatCountsFn extends DoFn<KV<Integer, Long>, TableRow> {
		@Override
		public void processElement(ProcessContext c) {
			TableRow row = new TableRow()
					.set("month", c.element().getKey())
					.set("tornado_count", c.element().getValue());
			c.output(row);
		}
	}
	
	static class CountTornadoes extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
		@Override
		public PCollection<TableRow> apply(PCollection<TableRow> rows) {
			
			PCollection<Integer> tornadoes = rows.apply(ParDo.of(new ExtractTornadoesFn()));
			
			PCollection<KV<Integer, Long>> tornadoCounts = tornadoes.apply(Count.<Integer>perElement());
			
			PCollection<TableRow> results = tornadoCounts.apply(ParDo.of(new FormatCountsFn()));
			
			return results;
		}
	}
	
	private interface Options extends PipelineOptions {
		@Description("Table to read from, specified as "+ "<project_id>:<dataset_id>.<table_id>")
		@Default.String(WEATHER_SAMPLES_TABLE)
		String getInput();
		void setInput(String value);
		
		@Description("BigQuery table to write to, specified as " + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}
	
	public static void main(String[] args) {
		
		
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		
		Pipeline p = Pipeline.create(options);
		
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("tornado_count").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(fields);
		
		p.apply(BigQueryIO.Read.from(options.getInput()))
				.apply(new CountTornadoes())
				.apply(BigQueryIO.Write
						.to(options.getOutput())
						.withSchema(schema)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		p.run();
		
	}
	
}