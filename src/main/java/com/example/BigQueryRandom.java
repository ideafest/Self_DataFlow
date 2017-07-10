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

public class BigQueryRandom {

	private static final String source = "clouddataflow-readonly:samples.weather_stations";
	
	static class ExtractTornado extends DoFn<TableRow, Integer>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow rowElement = context.element();
			if((Boolean) rowElement.get("tornado")){
				context.output(Integer.parseInt((String)rowElement.get("month")));
			}
		}
	}
	
	static class FormatOutput extends DoFn<KV<Integer, Long>, TableRow>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = new TableRow().set("month", context.element().getKey())
					.set("tornado_count", context.element().getValue());
			context.output(row);
		}
	}
	
	static class CountTornadoes extends PTransform<PCollection<TableRow>, PCollection<TableRow>>{
		
		@Override
		public PCollection<TableRow> apply(PCollection<TableRow> rowElement){
			PCollection<Integer> tornadoes = rowElement.apply(ParDo.of(new ExtractTornado()));
			PCollection<KV<Integer, Long>> tornadoCount = tornadoes.apply(Count.<Integer>perElement());
			return tornadoCount.apply(ParDo.of(new FormatOutput()));
		}
	}
	
	private interface TableOptions extends PipelineOptions{
		@Description("Source for data")
		@Default.String(source)
		String getInput();
		void setInput(String value);
		
		@Description("Destination of data")
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}
	
	public static void main(String[] args) {

		TableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TableOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		
		List<TableFieldSchema> schemaList = new ArrayList<>();
		schemaList.add(new TableFieldSchema().setName("month").setType("INTEGER"));
		schemaList.add(new TableFieldSchema().setName("tornado_count").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(schemaList);
		
		pipeline.apply(BigQueryIO.Read.named("Reader").from(options.getInput()))
				.apply(new CountTornadoes())
				.apply(BigQueryIO.Write.named("Writer").to(options.getOutput())
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		
		pipeline.run();
				
	}

}
