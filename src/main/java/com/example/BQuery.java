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

public class BQuery {
	
	private static final String query = "SELECT word  FROM [zimetrics:Learning.shakespeare_copy] LIMIT 20";
	private static final String sourceTable = "zimetrics:Learning.shakespeare_copy";
	
	private interface Options extends PipelineOptions{
		@Description("Input path")
		@Default.String(sourceTable)
		String getInput();
		void setInput(String value);
	}
	
	private static class ExctractWords extends DoFn<TableRow, String>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String word = (String) row.get("word");
			context.output(word);
		}
	}
	
	
	public static class doThatOperation extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
		@Override
		public PCollection<TableRow> apply(PCollection<TableRow> inputTable){
			PCollection<String> rows = inputTable.apply(ParDo.of(new ExctractWords()));
			PCollection<KV<String, Long>> counted = rows.apply(Count.<String>perElement());
			PCollection<TableRow> returnThisVar = counted.apply(ParDo.of(new DoFn<KV<String, Long>, TableRow>() {
				@Override
				public void processElement(ProcessContext context) throws Exception {
					TableRow row = new TableRow()
							.set("word", context.element().getKey())
							.set("wordCount", context.element().getValue());
					context.output(row);
				}
			}));
			
			return returnThisVar;
		}
	}
	
	
	public static void main(String[] args) {
		
		Options pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		
		List<TableFieldSchema> schemaList = new ArrayList<>();
		schemaList.add(new TableFieldSchema().setName("word").setType("STRING"));
		schemaList.add(new TableFieldSchema().setName("wordCount").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(schemaList);
		
		pipeline.apply(BigQueryIO.Read.fromQuery(query))
			//	.apply(new doThatOperation())
				.apply(BigQueryIO.Write.to("zimetrics:Learning.word_count")
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		
		pipeline.run();
		
	}
	
}
