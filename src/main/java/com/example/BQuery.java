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
	
	private static final String query = "SELECT\n" +
			"  word,\n" +
			"  COUNT(word) as wordCount\n" +
			"FROM\n" +
			"  [zimetrics:Learning.shakespeare_copy]\n" +
			"GROUP BY\n" +
			"  word\n" +
			"LIMIT\n" +
			"  1000";
	private static final String sourceTable = "zimetrics:Learning.shakespeare_copy";

	private static class ExtractWords extends DoFn<TableRow, String>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String word = (String) row.get("word");
			context.output(word);
		}
	}
	
	private static class ExtractAndSend extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element());
		}
	}
	
	public static class doThatOperation extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
		@Override
		public PCollection<TableRow> apply(PCollection<TableRow> inputTable){
			PCollection<String> rows = inputTable.apply(ParDo.of(new ExtractWords()));
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
		
		DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
		
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		
		List<TableFieldSchema> schemaList = new ArrayList<>();
		schemaList.add(new TableFieldSchema().setName("word").setType("STRING"));
		schemaList.add(new TableFieldSchema().setName("wordCount").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(schemaList);
		
		pipeline.apply(BigQueryIO.Read.fromQuery(query))
				.apply(ParDo.of(new ExtractAndSend()))
				.apply(BigQueryIO.Write.to("zimetrics:Learning.word_count")
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		
		pipeline.run();
		
	}
	
}
