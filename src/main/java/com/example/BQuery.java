package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BQuery {
	
	
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
	
	private static interface Options extends PipelineOptions {
		@Description("Input path to file containing query")
		String getInputPath();
		void setInputPath(String inputPath);
		
		@Description("Output Path")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) throws IOException {
		
		Options pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		
		List<TableFieldSchema> schemaList = new ArrayList<>();
		schemaList.add(new TableFieldSchema().setName("word").setType("STRING"));
		schemaList.add(new TableFieldSchema().setName("wordCount").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(schemaList);
		
		pipeline.apply(BigQueryIO.Read.fromQuery(readFile(pipelineOptions.getInputPath())))
				.apply(ParDo.of(new ExtractAndSend()))
				.apply(BigQueryIO.Write.to(pipelineOptions.getOutput())
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		
		pipeline.run();
		
	}
	
	
	
	public static String readFile(String filePath) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			return sb.toString();
		} finally {
			br.close();
		}
	}
}
