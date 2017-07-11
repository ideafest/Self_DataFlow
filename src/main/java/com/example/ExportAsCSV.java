package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

public class ExportAsCSV {
	
	private static final String sourceBQTable = "zimetrics:Learning.shakespeare_copy";
	private static final String destinationGCS = "gs://learning001/output/CSV/CSV_output";
	
	static class ExtractFromTable extends DoFn<TableRow, String>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String word = (String) row.get("word");
			String word_count = (String) row.get("word_count");
			String corpus = (String) row.get("corpus");
			String corpus_date = (String) row.get("corpus_date");
			
			context.output(word + ", " + word_count + ", " + corpus + ", " + corpus_date);
		}
	}
	
	private interface Options extends PipelineOptions{
		@Description("Source")
		@Default.String(sourceBQTable)
		String getInput();
		void setInput(String value);
		
		@Description("Destination")
		@Default.String(destinationGCS)
		String getOutput();
		void setOutput(String value);
	}
	
	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		pipeline.apply(BigQueryIO.Read.named("Reading table").from(options.getInput()))
				.apply(ParDo.of(new ExtractFromTable()))
				.apply(TextIO.Write.named("Writing to GCS").to(options.getOutput()));
		
		pipeline.run();
	}
}
