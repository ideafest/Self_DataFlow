package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

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
	
	
	static class ExtractFromGS extends DoFn<String, String>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element());
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
	private interface ExtractOptions extends PipelineOptions{
		@Description("Source")
		@Default.String("gs://learning001/output/CSV/CSV_output-00000-of-00002")
		String getInput();
		void setInput(String value);
		
		@Description("Destination")
		@Default.InstanceFactory(OutputFactory.class)
		String getOutput();
		void setOutput(String value);
		
		class OutputFactory implements DefaultValueFactory<String> {
			@Override
			public String create(PipelineOptions options) {
				DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
				if (dataflowOptions.getStagingLocation() != null) {
					return GcsPath.fromUri(dataflowOptions.getStagingLocation())
							.resolve("counts.txt").toString();
				} else {
					throw new IllegalArgumentException("Must specify --output or --stagingLocation");
				}
			}
		}
		
	}
	
	
	public static void main(String[] args) {
//		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//		Pipeline pipeline = Pipeline.create(options);

		ExtractOptions extractOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExtractOptions.class);
		Pipeline pipeline1 =Pipeline.create(extractOptions);
		
//		pipeline.apply(BigQueryIO.Read.named("Reading table").from(options.getInput()))
//				.apply(ParDo.of(new ExtractFromTable()))
//				.apply(TextIO.Write.named("Writing to GCS").to(options.getOutput()));
		
		pipeline1.apply(TextIO.Read.from(extractOptions.getInput()))
				.apply(ParDo.of(new ExtractFromGS()))
				.apply(TextIO.Write.to(extractOptions.getInput()));
		
//		pipeline.run();
		pipeline1.run();
	}
}
