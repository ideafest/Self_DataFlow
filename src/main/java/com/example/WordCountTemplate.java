package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class WordCountTemplate {
	static class ExtractWordsFn extends DoFn<String, String> {
		private final Aggregator<Long, Long> emptyLines =
				createAggregator("emptyLines", new Sum.SumLongFn());
		
		@Override
		public void processElement(ProcessContext c) {
			if (c.element().trim().isEmpty()) {
				emptyLines.addValue(1L);
			}
			
			String[] words = c.element().split("[^a-zA-Z']+");
			
			for (String word : words) {
				if (!word.isEmpty()) {
					c.output(word);
				}
			}
		}
	}
	
	public static class FormatAsTextFn extends DoFn<KV<String, Long>, String> {
		@Override
		public void processElement(ProcessContext c) {
			c.output(c.element().getKey() + ": " + c.element().getValue());
		}
	}
	
	public static class CountWords extends PTransform<PCollection<String>,
			PCollection<KV<String, Long>>> {
		@Override
		public PCollection<KV<String, Long>> apply(PCollection<String> lines) {
			
			PCollection<String> words = lines.apply(
					ParDo.of(new ExtractWordsFn()));
			
			PCollection<KV<String, Long>> wordCounts =
					words.apply(Count.<String>perElement());
			
			return wordCounts;
		}
	}
	
	
	public interface WordCountOptions extends PipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://learning001/files/kinglear.txt")
		String getInputFile();
		void setInputFile(String value);
		
		@Description("Path of the file to write to")
		@Default.InstanceFactory(WordCountOptions.OutputFactory.class)
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
		WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(WordCountOptions.class);
		Pipeline p = Pipeline.create(options);
		
		p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile()))
				.apply(new WordCount.CountWords())
				.apply(ParDo.of(new WordCount.FormatAsTextFn()))
				.apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));
		
		p.run();
	}
	
}
