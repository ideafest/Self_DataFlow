package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class LocalWordCount {

	static class ExtractWords extends DoFn<String, String>{
		@Override
		public void processElement(ProcessContext context){
			String[] words = context.element().split("[^a-zA-Z']+");
			for(String word : words){
				if(!word.isEmpty()){
					context.output(word);
				}
			}
		}
	}

	
	static class FormatText extends DoFn<KV<String, Long>, String>{
		@Override
		public void processElement(ProcessContext context){
			context.output(context.element().getKey() +" : "+context.element().getKey());
		}
	}
	
	public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
		@Override
		public PCollection<KV<String, Long>> apply(PCollection<String> lines){
			PCollection<String> words = lines.apply(ParDo.of(new ExtractWords()));
			return words.apply(Count.<String>perElement());
		}
	}
	
	public static void main(String[] args) {
		DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		
		pipeline.apply(TextIO.Read.named("Reader").from("/home/zimetrics/Documents/text.txt"))
				.apply(new CountWords())
				.apply(ParDo.of(new FormatText()))
				.apply(TextIO.Write.named("Writer").to("/home/zimetrics/Documents/staging_Loc/output_local_text.txt"));
		
		pipeline.run();
				
	}

}
