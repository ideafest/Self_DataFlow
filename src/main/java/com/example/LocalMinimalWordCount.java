package com.example;


import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;

public class LocalMinimalWordCount {
	
	public static void main(String[] args) {
		
		DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		
		Pipeline pipeline =  Pipeline.create(pipelineOptions);
		
		pipeline.apply(TextIO.Read.from("/home/zimetrics/Documents/text.txt"))
				.apply(ParDo.named("LocalWordExtract").of(new DoFn<String, String>() {
					@Override
					public void processElement(ProcessContext context){
						for(String word : context.element().split("[^a-zA-Z']+")){
							if(!word.isEmpty()) {
								context.output(word);
							}
						}
					}
				}))
				.apply(Count.<String>perElement())
				.apply(ParDo.named("LocalFormatResults").of(new DoFn<KV<String ,Long>, String>() {
					@Override
					public void processElement(ProcessContext context){
						context.output(context.element().getKey() +" : "+ context.element().getValue());
					}
				}))
				.apply(TextIO.Write.to("/home/zimetrics/Documents/staging_Loc/output_text.txt"));
		
		pipeline.run();
		
	}
	
}
