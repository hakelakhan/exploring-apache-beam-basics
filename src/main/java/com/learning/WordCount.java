package com.learning;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {

        Instant before = Instant.now();
        System.out.println("Starting pipeline");
        PipelineOptions options = PipelineOptionsFactory
                .create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> lines = pipeline.apply("readFromFile",
                TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"));
        PCollection<String> words = lines.apply (
                FlatMapElements.into(TypeDescriptors.strings())
                        .via( (String line) -> Arrays.asList(line.split(Constants.MATCH_WORDS_CONSISTING_OF_LETTERS)) )
        );
        PCollection<String> nonEmptyWords = words.apply(
                Filter.by ( (String word) -> !word.isEmpty())
        );
        PCollection<KV<String, Long>> countPerWord =  nonEmptyWords.apply(
                Count.perElement()
        );
        PCollection<String> formattedWordCount = countPerWord.apply( MapElements.into(TypeDescriptors.strings())
                .via(
                        (KV<String, Long> wordCount) -> String.format("%s = %d", wordCount.getKey(), wordCount.getValue()))
                );
        formattedWordCount.apply(
                TextIO.write().to("word-count")
        );
        pipeline.run().waitUntilFinish();
        Instant after = Instant.now();
        System.out.println("Pipeline execution finished in "+  Duration.between(after, before).toString() + " minutes");
    }
}
