package org.datamacgyver.Section1_ReadFiles;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ReadingCsv {

    public static void main(String[] args){
        String inFileCsv = "data/transformers.csv";
        Pipeline p = Pipeline.create();

        PCollection<String> readCsv = p.apply("Read CSV files", TextIO.read().from(inFileCsv));

        readCsv.apply("Preview csv data", MapElements.into(TypeDescriptors.strings()).via(x -> {System.out.println(x); return "";}));
        p.run().waitUntilFinish();
    }

}
