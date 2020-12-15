package org.datamacgyver.Section1_ReadFiles;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ReadingCsvWritingParquet1 {

    public static void main(String[] args){
        String inFileCsv = "data/transformers.csv";
        Pipeline p = Pipeline.create();

        PCollection<String> readCsv = p.apply("Read CSV files", TextIO.read().from(inFileCsv));

        //Apologies for this. I need it to show you the read in. It's not yet important to understand (it is the next thing I'll explain!!!!!
        readCsv.apply("Preview csv data", MapElements.into(TypeDescriptors.strings()).via(x -> {System.out.println(x); return "";}));

        p.run().waitUntilFinish();
    }

}
