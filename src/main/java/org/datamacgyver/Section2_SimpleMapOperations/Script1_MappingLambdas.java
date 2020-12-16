package org.datamacgyver.Section2_SimpleMapOperations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class Script1_MappingLambdas {

    public static void main(String[] args){
        String inFileCsv = "data/transformers.csv";
        Pipeline p = Pipeline.create();

        //I'm using the CSV record here as it's pretty easy to work with, each record is simply a string!
        PCollection<String> readCsv = p.apply("Read CSV files", TextIO.read().from(inFileCsv));


        //It should be noted that you've already seen a lambda in section one!
        readCsv.apply("Preview csv data",
                MapElements                                                         //Map records one to another, we don't create new records or drop any. If you had 100 to start with, you will have 100 at the end!
                        .into(TypeDescriptors.strings())                            //Lambdas erase the types so you need to tell the Map function what you are returning. I find this messy and I often get nonsensical exceptions.
                        .via(record -> {System.out.println(record); return "";})    //This is the lambda function to apply, for each record: print it and then return an empty string.
        );

        //Let's do something more interesting though. For example, our csv is read in as one record per line, it needs splitting on commas!
        PCollection<List<String>> splitRows = readCsv
                .apply("Split on comma",
                        MapElements
                        .into(TypeDescriptors.lists(TypeDescriptors.strings()))    // This time we are outputting a list of strings (these get complicated!)
                        .via((String Row) -> Arrays.asList(Row.split(","))) // Return an arrayList made from splitting on a comma. Note we have to specify out input type here too
                );

        // This can be useful for quick jobs which is why I use it for these print statements. You can use a lambda
        // with more complex schemas too but we will come to that in the next chapter!
        // Word of warning though, you can get really weird errors with these. For exmaple, if you change the splitRows
        // code above to something like `.into(TypeDescriptors.strings())` (Basically remove the list type) then
        // you get the error "cannot resolve method via" which is not very intuitive.

        splitRows.apply("Preview split data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }

}
