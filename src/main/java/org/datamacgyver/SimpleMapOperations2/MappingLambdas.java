package org.datamacgyver.SimpleMapOperations2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class MappingLambdas {

    public static void main(String[] args){
        String inFileCsv = "data/transformers.csv";
        Pipeline p = Pipeline.create();

        PCollection<String> readCsv = p.apply("Read CSV files", TextIO.read().from(inFileCsv));

        //You've already seen one.
        readCsv.apply("Preview csv data",
                MapElements  //Map records one to another  //TODO: Note that I'm not saying rows?
                        .into(TypeDescriptors.strings())  //Lambdas erase the types so you need to tell it the return type. I find this messy and I often get exceptions.
                        .via(x -> {System.out.println(x); return "";})  //This is the lambda function to apply, X is each record //TODO: Rename X
        );

        //Let's do something more interesting though. For example, our csv is read in as one record per line, it needs splitting!
        PCollection<List<String>> splitRows = readCsv.apply("Split on comma",
                MapElements  //Map records one to another  //TODO: Note that I'm not saying rows?
                        .into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via((String x) -> Arrays.asList(x.split(",")))
        );
        // Seen how a lambda works, you've also seen how they can be a bit complex and verbose. You also need to make sure you get hte Type Descriptors right as if you don't you get
        // confusing errors. For exmaple, if you change the example above to something not right (for example just strings without the list) then it says it "cannot resolve method via"
        // which isn't where the problem is!!!!!!!!!
        // Nevertheless it can be useful for quick jobs which is why I use it for these print statements. You can use a lambda with more complex schemas too but we will come to that in the next chapter!

        splitRows.apply("Preview split data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

        p.run().waitUntilFinish();
    }

}
