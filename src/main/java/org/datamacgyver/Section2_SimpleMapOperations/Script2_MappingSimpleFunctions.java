package org.datamacgyver.Section2_SimpleMapOperations;


import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;


public class Script2_MappingSimpleFunctions {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        //Remember in section 1 when we visualised the below record, we got something that had nice key value pairs
        //We can use these pairs to access the data within each record. It's a little clunky but it's easily adapted to
        //new data.
        PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet));

        //Doing this would be pretty heavy in a lambda as you would have to keep using .get().toString() so let's try
        //it with a simple function. More verbose but, in my opinion, more stable and readable as you don't have to deal with
        //the loss of the types and you can put your actual code elsewhere.
        PCollection<KV<String, String>> CombinerInfo = readParquet
                .apply("Find Combiner Mapping", MapElements.via(new GetCombinerName()));

        CombinerInfo.apply("Print Combiner Info", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

        p.run().waitUntilFinish();
    }


    //A simple function can be defined inline in your pipeline (ewww!) but it can also just be a class stored anywhere
    //in your project. This function will parse the Generic record and return a key/value pair that tells you which
    //combiner each transformer creates.
    //NOte the use of diamond notation here, for example in the first line of the class we have
    //`SimpleFunction<GenericRecord, KV<String, String>>` which means <inputType, outputType>, in our case, that's
    //GenericRecord (our transformers data) and  KV<String, String>, a key/value pair of two strings.
    public static class GetCombinerName extends SimpleFunction<GenericRecord, KV<String, String>> {
        @Override                                                       //We @Override the apply method in SimpleFunction with what we want it to do.
        public KV<String, String> apply(GenericRecord r) {              //We also need the return type and input type here
            String combiner;
            if (r.get("Combiner") == null){                             //This is where we can see how clunky the GenericRecords can be. We have to .get() by field name and the return types can be null.
                combiner = "None";
            } else {
                combiner = r.get("Combiner").toString();                //...in addition, when you .get() a field you then need to cast it to a String to use it.
            }

            String name = r.get("Name").toString();                     //Safe to use raw as not nullable field in the avroSchema

            return KV.of(name, combiner);
        }
    }

}