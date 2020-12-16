package org.datamacgyver.Section5_ReduceOperations;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
import org.datamacgyver.Section3_Schemas.TransformersRecord;

public class Script4_GroupByUsingSchemas {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        //We have to apply a schema before we can do anything else here.
        PCollection<TransformersRecord> transformers = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()))
                ;

        //Ouptut 1: a list of rows per key.
        //I'm going to use the variable names in the TransformersRecord Schema to create a grouped KV pair from our
        // data. The output type will be a Row but it'll be a row in teh form of `KeyRow: [ValueRows]`
        // for a primer on Rows, see the readme for this section.
        //Also note that we need to use diamond notation on the Group so it knows what types it's dealing with.
        // that's not always in the documented examples.
        PCollection<Row> transformersGrouped = transformers
                .apply("Group Transformers by combiner",
                        Group.<TransformersRecord>byFieldNames("combiner")  //Value to group on
                        .withKeyField("combinerName")     //name to give the key field
                        .withValueField("transformer"));  //name to give the value field (it'll be the same type as that given by the <>

        transformersGrouped.apply("Preview grouped data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

        //Output 2: A count of rows per key.
        //There's lots of otehr stuff you can do. General aggregates such as counts and sums, joins, etc etc. The documentation is here:
        //https://beam.apache.org/documentation/programming-guide/#using-schemas but note that the examples given usually require some <> notation
        //to work! Here's a toy example:
        transformers
                .apply("Count transformers in combiner",
                        Group.<TransformersRecord>byFieldNames("combiner")
                        .aggregateField("name",             //Input field that we are aggregating
                                Count.combineFn(),                        //aggregate function
                                "Num. Transformers"))     //Output field name
                .apply("Preview aggregated data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }))
                ;


        p.run().waitUntilFinish();
    }
}