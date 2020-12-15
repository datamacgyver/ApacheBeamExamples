package org.datamacgyver.Section5_ReduceOperations;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.transforms.Cast;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet2;
import org.datamacgyver.Section3_Schemas.TransformersRecord;

public class GroupByUsingSchemas4 {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(ReadingDataParquet2.avroSchema).from(inFileParquet));

        //Note that teh type here is now "Row". This is what Beam calls a record set that has a schema (it knows what it is as we created the original) but it doesn't have an official name.
        //Row contains all the variable names and things that we had before but it's got different fields so can't be called a "TransformersRecord" anymore.
        //This. Is the problem. Not having access to all the loveliness that a schema provides you means you may struggle with later operations (or simply have to learn
        //how to do things differently.
        //In my head, this form of notation is great for simple transforms that you can perform on the data after you've done the heavy hygeine
        PCollection<Row> transformersGrouped = readParquet
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()))  //Create Schema as normal, this lets us use schema notation for the group by
                .apply(Group.<TransformersRecord>byFieldNames("combiner")  //Value to group on
                        .withKeyField("combinerName")     //name to give the key field
                        .withValueField("transformer"));  //name to give the value field (it'll be the same type as that given by the <>

        transformersGrouped.apply("Preview grouped data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }
}