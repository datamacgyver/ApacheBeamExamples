package org.datamacgyver.Section5_ReduceOperations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
import org.datamacgyver.Section3_Schemas.TransformersRecord;

public class Script1_CreateKvPair {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        //Yeah, I'm sick of defining teh initial read in seperately
        PCollection<TransformersRecord> transformers = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));  //Create Schema as normal, this lets us use schema notation for the group by

        PCollection<KV<String, TransformersRecord>> transformersKV = transformers
                .apply("Create Key Value Pairs", MapElements  //Here we are using a lambda to create the
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(TransformersRecord.class)))  //TODO: Notes on this and make sure there's good stuff in the lambda function bit
                        .via(r -> KV.of(r.getCombiner(), r)))
                ;

        transformersKV.apply("Preview grouped data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }
}