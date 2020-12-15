package org.datamacgyver.Section5_ReduceOperations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet2;
import org.datamacgyver.Section3_Schemas.TransformersRecord;

public class GroupByKey2 {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        //Yeah, I'm sick of defining teh initial read in seperately
        PCollection<TransformersRecord> transformers = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet2.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));  //Create Schema as normal, this lets us use schema notation for the group by

        //As the previous section
        PCollection<KV<String, TransformersRecord>> transformersKV = transformers
                .apply("Create Key Value Pairs", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(TransformersRecord.class)))
                        .via(r -> KV.of(r.getCombiner(), r)));

        //TODO: more comments
        PCollection<KV<String, Integer>> combinerCount = transformersKV
                .apply("Group by our key", GroupByKey.create())
                .apply("Combine our values", ParDo.of(new CombinerCount()))
                ;

        combinerCount.apply("Preview grouped data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }
}

//GroubByKey performs an initial collection phase where all teh values for a given key need to be collected on one worker.
//This means that your worker needs to be able to handle all the records for a given key *and* you need to start thinking
//about skew and hot keys. It does, however mean that there's very little overhead to the combination process. You just spin
//over the resulting iterator and get a result. Additionally, the iterators are lazy evaluated so can be quite efficient
//If you are in doubt about which reduce operation you need in the first instance, it's probably this one!!!!!
//TODO: More comments
class CombinerCount extends DoFn<KV<String, Iterable<TransformersRecord>>, KV<String, Integer>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<TransformersRecord>> recs, OutputReceiver<KV<String, Integer>> out) {
        Integer count = 0;
        Iterable<TransformersRecord> transformers = recs.getValue();
        for (TransformersRecord rec : transformers) {
            count++;
        }
        out.output(KV.of(recs.getKey(), count)); //TODO: Note that this can also be another object/schema
    }
}