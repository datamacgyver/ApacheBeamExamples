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
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
import org.datamacgyver.Section3_Schemas.TransformersRecord;

public class Script2_GroupByKey {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<TransformersRecord> transformers = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));

        //As in Script 1
        PCollection<KV<String, TransformersRecord>> transformersKV = transformers
                .apply("Create Key Value Pairs", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(TransformersRecord.class)))
                        .via(r -> KV.of(r.getCombiner(), r)));

        //Now we have our KV pairs we can operate on the keys. As in previous examples, the magic is in the class we
        // call but note that we have two stages:
        PCollection<KV<String, Integer>> combinerCount = transformersKV
                .apply("Group by our key", GroupByKey.create())                 // Group by our key
                .apply("Combine our values", ParDo.of(new CombinerCount()))     // Combine grouped values
                ;

        combinerCount.apply("Preview grouped data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }

    //GroubByKey performs an initial collection phase where all teh values for a given key need to be collected on one worker.
    //This means that your worker needs to be able to handle all the records for a given key *and* you need to start thinking
    //about skew and hot keys. It does, however, mean that there's very little overhead to the combination process. You just spin
    //over the resulting iterator and get a result. Additionally, the iterators are lazy evaluated so can be quite efficient.
    //This is probably a good place to start for reduce operations!
    static class CombinerCount extends DoFn<KV<String, Iterable<TransformersRecord>>, KV<String, Integer>> {  //Note that this is just a special case of a DoFn where the input type is a K/V pair of key/iterator.

        @ProcessElement
        public void processElement(@Element KV<String, Iterable<TransformersRecord>> recs, OutputReceiver<KV<String, Integer>> out) {
            Integer count = 0;
            Iterable<TransformersRecord> transformers = recs.getValue();
            for (TransformersRecord rec : transformers) {                       // For each value in our grouped key, do something.
                count++;
            }
            out.output(KV.of(recs.getKey(), count)); //Output the result which can also be another schema. I've used this to, for example, only return the newest record with that key.
        }
    }
}

