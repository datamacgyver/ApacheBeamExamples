package org.datamacgyver.Section5_ReduceOperations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
import org.datamacgyver.Section3_Schemas.TransformersRecord;

public class Script3_CombinePerKey {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        //Yeah, I'm sick of defining teh initial read in seperately
        PCollection<TransformersRecord> transformers = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));  //Create Schema as normal, this lets us use schema notation for the group by

        //As the previous section
        PCollection<KV<String, TransformersRecord>> transformersKV = transformers
                .apply("Create Key Value Pairs", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(TransformersRecord.class)))
                        .via(r -> KV.of(r.getCombiner(), r)));

        //TODO: more comments
        //Note I'm doing String values here, it's simply so we can show how the accumulator works, I didn't want to build it as <TransformersRecord, Integer, Integer> as it'd be harder to see what's going on.
        PCollection<KV<String, String>> combinerCount = transformersKV
//                .apply("Group by our key", GroupByKey.create())
//                .apply("Combine our values", ParDo.of(new CombinerCount()))
                //So we no longer need either of these lines, instead we just need the one:
                .apply(Combine.perKey(new TransformerAggregatorFn()))
                ;

        combinerCount.apply("Preview grouped data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }

    //TODO: More comments
    //TODO: I've moved these classes inside our main classes, I should probably add some documentation about that....
    //This one is pretty cool. What it does is calculate each result locally, without the need for getting all the keys in one place,
//It then merges all the seperate accumulators and outputs a final answer, hence the need for three input types: Input type, accumulator type and output type.
//I've used Ints and string here but there's nothing to stop you doing something more complex such as having schema objects on output.
//This type can be faster at scale (but slower on smaller datasets). It also allows hot-keys to be parellelised, thus removing the need to care
//about skew. It does, however require that the interim results be sent over the wire, therefore keep your objects small and please, please, please don't
//grow lists!
    static class TransformerAggregatorFn extends Combine.CombineFn<TransformersRecord, Integer, String> {  //<Input, accumulator, output>

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer addInput(Integer accumulator, TransformersRecord input) {
            accumulator++;
            return accumulator;
        }

        @Override
        public Integer mergeAccumulators(Iterable<Integer> accumulators) {
            Integer total = 0;
            for (Integer accumulator : accumulators) {
                total += accumulator;
            }
            return total;
        }

        @Override
        public String extractOutput(Integer accumulator) {
            return "Number of transformers: " + accumulator;
        }


    }
}
