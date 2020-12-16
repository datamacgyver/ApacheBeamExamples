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

        PCollection<TransformersRecord> transformers = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));

        //As in Script 1
        PCollection<KV<String, TransformersRecord>> transformersKV = transformers
                .apply("Create Key Value Pairs", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(TransformersRecord.class)))
                        .via(r -> KV.of(r.getCombiner(), r)));

        //Note that Combine doesn't require that first groupBy phase in Script 2. As always, the magic is in the class
        PCollection<KV<String, String>> combinerCount = transformersKV
                .apply(Combine.perKey(new TransformerAggregatorFn()))
                ;

        combinerCount.apply("Preview grouped data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }

    //This one is pretty cool. What it does is calculate each result locally, without the need for getting all the keys
    // in one place, it then merges all the separate accumulators and outputs a final answer, hence the need for three
    // input types: Input type, accumulator type and output type.
    //I've used ints and string here but there's nothing to stop you doing something more complex such as having
    // schema objects on output.
    //This type can be faster at scale (but slower on smaller datasets). It also allows hot-keys to be parellelised,
    // thus removing the need to care about skew. It does, however, require that the interim results be sent over
    // the wire, therefore keep your objects small and please, please, please don't grow lists!
    static class TransformerAggregatorFn extends Combine.CombineFn<TransformersRecord, Integer, String> {  //<Input type, accumulator type, output type>

        @Override
        public Integer createAccumulator() {                                        //Instansiates an accumulator object
            return 0;
        }

        @Override
        public Integer addInput(Integer accumulator, TransformersRecord input) {    //Adds a new record to the accumulator and does something.
            accumulator++;
            return accumulator;
        }

        @Override
        public Integer mergeAccumulators(Iterable<Integer> accumulators) {          //Once all the accumulation has been done locally, the seperate accumulators need to be merged.
            Integer total = 0;
            for (Integer accumulator : accumulators) {
                total += accumulator;
            }
            return total;
        }

        @Override
        public String extractOutput(Integer accumulator) {                          //You then extract the required output from the merged accumulators.
            return "Number of transformers: " + accumulator;
        }


    }
}
