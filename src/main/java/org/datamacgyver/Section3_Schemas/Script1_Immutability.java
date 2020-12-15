package org.datamacgyver.Section3_Schemas;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;

public class Script1_Immutability {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet));

        //We've already seen that getting data out of a GenericRecord can be problematic. We also have the additional problem that *REcords are immutable* that is, they cannot be modified as part of any operation.
        //Therefore to update a value you need to make a new one. See, for example, this operation, which will raise with an error about changing unputs.
        readParquet.apply("Fill nulls", MapElements.via(new FillNulls()));
        p.run().waitUntilFinish();

        //For it to work I'd need to make a new Generic Record and add in the old values, if I was doing that, I may as well setup a proper Beam schema which comes with side-benefits....
    }

    public static class FillNulls extends SimpleFunction<GenericRecord, GenericRecord> {
        @Override public GenericRecord apply(GenericRecord r) {
            if (r.get("Combiner") == null){
                r.put("Combiner", "None");
            }
            return r;
        }
    }
}