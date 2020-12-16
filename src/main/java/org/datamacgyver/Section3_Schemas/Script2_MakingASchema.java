package org.datamacgyver.Section3_Schemas;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;

public class Script2_MakingASchema {

        public static void main(String[] args) {
            String inFileParquet = "data/transformers.parquet";
            Pipeline p = Pipeline.create();

            //We read our generic records as normal...
            PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet));

            //Then we apply a SimpleFunction to make a TransformersRecord. At this point, please go to the
            // TransformersRecord Class that's in this directory to see how it works! If you are in IntelliJ just
            // Ctrl+left click.
            PCollection<TransformersRecord> transformersIn = readParquet.apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));

            //This will use my tostring function that lombok has made me.
            transformersIn.apply("Preview schema data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

            p.run().waitUntilFinish();
        }
}


