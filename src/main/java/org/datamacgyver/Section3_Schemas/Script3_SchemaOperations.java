package org.datamacgyver.Section3_Schemas;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;

public class Script3_SchemaOperations {

        public static void main(String[] args) {
            String inFileParquet = "data/transformers.parquet";
            Pipeline p = Pipeline.create();

            //Note that I just import the Transformers record from the last section, you've already read that though, right?
            PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet));
            PCollection<TransformersRecord> transformersIn = readParquet.apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));

            transformersIn.apply("Preview schema data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

//            Now we have a schema, Beam is aware of my field names so I can use this to perform operations. This feels a bit hand wavy.
            //Note that some of these have multiple forms in beam, you want `org.apache.beam.sdk.schemas.transforms...`
            transformersIn
                    .apply("Filter Records", Filter.<TransformersRecord>create().whereFieldName("name", n -> n.equals("Grimlock")))
                    .apply("Select Columns", AddFields.<TransformersRecord>create().field("ExampleField", FieldType.STRING, "This is new information"))
                    .apply("Select Columns", Select.fieldNames("name", "alternateForm", "ExampleField"))
                    .apply("Preview altered schema data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }))
                    ;

            //I've also started chaining my .apply methods here, this is perfectly valid and makes the code much nicer to navigate IMHO. You should see the abomination that
            // I found in their wordcount example!
            // ((PCollection)((PCollection)((PCollection)p.apply("ReadLines", TextIO.read().from(options.getInputFile()))).apply(new WordCount.CountWords())).apply(MapElements.via(new WordCount.FormatAsTextFn()))).apply("WriteCounts", TextIO.write().to(options.getOutput()));

            p.run().waitUntilFinish();
        }
}

