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

            // The data read in is from Section 1.
            PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet));

            // This is the schema transform from Script 2 of this section.
            PCollection<TransformersRecord> transformersIn = readParquet.apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));

            //Now we have a schema, Beam is aware of my field names so I can use this to perform operations. This feels
            // a bit hand wavy in places, especially as it doesn't return data in a Schema, merely a generic Row record.
            // but it's definately useful for chaining ELT together before writng to either a new schema or an output
            // location.
            // Note that some of functions have multiple forms in beam, you want those under `org.apache.beam.sdk.schemas.transforms...`
            transformersIn
                    //I've also started chaining my .apply methods here, this is perfectly valid and makes the code much nicer to navigate
                    .apply("Filter Records", Filter.<TransformersRecord>create().whereFieldName("name", n -> n.equals("Grimlock")))
                    .apply("Add Columns", AddFields.<TransformersRecord>create().field("ExampleField", FieldType.STRING, "This is new information"))
                    .apply("Select Columns", Select.fieldNames("name", "alternateForm", "ExampleField"))
                    .apply("Preview altered schema data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }))
                    ;
            //This just scratches the surface! You can also do aggregates (see Section 5) and joins (see Script 4; but
            // personally I prefer to us a KV pair for joins, as in Section 4).
            p.run().waitUntilFinish();
        }
}

