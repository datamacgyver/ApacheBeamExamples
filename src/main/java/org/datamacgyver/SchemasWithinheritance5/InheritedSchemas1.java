package org.datamacgyver.SchemasWithinheritance5;

import lombok.Data;
import lombok.With;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class InheritedSchemas1 {

    static String schemaJSON =
            "{\"namespace\": \"ioitavro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"TransformersSchema\",\n"
            + " \"fields\": [\n"
                + "{\"name\": \"Name\", \"type\": \"string\"},\n"
                + "{\"name\": \"AlternateForm\", \"type\": \"string\"},\n"
                + "{\"name\": \"Combiner\", \"type\": [\"string\", \"null\"]},\n"  //TODO: Note the nullable here
                + "{\"name\": \"allegiance\", \"type\": \"string\"},\n"
                + "{\"name\": \"FirstApperanceSeason\", \"type\": \"int\"},\n"
                + "{\"name\": \"FirstApperanceEpisode\", \"type\": \"int\"}\n"
            + " ]\n"
            + "}";

    static Schema avroSchema = new Schema.Parser().parse(schemaJSON);

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(avroSchema).from(inFileParquet));
        readParquet.apply("Preview parquet data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

        p.run().waitUntilFinish();
    }
}
