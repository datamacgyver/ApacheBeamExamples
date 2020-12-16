package org.datamacgyver.Section1_ReadFiles;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ReadingDataParquet {

    //Beam can't infer Parquet Schemas yet, as such we have to define them, you'd probably want this as
    //a JSON file in production but inline is fine here.
    static String avroSchemaJSON =
            "{\"namespace\": \"ioitavro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"TransformersSchema\",\n"
            + " \"fields\": [\n"
                + "{\"name\": \"Name\", \"type\": \"string\"},\n"
                + "{\"name\": \"AlternateForm\", \"type\": \"string\"},\n"
                + "{\"name\": \"Combiner\", \"type\": [\"string\", \"null\"]},\n"  //Note the nullable here as this column can be empty.
                + "{\"name\": \"allegiance\", \"type\": \"string\"},\n"
                + "{\"name\": \"FirstApperanceSeason\", \"type\": \"int\"},\n"
                + "{\"name\": \"FirstApperanceEpisode\", \"type\": \"int\"}\n"
            + " ]\n"
            + "}";

    public static Schema avroSchema = new Schema.Parser().parse(avroSchemaJSON);

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(avroSchema).from(inFileParquet));

        readParquet.apply("Preview parquet data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }
}