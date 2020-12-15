package org.datamacgyver.Schemas3;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class Immutability1 {

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