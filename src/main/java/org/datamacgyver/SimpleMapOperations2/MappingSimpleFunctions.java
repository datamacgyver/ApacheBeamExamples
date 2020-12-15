package org.datamacgyver.SimpleMapOperations2;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;


public class MappingSimpleFunctions {

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

    //TOD: I feel all these mains need to be at the top...
    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(avroSchema).from(inFileParquet));
        readParquet.apply("Preview parquet data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        //Note that, when we run the above, we get something that looks similar to a json string with all our fields and variables per record. It's pretty nice!

        //Because we have a schema here we can access the data by name which is helpful, it's a little clunky as you need to know the column names however, this also means that if you get a schema change it may not fail nicely!
        //Doing this would be pretty heavy in a lambda as you would have to keep using .get() so let's try it with a simple function. More verbose but, in my opinion, more stable and readable as you don't have to deal with the loss of the types.

        //TODO: can we work out how to add variables while we are here?
        PCollection<KV<String, String>> CombinerInfo = readParquet.apply("Find Combiner Mapping", MapElements.via(new GetCombinerName()));
        CombinerInfo.apply("Preview null filled data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

        p.run().waitUntilFinish();
    }


    public static class GetCombinerName extends SimpleFunction<GenericRecord, KV<String, String>> {  //This is <in type, out type>
        @Override public KV<String, String> apply(GenericRecord r) { //<out type>
            String combiner;
            if (r.get("Combiner") == null){
                combiner = "None";
            } else {
                combiner = r.get("Combiner").toString(); //Note the need for a cast here, and that it'll raise if null
            }
            String name = r.get("Name").toString();  //Safe to do as not nullable field

            return KV.of(combiner, name);
        }
    }

}