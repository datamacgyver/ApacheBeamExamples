package org.datamacgyver.Schemas3;

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

public class MakingASchema2 {
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
            PCollection<TransformersRecord> transformersIn = readParquet.apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));

            //This will use my tostring function that lombok has made me.
            transformersIn.apply("Preview schema data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

            p.run().waitUntilFinish();
        }
}


//There's other ways to do this, as detailed in the beam docs. This is the one I've come to prefer.
@DefaultSchema(JavaBeanSchema.class)
//The beam docs suggest you use XXX for making sure everything is setup right, I like Lombok which adds your getters,setters, hashes, tostring etc for you.
@Data
class TransformersRecord{

    // With is a lombok function. As all the variables are final, with lets me update them by returning a new record object
    @With private final String name;
    @With private final String alternateForm;
    @With private final String combiner;
    @With private final String allegiance;
    @With private final int firstApperanceSeason;
    @With private final int firstApperanceEpisode;

    //I need to manually specify the constructor so I can add the SchemaCreate annotation (Beam needs this to know that it's a
    //schema class)
    @SchemaCreate
    public TransformersRecord(String name, String alternateForm, String combiner, String allegiance,
                              int firstApperanceSeason, int firstApperanceEpisode) {
        this.name = name;
        this.alternateForm = alternateForm;
        this.combiner = combiner;
        this.allegiance = allegiance;
        this.firstApperanceSeason = firstApperanceSeason;
        this.firstApperanceEpisode = firstApperanceEpisode;
    }

    //Insert other ELT tasks here! You can add more expensive calculations that you don't want in the constructor,
    //standard transforms, anythign you fancy really.

    //This is a simple function which was introduced in the last section. Note that this one takes in a generic record and returns
    // a TransformerRecord (ie this). It allows you to put  all your hygeine and standardisation code in a logical place
    public static class MakeTransformerRecordFromGeneric extends SimpleFunction<GenericRecord, TransformersRecord> {
        @Override public TransformersRecord apply(GenericRecord r) {
            //This lets me do some basic hygeine on the record so we know what we are getting.
            String combiner;
            if (r.get("Combiner") == null)
                combiner = "None";
            else
                combiner = r.get("Combiner").toString();

            int firstApperanceSeason = Integer.parseInt(r.get("FirstApperanceSeason").toString());
            int firstApperanceEpisode = Integer.parseInt(r.get("FirstApperanceEpisode").toString());

            return new TransformersRecord(
                    r.get("Name").toString(),
                    r.get("AlternateForm").toString(),
                    combiner,
                    r.get("allegiance").toString(),
                    firstApperanceSeason,
                    firstApperanceEpisode
            );
        }
    }
}
