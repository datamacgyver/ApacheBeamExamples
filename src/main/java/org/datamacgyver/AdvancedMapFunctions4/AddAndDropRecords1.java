package org.datamacgyver.AdvancedMapFunctions4;

import lombok.Data;
import lombok.With;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AddAndDropRecords1 {

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

        PCollection<TransformersRecord> transformersIn = p
                .apply("ReadLines field", ParquetIO.read(avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));

        //In this example, we are applying two sepearte transforms to the same input data. That's completely
        //fine and let's you do branching operations and transforms.

        //Transform 1:
        transformersIn
                .apply("Get Allegiance", ParDo.of(new ParseAllegiance()))
                .apply("Show ParseAllegiance results", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

        //Transform 2:
        transformersIn
                .apply("Defect Dinobots", ParDo.of(new DinobotsDefect()))
                .apply("Show Defection", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

        p.run().waitUntilFinish();
    }

    //Transform 1:
    //This is a very throwaway example but just take a moment to think about this. We could use the below
    // pattern to break one record into many, perform complex filtering or even return two different kinds
    // of record depending on the data in it (one for a later section).
    static class ParseAllegiance extends DoFn<TransformersRecord, String> {  //This is <in type, out type>

        // This per-row operations. Run once per element (row). The annotation tells beam that we are
        // going to use this to process each record and @Element tells it what the record is.
        @ProcessElement
        public void processElement(@Element TransformersRecord r, OutputReceiver<String> out) {  //You also need the output type in the Output reciever.
            //See that by using a schema we get the getters for each variable making things a little more intuitive.
            if (r.getAllegiance().toLowerCase().equals("autobot")){
                out.output(r.getName() + " is an Autobot");
                out.output(r.getName() + " is awesome");
            } else {
                //I could just as easily comment out this line and we wouldn't get any records back for non-autobots
                out.output("We don't talk about Decepticons");
            }
        }
    }

    //Transform 2:
    // The above uses a string output type, it could just as easily be the TransformersRecord, another Schema or
    // indeed any supported data type. For example:
    static class DinobotsDefect extends DoFn<TransformersRecord, TransformersRecord> {  //This is <in type, out type>

        // This per-row operations. Run once per element (row). The annotation tells beam that we are
        // going to use this to process each record and @Element tells it what the record is.
        @ProcessElement
        public void processElement(@Element TransformersRecord r, OutputReceiver<TransformersRecord> out) {  //You also need the output type in the Output reciever.
            //so here, we check if the name is in the Dinobots list. If it is then we set Allegiance to
            //Decepticon (this did happen btw, it's canon), otherwise we output nothing. Note that I use
            // with which returns a new copy of the object, rather than set. This is because we cannot
            // modify an input element.
            ArrayList<String> dinobots = new ArrayList<>(Arrays.asList("grimlock","slag","sludge","snarl","swoop"));
            if (dinobots.indexOf(r.getName().toLowerCase()) != -1){
                out.output(r.withAllegiance("Decepticons"));
            }
        }
    }
}



// This is copied from the Schemas section for reference. See there for comments and advice on how it works.
@DefaultSchema(JavaBeanSchema.class)
@Data
class TransformersRecord{

    @With private final String name;
    @With private final String alternateForm;
    @With private final String combiner;
    @With private final String allegiance;
    @With private final int firstApperanceSeason;
    @With private final int firstApperanceEpisode;

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

     public static class MakeTransformerRecordFromGeneric extends SimpleFunction<GenericRecord, TransformersRecord> {
        @Override public TransformersRecord apply(GenericRecord r) {
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