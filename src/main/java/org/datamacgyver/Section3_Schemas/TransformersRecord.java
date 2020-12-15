package org.datamacgyver.Section3_Schemas;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.With;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.SimpleFunction;

//There's other ways to do this, as detailed in the beam docs. This is the one I've come to prefer.
@DefaultSchema(JavaBeanSchema.class)
@Data //The beam docs suggest you use XXX for making sure everything is setup right, I like Lombok which adds your getters,setters, hashes, tostring etc for you.
@DefaultCoder(AvroCoder.class)  //This gives a nudge to beam that we want to use the avro coder TODO: Add links to what coders are here.
@RequiredArgsConstructor(onConstructor=@__({@SchemaCreate}))     //I need to specify the beam annotation on the constructor so I can add the SchemaCreate (Beam needs this to know that it's a schema class)
@NoArgsConstructor(force = true)  //This allows us to create a no argument constructor. This allows us to call TransformersRecord.class without issue (thus allowing us to use lambdas!!!). force means that the no args constructor sets all finals to false
// TODO: Add the above info to lambda function stuff.
public class TransformersRecord{

    // With is a lombok function. As all the variables are final, with lets me update them by returning a new record object
    @With private final String name;
    @With private final String alternateForm;
    @With private final String combiner;
    @With private final String allegiance;
    @With private final int firstApperanceSeason;
    @With private final int firstApperanceEpisode;



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
