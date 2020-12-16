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


//There's a lot to unpack in this class, it is bascially instructions to Beam as to how to create a schema for our data
// but I've added a few bits of Lombok to dramatically reduce the boiler plate! There's other ways to do this, as
// detailed in the beam docs. I got this one working and it's pretty adaptable.
//To see how this looks without Lombok, please see DelombokTransformersRecord which is also in this folder

@DefaultSchema(JavaBeanSchema.class)                            //This is a Beam annotation that tells Beam to get a schema from this object.
@DefaultCoder(AvroCoder.class)                                  //This is a Beam schema that nudges it to use the avro coder

@Data                                                           //Lombok annotation which adds constructors, getters, setters, hashes, tostring etc for you. It's awesome
@RequiredArgsConstructor(onConstructor=@__({@SchemaCreate}))    //Lombok annotation that overrides the @Data constructor for final types and adds the Beam @SchemaCreate annotation to it. This let's beam know it's the constructor for the schema.
@NoArgsConstructor(force = true)                                //Lombok annotation that also creates a no argument constructor with final objects set to null. This allows us to call TransformersRecord.class without raising an exception (and thus allowing us to use lambdas!)
public class TransformersRecord{

    // @With is a lombok annotation. As all the variables are final, with lets me update them by returning a new record object `TransformersRecord updated = withName("new Name");`
    // Variables are final as we can't update them anyway. I have used @Getter(lazy=true) for expensive variables without issue.
    @With private final String name;
    @With private final String alternateForm;
    @With private final String combiner;
    @With private final String allegiance;
    @With private final int firstApperanceSeason;
    @With private final int firstApperanceEpisode;

    ////////////////////////////////////////////////////////////
    //Insert other ELT tasks here! You can add more expensive calculations that you don't want in the constructor,
    //standard transforms, anything you fancy really.
    ////////////////////////////////////////////////////////////


    //This is a simple function (see section 2).
    // Note that this one takes in a generic record and returns a TransformerRecord (ie this object). It
    // basically outlines how to conver from one to the other and does some basic hygeine so we know what we are
    // getting.
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
