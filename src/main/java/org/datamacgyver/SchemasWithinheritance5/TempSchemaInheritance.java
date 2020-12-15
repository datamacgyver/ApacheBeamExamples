package org.datamacgyver.SchemasWithinheritance5;

import lombok.Data;
import lombok.With;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.SimpleFunction;

@DefaultSchema(JavaBeanSchema.class)
@Data
class TransformersRecord{

    @With
    private final String name;
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

@Data
class CombinerRecord extends TransformersRecord{


    @SchemaCreate
    public CombinerRecord(String name, String alternateForm, String combiner, String allegiance, int firstApperanceSeason, int firstApperanceEpisode) {
        super(name, alternateForm, combiner, allegiance, firstApperanceSeason, firstApperanceEpisode);
    }
}
