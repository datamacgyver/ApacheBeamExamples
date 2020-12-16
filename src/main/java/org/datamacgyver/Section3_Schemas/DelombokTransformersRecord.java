package org.datamacgyver.Section3_Schemas;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.SimpleFunction;


//This is the De-lomboked version of TransformersRecord which let's you see how it'd look if you wrote it yourself.
// For an explanation of what is what in terms of Beam, see the original class in this directory.

@DefaultSchema(JavaBeanSchema.class)
@DefaultCoder(AvroCoder.class)
public class DelombokTransformersRecord {
    private final String name;
    private final String alternateForm;
    private final String combiner;
    private final String allegiance;
    private final int firstApperanceSeason;
    private final int firstApperanceEpisode;

    public DelombokTransformersRecord() {
        this.name = null;
        this.alternateForm = null;
        this.combiner = null;
        this.allegiance = null;
        this.firstApperanceSeason = 0;
        this.firstApperanceEpisode = 0;
    }

    @SchemaCreate
    public DelombokTransformersRecord(String name, String alternateForm, String combiner, String allegiance, int firstApperanceSeason, int firstApperanceEpisode) {
        this.name = name;
        this.alternateForm = alternateForm;
        this.combiner = combiner;
        this.allegiance = allegiance;
        this.firstApperanceSeason = firstApperanceSeason;
        this.firstApperanceEpisode = firstApperanceEpisode;
    }

    public String getName() {
        return this.name;
    }

    public String getAlternateForm() {
        return this.alternateForm;
    }

    public String getCombiner() {
        return this.combiner;
    }

    public String getAllegiance() {
        return this.allegiance;
    }

    public int getFirstApperanceSeason() {
        return this.firstApperanceSeason;
    }

    public int getFirstApperanceEpisode() {
        return this.firstApperanceEpisode;
    }

    public boolean equals(final Object o) {
        if (o == this) return true;
        if (!(o instanceof DelombokTransformersRecord)) return false;
        final DelombokTransformersRecord other = (DelombokTransformersRecord) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$name = this.getName();
        final Object other$name = other.getName();
        if (this$name == null ? other$name != null : !this$name.equals(other$name)) return false;
        final Object this$alternateForm = this.getAlternateForm();
        final Object other$alternateForm = other.getAlternateForm();
        if (this$alternateForm == null ? other$alternateForm != null : !this$alternateForm.equals(other$alternateForm))
            return false;
        final Object this$combiner = this.getCombiner();
        final Object other$combiner = other.getCombiner();
        if (this$combiner == null ? other$combiner != null : !this$combiner.equals(other$combiner)) return false;
        final Object this$allegiance = this.getAllegiance();
        final Object other$allegiance = other.getAllegiance();
        if (this$allegiance == null ? other$allegiance != null : !this$allegiance.equals(other$allegiance))
            return false;
        if (this.getFirstApperanceSeason() != other.getFirstApperanceSeason()) return false;
        if (this.getFirstApperanceEpisode() != other.getFirstApperanceEpisode()) return false;
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof DelombokTransformersRecord;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $name = this.getName();
        result = result * PRIME + ($name == null ? 43 : $name.hashCode());
        final Object $alternateForm = this.getAlternateForm();
        result = result * PRIME + ($alternateForm == null ? 43 : $alternateForm.hashCode());
        final Object $combiner = this.getCombiner();
        result = result * PRIME + ($combiner == null ? 43 : $combiner.hashCode());
        final Object $allegiance = this.getAllegiance();
        result = result * PRIME + ($allegiance == null ? 43 : $allegiance.hashCode());
        result = result * PRIME + this.getFirstApperanceSeason();
        result = result * PRIME + this.getFirstApperanceEpisode();
        return result;
    }

    public String toString() {
        return "DelombokTransformersRecord(name=" + this.getName() + ", alternateForm=" + this.getAlternateForm() + ", combiner=" + this.getCombiner() + ", allegiance=" + this.getAllegiance() + ", firstApperanceSeason=" + this.getFirstApperanceSeason() + ", firstApperanceEpisode=" + this.getFirstApperanceEpisode() + ")";
    }

    public DelombokTransformersRecord withName(String name) {
        return this.name == name ? this : new DelombokTransformersRecord(name, this.alternateForm, this.combiner, this.allegiance, this.firstApperanceSeason, this.firstApperanceEpisode);
    }

    public DelombokTransformersRecord withAlternateForm(String alternateForm) {
        return this.alternateForm == alternateForm ? this : new DelombokTransformersRecord(this.name, alternateForm, this.combiner, this.allegiance, this.firstApperanceSeason, this.firstApperanceEpisode);
    }

    public DelombokTransformersRecord withCombiner(String combiner) {
        return this.combiner == combiner ? this : new DelombokTransformersRecord(this.name, this.alternateForm, combiner, this.allegiance, this.firstApperanceSeason, this.firstApperanceEpisode);
    }

    public DelombokTransformersRecord withAllegiance(String allegiance) {
        return this.allegiance == allegiance ? this : new DelombokTransformersRecord(this.name, this.alternateForm, this.combiner, allegiance, this.firstApperanceSeason, this.firstApperanceEpisode);
    }

    public DelombokTransformersRecord withFirstApperanceSeason(int firstApperanceSeason) {
        return this.firstApperanceSeason == firstApperanceSeason ? this : new DelombokTransformersRecord(this.name, this.alternateForm, this.combiner, this.allegiance, firstApperanceSeason, this.firstApperanceEpisode);
    }

    public DelombokTransformersRecord withFirstApperanceEpisode(int firstApperanceEpisode) {
        return this.firstApperanceEpisode == firstApperanceEpisode ? this : new DelombokTransformersRecord(this.name, this.alternateForm, this.combiner, this.allegiance, this.firstApperanceSeason, firstApperanceEpisode);
    }

    public static class MakeTransformerRecordFromGeneric extends SimpleFunction<GenericRecord, DelombokTransformersRecord> {
        @Override public DelombokTransformersRecord apply(GenericRecord r) {
            String combiner;
            if (r.get("Combiner") == null)
                combiner = "None";
            else
                combiner = r.get("Combiner").toString();

            int firstApperanceSeason = Integer.parseInt(r.get("FirstApperanceSeason").toString());
            int firstApperanceEpisode = Integer.parseInt(r.get("FirstApperanceEpisode").toString());

            return new DelombokTransformersRecord(
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
