//From https://beam.apache.org/documentation/dsls/sql/walkthrough/

package org.datamacgyver.Section1_ReadFiles;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

public class InLineData {

        // Define the Row schema for the records. Note ROW SCHEMA.
        public static Schema episodeRowSchema = Schema
                .builder()
                .addInt32Field("Season")
                .addInt32Field("Episode")
                .addStringField("EpisodeTitle")
                .build();

        // Create a row with that type.
        public static Row episodesList = Row
                .withSchema(episodeRowSchema)
                .addValues(1, 4, "S.O.S. Dinobots")
                .build();

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        // Create a source PCollection containing only that row.
        PCollection<Row> exampleData = p.apply(Create.of(episodesList).withCoder(RowCoder.of(episodeRowSchema)));

        exampleData.apply("Preview created data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();

    }
}