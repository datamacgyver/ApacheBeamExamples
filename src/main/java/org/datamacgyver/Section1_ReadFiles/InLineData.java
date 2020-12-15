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

import java.util.Date;

public class InLineData {

    public static void main(String[] args) {
        String inFileCsv = "data/transformers.csv";
        Pipeline p = Pipeline.create();

        // Define the schema for the records.
        Schema appSchema = Schema
                .builder()
                .addInt32Field("appId")
                .addStringField("description")
                .addDateTimeField("rowtime")
                .build();

        // Create a row with that type.
        Row row = Row
                .withSchema(appSchema)
                .addValues(1, "Some cool app", new Date())
                .build();

        // Create a source PCollection containing only that row. We will talk more about rows later.
        //This process is most useful for creating demo and test data.
        //TODO: Talk about rows.
        PCollection<Row> exampleData = p.apply(Create.of(row).withCoder(RowCoder.of(appSchema)));

        exampleData.apply("Preview created data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));

    }
}
