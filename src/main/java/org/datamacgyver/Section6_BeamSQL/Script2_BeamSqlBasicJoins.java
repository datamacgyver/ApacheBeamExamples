package org.datamacgyver.Section6_BeamSQL;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.datamacgyver.Section1_ReadFiles.InLineData;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
import org.datamacgyver.Section3_Schemas.TransformersRecord;


public class Script2_BeamSqlBasicJoins {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<TransformersRecord> transformers = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));  //Create Schema as normal, this lets us use schema notation for the group by

        PCollection<Row> episodes = p
                .apply("Get the inline episodes", Create.of(InLineData.episodesList).withCoder(RowCoder.of(InLineData.episodeRowSchema)));

        PCollectionTuple transformersTuple = PCollectionTuple
                .of(new TupleTag<>("transformers"), transformers)
                .and(new TupleTag<>("episodes"), episodes);

        //Note that we are again getting a row schema, it's good enough.
        PCollection<Row> transformersSql = transformersTuple.apply(
                SqlTransform.query(
                        "SELECT transformers.*,  episodes.EpisodeTitle " +
                                "FROM transformers " +
                                "INNER JOIN episodes " +
                                "ON transformers.firstApperanceSeason = episodes.Season " +
                                "AND transformers.firstApperanceEpisode = episodes.Episode"));

        // You can also use an interactive SQL shell: https://beam.apache.org/documentation/dsls/sql/shell/
        // There's a few more bits available here: https://beam.apache.org/documentation/dsls/sql/overview/

        transformersSql.apply("Preview grouped data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }
}
