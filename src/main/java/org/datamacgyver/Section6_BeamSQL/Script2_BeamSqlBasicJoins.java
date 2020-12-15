package org.datamacgyver.Section6_BeamSQL;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
import org.datamacgyver.Section3_Schemas.TransformersRecord;

public class Script2_BeamSqlBasicJoins {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<TransformersRecord> transformers = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));  //Create Schema as normal, this lets us use schema notation for the group by

        PCollection<TransformersRecord> decepticons = transformers.apply(Filter.<TransformersRecord>create().whereFieldName("allegiance", c -> c.equals("Decepticon")));
        PCollection<TransformersRecord> autobots = transformers.apply(Filter.<TransformersRecord>create().whereFieldName("allegiance", c -> c.equals("Autobot")));

        PCollectionTuple transformersTuple = PCollectionTuple
                .of(new TupleTag<>("decepticon"), decepticons)
                .and(new TupleTag<>("autobot"), autobots);

        //Note that we are again getting a row schema, it's good enough.
        PCollection<Row> transformersSql = transformersTuple.apply(
                SqlTransform.query(
                        "SELECT decepticon.* " +
                                "FROM decepticon " +
                                "INNER JOIN autobot " +
                                "ON decepticon.firstApperanceSeason = autobot.firstApperanceSeason"));

        // You can also use an interactive SQL shell: https://beam.apache.org/documentation/dsls/sql/shell/
        // There's a few more bits available here: https://beam.apache.org/documentation/dsls/sql/overview/

        transformersSql.apply("Preview grouped data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }
}
