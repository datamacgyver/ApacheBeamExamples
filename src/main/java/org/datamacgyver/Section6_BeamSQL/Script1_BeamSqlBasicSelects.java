package org.datamacgyver.Section6_BeamSQL;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
import org.datamacgyver.Section3_Schemas.TransformersRecord;

public class Script1_BeamSqlBasicSelects {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        // We need the schema for the column names in SQL
        PCollection<TransformersRecord> transformers = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));

        //This defines a tuple tag, basically gives our Pcollection a name so we can refer to it in SQL
        PCollectionTuple transformersTuple = PCollectionTuple.of(new TupleTag<>("transformers"), transformers);

        //Note that we are again getting a row schema as an output.
        PCollection<Row> transformersSql = transformersTuple.apply(
                SqlTransform.query(
                        "SELECT name, combiner "
                                + "FROM transformers "
                                + "WHERE allegiance='Decepticon'"));

        transformersSql.apply("Preview grouped data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
        p.run().waitUntilFinish();
    }
}
