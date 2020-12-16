package org.datamacgyver.Section4_advancedMapFunctions;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.datamacgyver.Section1_ReadFiles.InLineData;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
import org.datamacgyver.Section3_Schemas.TransformersRecord;

public class Script3_Joins {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        // You've seen this bit a lot already!
        PCollection<TransformersRecord> transformersIn = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));

        // The secondary data I'm using to join is the inLine demo data I used in Section 1. You can checkout it's schema there.
        PCollection<Row> episodes = p
                .apply("Get the inline episodes", Create.of(InLineData.episodesList).withCoder(RowCoder.of(InLineData.episodeRowSchema)));

        //First we need to create key value pairs for each of our records. You can do this in several ways, for example using schema notation
        //or a lambda/simple function. I'm using a lambda as that probably is the place where you need to see some more examples!
        PCollection<KV<String, String>> episodesKV = episodes
                .apply("Create episodes kv", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))                         //You can see how messy the TypeDescriptors can get here!
                        .via(r -> KV.of(
                                r.getValue("Season").toString() + "-" + r.getValue("Episode").toString(),   //You can see how messy Row records can get here!
                                r.getValue("EpisodeTitle").toString())));

        PCollection<KV<String, TransformersRecord>> transformersKV = transformersIn
                .apply("Create transformers kv", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(TransformersRecord.class)))       //You can see how messy the TypeDescriptors can get here!
                        .via(r -> KV.of(r.getFirstApperanceSeason() + "-" + r.getFirstApperanceEpisode(), r)));

        //Now we can join on these Keys. I've used strings here but you can also use ints, or even Java objects with
        // .equals() to get what you want. Note that there are two kinds of join in Beam, this uses
        // `org.apache.beam.sdk.extensions.joinlibrary.Join`. The other one is  for schema joins.
        PCollection<KV<String, KV<TransformersRecord, String>>> joinedData = Join.innerJoin(transformersKV, episodesKV);
        //Our PCollection type has got a little messy as we have the join key and the left and right hand side values.
        // You can process these out using a lambda quite easily, for example `x -> KV::getValues` if you want to.

        joinedData.apply("Preview parquet data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x);return ""; }));
        p.run().waitUntilFinish();

    }

}
