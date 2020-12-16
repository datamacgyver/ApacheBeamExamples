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


    //TOD: I feel all these mains need to be at the top...
    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<TransformersRecord> transformersIn = p
                .apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet))
                .apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));

        PCollection<Row> episodes = p
                .apply("Get the inline episodes", Create.of(InLineData.episodesList).withCoder(RowCoder.of(InLineData.episodeRowSchema)));

        //First we need to create key value pairs for each of our records. You can do this in several ways, for example using schema notation
        //or a lambda/simple function. I'm going for the second option as it lets me maintain my original schemas and not convert things to
        //rows.
        PCollection<KV<String, String>> episodesKV = episodes
                .apply("Create episodes kv", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via(r -> KV.of(
                                r.getValue("Season").toString() + "-" + r.getValue("Episode").toString(),
                                r.getValue("EpisodeTitle").toString())));

        PCollection<KV<String, TransformersRecord>> transformersKV = transformersIn
                .apply("Create transformers kv", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(TransformersRecord.class)))
                        .via(r -> KV.of(r.getFirstApperanceSeason() + "-" + r.getFirstApperanceEpisode(), r)));

        //Note that there are two kinds of join in Beam, this uses `org.apache.beam.sdk.extensions.joinlibrary.Join`. The other one is 
        //for schema joins. 
        PCollection<KV<String, KV<TransformersRecord, String>>> joinedData = Join.innerJoin(transformersKV, episodesKV);
        joinedData.apply("Preview parquet data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x);return ""; }));
        p.run().waitUntilFinish();

    }

//    class makeEpisodeJoinKey extends SimpleFunction<String, List<String>> {
//        @Override
//        public List<String> apply(String word) {
//            return Arrays.asList(word, "Its weekend!");
//        }
//    }
}
