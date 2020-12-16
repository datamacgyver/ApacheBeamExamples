//package org.datamacgyver.Section3_Schemas;
//
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import lombok.RequiredArgsConstructor;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.coders.RowCoder;
//import org.apache.beam.sdk.io.parquet.ParquetIO;
//import org.apache.beam.sdk.schemas.JavaBeanSchema;
//import org.apache.beam.sdk.schemas.JavaFieldSchema;
//import org.apache.beam.sdk.schemas.Schema;
//import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
//import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
//import org.apache.beam.sdk.schemas.transforms.AddFields;
//import org.apache.beam.sdk.schemas.transforms.Filter;
//import org.apache.beam.sdk.schemas.transforms.Join;
//import org.apache.beam.sdk.schemas.transforms.Select;
//import org.apache.beam.sdk.transforms.Create;
//import org.apache.beam.sdk.transforms.MapElements;
//import org.apache.beam.sdk.transforms.SimpleFunction;
//import org.apache.beam.sdk.values.PCollection;
//import org.apache.beam.sdk.values.Row;
//import org.apache.beam.sdk.values.TypeDescriptor;
//import org.apache.beam.sdk.values.TypeDescriptors;
//import org.datamacgyver.Section1_ReadFiles.InLineData;
//import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
//
//import static org.datamacgyver.Section1_ReadFiles.InLineData.episodeRowSchema;
//import static org.datamacgyver.Section1_ReadFiles.InLineData.episodesList;
//
//public class Script4_SchemaJoins {
//    public static void main(String[] args) {
//        String inFileParquet = "data/transformers.parquet";
//        Pipeline p = Pipeline.create();
//
//        //Note that I just import the Transformers record from the last section, you've already read that though, right?
//        PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet));
//        PCollection<TransformersRecord> transformersIn = readParquet.apply("Convert Schema", MapElements.via(new TransformersRecord.MakeTransformerRecordFromGeneric()));
//        PCollection<EpisodesRecord> episodesIn = p
//                .apply("Create inline data", Create.of(episodesList).withCoder(RowCoder.of(episodeRowSchema)))
//                .apply("Map to episodes schema", MapElements.via(new EpisodesRecord.MakeEpisodesRecord()));
//
//        PCollection<Row> joinedData = episodesIn
//                .apply(Join.innerJoin(transformersIn)
//                        .on(Join.FieldsEqual
//                                .left("firstApperanceSeason", "firstApperanceEpisode")
//                                .right("Season", "Episode")
//                        ));
//
//        joinedData.apply("Preview schema data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
//
//
//        ;
//
//
//
//        p.run().waitUntilFinish();
//    }
//}
//
////I've made another basic schema here, it's for the episodes inline dataset
//@DefaultSchema(JavaBeanSchema.class)
//@Data
//@RequiredArgsConstructor(onConstructor=@__({@SchemaCreate}))
//@NoArgsConstructor(force = true)
//class EpisodesRecord {
//    public final int season;
//    public final int episode;
//    public final String episodeTitle;
//
//    public static class MakeEpisodesRecord extends SimpleFunction<Row, EpisodesRecord> {
//        @Override public EpisodesRecord apply(Row r) {
//            return new EpisodesRecord(
//                    r.getValue("Season"),
//                    r.getValue("Episode"),
//                    r.getValue("EpisodeTitle")
//            );
//        }
//    }
//
//}
