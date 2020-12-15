//package org.datamacgyver.Section4_advancedMapFunctions;
//
//public class TODOScript4_MultipleOutputs {
//    // To emit elements to multiple output PCollections, create a TupleTag object to identify each collection
//// that your ParDo produces. For example, if your ParDo produces three output PCollections (the main output
//// and two additional outputs), you must create three TupleTags. The following example code shows how to
//// create TupleTags for a ParDo with three output PCollections.
//
//    // Input PCollection to our ParDo.
//    PCollection<String> words = ...;
//
//    // The ParDo will filter words whose length is below a cutoff and add them to
//    // the main output PCollection<String>.
//    // If a word is above the cutoff, the ParDo will add the word length to an
//    // output PCollection<Integer>.
//    // If a word starts with the string "MARKER", the ParDo will add that word to an
//    // output PCollection<String>.
//    final int wordLengthCutOff = 10;
//
//    // Create three TupleTags, one for each output PCollection.
//    // Output that contains words below the length cutoff.
//    final TupleTag<String> wordsBelowCutOffTag =
//            new TupleTag<String>(){};
//    // Output that contains word lengths.
//    final TupleTag<Integer> wordLengthsAboveCutOffTag =
//            new TupleTag<Integer>(){};
//    // Output that contains "MARKER" words.
//    final TupleTag<String> markedWordsTag =
//            new TupleTag<String>(){};
//
//// Passing Output Tags to ParDo:
//// After you specify the TupleTags for each of your ParDo outputs, pass the tags to your ParDo by invoking
//// .withOutputTags. You pass the tag for the main output first, and then the tags for any additional outputs
//// in a TupleTagList. Building on our previous example, we pass the three TupleTags for our three output
//// PCollections to our ParDo. Note that all of the outputs (including the main output PCollection) are
//// bundled into the returned PCollectionTuple.
//
//    PCollectionTuple results =
//            words.apply(ParDo
//                    .of(new DoFn<String, String>() {
//                        // DoFn continues here.
//            ...
//                    })
//                    // Specify the tag for the main output.
//                    .withOutputTags(wordsBelowCutOffTag,
//                            // Specify the tags for the two additional outputs as a TupleTagList.
//                            TupleTagList.of(wordLengthsAboveCutOffTag)
//                                    .and(markedWordsTag)));
//
//}
