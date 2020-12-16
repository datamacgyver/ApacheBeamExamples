# Beam Schemas
Note that this is about schemas defined using Java objects. We've
already seen how Parquet GenericSchemas work in section 2. Row schemas
are very similar to GenericSchemas in how they are used but Row schemas
play much nicer with Beam's API.  

The first script simply makes a point about record immutability which is an
important feature of many Beam Transforms! The second outlines how to make 
a schema (there's a seperate class file for that). Finally, the third script
outlines some of the benefits of using schemas, mainly that you get access to
a shorthand for some frequently used transforms. 

Note that to use schemas you need to change some POM settings, details in the 
main readme. 

## Annotations
Much like decorators in Python, an annotation adds functionality to your code 
without the need for excessive boilerplate. If you need a quick primer on 
annotations, see [here](https://beginnersbook.com/2014/09/java-annotations/).

### Lombok
This section makes extensive use of [Lombok](https://projectlombok.org/features/all)
annotations to provide boiler plate on our schema objects. If you've not used it before, 
it's awesome. There's also an IntelliJ plugin that can make your life easier 
by making sure it understands what the annotations mean, as well as providing a 
"delombok" refactor so you can see what it's doing. 

For simplicity I've also provided a de-lombok-ed version of the schema under 
DelombokTransformersRecord. 

## Coders
These are mentioned in passing but they are the functions that turn your objects into
byte strings and back again for processing. You probably don't need to worry about these
at this stage, other than the annotation in the Schema which saves some less than intuitive 
errors. 

## Chaining .apply()
I've also started chaining my .apply methods here, this is perfectly valid and makes the code 
much nicer to navigate IMHO. 
```
p.apply(someThing)
 .apply(someThingElse)
 .apply(aFinalThing);

```
This is much better than some alterniatives. You should see the abomination that I found in 
their wordcount example!
`((PCollection)((PCollection)((PCollection)p.apply("ReadLines", TextIO.read().from(options.getInputFile()))).apply(new WordCount.CountWords())).apply(MapElements.via(new WordCount.FormatAsTextFn()))).apply("WriteCounts", TextIO.write().to(options.getOutput()));`
