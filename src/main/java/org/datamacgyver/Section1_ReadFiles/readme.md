# Reading files
*Note that each script in this section has it's own comments and 
notes on it, the below are just general points*

Scripts in this section are all pretty short and sweet. They 
can be consumed in any order you wish. There are a few general 
points to understand first that are going to serve you well 
throughout this repo:

## Schemas
The word schema is over-used in Beam. Parquet GeneralRecords 
have a schema but this is different to a Beam Row schema. That
is also different to a Beam schema specified using a java object. 

It's confusing and we will discuss it more in section 3. For now 
just know that all schemas are not created equal and some approaches 
are going to be more flexible while others are going to be more robust. 

## Printing outputs
This code:

```
exampleData.apply("Preview created data", MapElements.into(TypeDescriptors.strings()).via(x -> { System.out.println(x); return ""; }));
```

Exists in every single script in this repo. You will learn what it 
means in Section 2 but for now just be happy that it means "Print each 
record". 

## PCollections
These are basically collections of Beam records, definition of a PCollection
also requires providing the type using diamond notation (see below)

## Diamond notation `<XXX>`
As we all know, Java is a strongly typed language, everything needs a type 
and Beam is no exception. Of course your data could have any type so the Beam
API makes strong use of the [diamond operator](https://www.baeldung.com/java-diamond-operator)
which allows you to pass the type as an argument. The simplest example of this is
using lists, `List<String>` means "list of type String", therefore a `PCollection<Row>`
is a PCollection of type Row.

## Pipelines
All the scripts here start with 

`Pipeline p = Pipeline.create();`
and end with 
`p.run().waitUntilFinish();`

All operations are done on this pipeline object. In pseudocode: `p.readData().apply(aTransform).writeData()`
but you can have intermediate objects in this, it's clear once you look at a script. 
