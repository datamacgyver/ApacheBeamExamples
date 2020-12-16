# Simple Map Operations
Most of what you need to know here is in the functions themselves. 
Couple of new concepts:

## Java Lambdas
See [here](https://www.w3schools.com/java/java_lambda.asp#:~:text=Lambda%20Expressions%20were%20added%20in,the%20body%20of%20a%20method.) 

## Key/Value pairs
Beam uses Key/Value Pairs quite a bit, they aren't a lot different 
to those in Java but it's worth a note that we create them using:
`KV.of(name, combiner)`, types are inferred but if we want to 
define a PCollection using them we do have to provide a type:
`PCollection<KV<String, GenericRecord>>` for example. 

## @Override
This annotation you have probably seen before, it means "override 
from the superclass" but if you don't get class inherience see [here](https://www.tutorialspoint.com/java/java_overriding.htm), 
if you need a quick primer on annotations, see [here](https://beginnersbook.com/2014/09/java-annotations/).

We will be seeing a *lot* of annotations in the next section!