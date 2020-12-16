# Advanced Map Functions
This details the power of the ParDo (Parallel Do) function. Some key terms:

## Branching Transforms
Thus far we've been running our .apply() statements linearly. That works 
fine but you can also branch them. In pseudocode:
```
inputData = p.readData()

output1 = inputData.apply(FirstThing).apply(SecondThing)
output2 = inputData.apply(ThirdThing).apply(FourthThing)

finalOutput = Join(output1, output2)
```

## Bundles
A bundle is a collection of Beam Records that are operated on in sequence on
a single worker. When you ask Beam to do something it'll break the data into
bundles that can be operated on in parallel. 

## TODOs
Sorry, I've not finished these. Feel free to work it out and add them. I've
read the docs but not put together a toy example. 
