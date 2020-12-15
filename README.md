# ApacheBeamExamples
Some learnings from how beam works, hoping to blogify it later. 

TODO: Add some links in all below.
TODO: Need to add a nod to the specific guides (https://beam.apache.org/documentation/transforms/java/overview/) as well as teh programming guide
TODO: A nod to composite functions. 
TODO: Add more fields to the data, perhaps also split up so we can do inheretence. 
 
## Beam documentation 
The programming guide for Beam is great but often I find that it doesn't 
match the functionality available in the code and the "simple" solutions
often require some diamond operators or pre-calculation. As such, I've
needed to work a few things out on my own.  

## Words of warning
First and foremost. I am a Data Scientist that enjoys tech, I am not
a java dev, nor am I formally trained in computer science. I do 
however love solving problems and we are currently trialing Beam 
as a solution to our EL(T) problems. As such, this is both a care 
and share on how Beam works and a chance for me to clarify my 
learnings. 

I can already hear the question: *"You are a Data Scientist, why 
is this not in Python?"* well, from what I can tell, once deployed
I would add my Python scripts to the Java ones that the devs have
put out. Also, our devs prefer Java and I'd like to learn some 
more Object Orientated stuff. 

Finally, I built this using IntelliJ so some setup steps may be 
peculiar to that. 

## Main args
Beam has some strong (and correct) opinions on how to pass arguments to 
your ELT process. This is recommended so you can, for example, pass file
names at runtime. I will not be adding this at this time. There's plenty of 
info out there on how they work (REF) and what I want to get accross is more 
how Beam works and the core functions without getting bogged down in boiler 
plate. I may add this in a later release. 

## Setup

### POM
This project is setup as Maven, I may add some Gradle notes in the 
future but I just learned Maven first so I prefer it. This should
detail all the imports you need. Should you want to add new runners
or the like you will need to add them. There's plenty out there, 
it's worth just checking what Beam have on Maven. 

### Parquet & Hadoop
I read Parquet as part of this pipeline. This is largely because we 
use it for data storage. This requires a couple of setup steps to get
the Hadoop jars working. It also provides a gremlin that you can 
checkout below. 

You will need to download the Hadoop *jars* that are relevent to the
current build. I say jars as you can get Hadoop without them. For me,
I grabbed 2.7.1 from [here](https://archive.apache.org/dist/hadoop/core/hadoop-2.7.1/)

You then need to get these added to IntelliJ which you can do using 
[this](https://tokluo.wordpress.com/2016/01/31/using-intellij-to-write-your-application/#:~:text=To%20add%20the%20Hadoop%20module,Select%20JARS%20or%20directories.)
guide. 


## Gremlins
* Note that you need Hadoop Jars to read Parquet properly. See the
relevent setup section for details. 
* Parquet saves its timestamps to an int96 which (AFAIK) java doesn't 
read. As such you can't read a Parquet timestamp. There's probably a 
workaround but I haven't had the time or inclination to fix it.  

## Where to go from here? 
https://cloud.google.com/blog/products/gcp/writing-dataflow-pipelines-with-scalability-in-mind
also some beam links. 

## References
https://tokluo.wordpress.com/2016/01/31/using-intellij-to-write-your-application/#:~:text=To%20add%20the%20Hadoop%20module,Select%20JARS%20or%20directories.
https://archive.apache.org/dist/hadoop/core/hadoop-2.7.1/