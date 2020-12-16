# ApacheBeamExamples
Some learnings from how beam works, hoping to blogify it later. I would advise you 
to read this whole document before you embark on your own journey but for those
that wish to dip in and out I've got the contents of the various sections just 
below here. If you scroll further you will find notes on some setup steps, 
gremlins and notes that will be worth reading. 

The [programming guide for Beam](https://beam.apache.org/documentation/programming-guide/) 
is great but often I find that it doesn't match the functionality 
available in the code and the "simple" solutions often require some 
diamond operators or pre-calculation. As such, I've needed to work 
a few things out on my own.  

Note that, all scripts in this repo should be runnable, just pull it, open in 
IntelliJ and import as a Maven project. The data are pushed too and they are, 
of course, a [record of Generation 1 Transformers](https://en.wikipedia.org/wiki/Transformers:_Generation_1). 

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

## Section Summaries
Although you can dip into these in whatever order you feel like, concepts are 
introduced in order so, if you start in section 4 and don't know what a lambda is, 
it has been explained, just checkout the the previous readme!

### Section 1 - Reading files
What it says on the tin! This is very basic reads from CSVs and Parquet, if you 
want to use anything more exciting I point you in the direction of a more  
complex github repo [here](https://github.com/rishisinghal/BeamPipelineSamples).

I also introduce basic components such as Collections, types, Pipelines, 
apply methods and how to generate inline test data. 

### Section 2 - Map operations
This outlines simple ways to transform records into other records through [Java 
lambdas](https://www.w3schools.com/java/java_lambda.asp#:~:text=Lambda%20Expressions%20were%20added%20in,the%20body%20of%20a%20method.) 
and Beam Simple functions. 

### Section 3 - Beam schemas
Applying a Beam schema (which is not the same as your Parquet or row schemas) to your 
data can make it easier to work with and lets you wrap up methods in schema objects. 
This section gives an example schema and outlines how to use them to make some basic
transforms less verbose. 

### Section 4 - Advanced map functions
Lambdas are all well and good but what if you want to do some heavier lifting? 
This section introduces the main workhorse of Beam - the Parallel Do function 
(ParDo).

### Section 5 - Reduce operations
A quick introduction to group-by operations and how different ones can be 
used for different use-cases. 

### Section 6 - Beam SQL 
Not something I've used a lot but, much like Spark, there is a SQL interface
for Beam. IMHO, I think having a SQL string in the middle of your code can 
feel a bit icky but it can replace some pretty heavy boilerplate when used 
right. 

## Main args
Beam has some strong (and correct) opinions on how to pass arguments to 
your ELT process. This is recommended so you can, for example, pass file
names at runtime. I will not be adding this at this time. [There's plenty of 
info out there on how they work](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WordCount.java)
and what I want to get across is more how Beam works and the core functions 
without getting bogged down in boiler plate. I may add this in a later version.
 

## Setup

### POM
This project is setup as Maven, I may add some Gradle notes in the 
future but I just learned Maven first so I prefer it. This should
detail all the imports you need. Should you want to add new runners
or the like you will need to add them. There's plenty out there, 
it's worth just checking what Beam have on Maven. 

I would like to draw attention to two parts of the POM:
```xml
    <properties>
<!--        These two are to allow @schemaCreate to work-->
        <!-- PLUGIN VERSIONS -->
        <maven-compiler-plugin.version>3.1</maven-compiler-plugin.version>

        <!-- OTHER PROPERTIES -->
        <java.version>1.8</java.version>
    </properties>
```
and
```xml
<!--This is to allow @SchemaCreate to work. As are the properties labelled above-->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>${maven-compiler-plugin.version}</version>
                <configuration>
                    <!-- Original answer -->
                    <compilerArgument>-parameters</compilerArgument>
                    <!-- Or, if you use the plugin version >= 3.6.2 -->
<!--                    <parameters>true</parameters>-->
<!--                    <testCompilerArgument>-parameters</testCompilerArgument>-->
<!--                    <source>${java.version}</source>-->
<!--                    <target>${java.version}</target>-->
                </configuration>
            </plugin>
```
The above code lets schemas work properly in beam, if you are using 
different versions of the plugin you'll need to comment/uncomment 
appropriately.

### Parquet & Hadoop
I read Parquet as part of this pipeline. This is largely because we 
use it for data storage. This requires a couple of setup steps to get
the Hadoop jars working. It also provides a gremlin that you can 
checkout below. 

You will need to download the Hadoop *jars* that are relevant to the
current build. I say jars as you can get Hadoop without them. For me,
I grabbed 2.7.1 from [here](https://archive.apache.org/dist/hadoop/core/hadoop-2.7.1/)

You then need to get these added to IntelliJ which you can do using 
[this](https://tokluo.wordpress.com/2016/01/31/using-intellij-to-write-your-application/#:~:text=To%20add%20the%20Hadoop%20module,Select%20JARS%20or%20directories.)
guide. 

Also, I know that you should be able to get a schema from a Parquet file!
[The Beam folks do too](https://github.com/apache/beam/pull/9721), it's 
just not been implemented yet. 


## Gremlins
* Note that you need Hadoop Jars to read Parquet properly. See the
setup section (above) for details. 
* Parquet saves its timestamps to an int96 which (AFAIK) java doesn't 
read. As such you can't read a Parquet timestamp. There's probably a 
workaround but I haven't had the time or inclination to fix it.  
* Beam Records are immutable! You should not try to alter the inputs 
of a mapping operation, instead return new ones. 

# Where Next?
* [Java functions](https://beam.apache.org/documentation/transforms/java/overview/)  
* [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)
* [Beam Quickstart](https://beam.apache.org/get-started/quickstart-java/)