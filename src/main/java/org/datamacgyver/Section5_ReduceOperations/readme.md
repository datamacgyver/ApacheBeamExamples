# Reduce Operations
We have mainly just dealt with maps and transforms thus far, what about
if we want to group, dedup or aggregate? Enter Reduce operations!

A few key terms:

## PCollection<Row>
We've come accross the idea of a Row schema a couple of times but never really 
used them. Now is the time! Row is what Beam calls a record set that has a schema 
but it doesn't have an official name. Row contains all the variable names and values 
you'd expect, it also (unlike a GenericRecord) counts as an actual Beam Schema. 

The problem is you lose the getters and setters you get with a "proper" schema so have 
to fall back on accessing values using their names `.getValue("name")`. Not having 
access to all the loveliness that a schema provides you means you may struggle with 
later operations (or simply have to learn how to do things differently).

In my head, this form of notation is great for simple transforms that you can perform 
on the data either before you make a proper schema or before you do a final write out. 