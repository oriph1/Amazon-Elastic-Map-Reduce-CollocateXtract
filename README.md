# CollocateXtract

## Introduction
Welcome to CollocateXtract, an automated collocation extraction tool powered by Amazon Elastic Map Reduce (EMR). Collocations are key
sequences of words or terms that frequently occur together, providing valuable insights into language patterns and semantic relationships.
In this project, we harness the power of EMR to automatically extract collocations from the Google 2-grams dataset, focusing on both English
and Hebrew corpora. Utilizing Normalized Pointwise Mutual Information (NPMI),
our goal is to identify significant word pairs that exhibit a higher co-occurrence rate than expected by chance.
By calculating the NPMI for each pair of words in each decade, we can determine which pairs constitute collocations based on
user-defined thresholds for minimal and relative minimal PMI values.
We used a Job Flow that is a collection of processing steps that Amazon Elastic MapReduce runs on a specified dataset using a set of
Amazon EC2 instances. A Job Flow consists of one or more steps, each of which must complete in sequence successfully, 
for the Job Flow to finish.
Additionally, no assumption on the memory capacity can be made. In particular, you cannot assume that the word pairs of any decade,
nor the set of pairs of a given word, nor a list of counts of unigrams etc, can be stored in memory.


## System Architecture
The system is composed of 6 elements: App class and 5 Steps Each step is scheduled using Amazon Elastic Map-Reduce (EMR).

## App
The main function serves the purpose of initializing the EMR program, encompassing the definition of all configurations
necessary for executing a multi-step job. This includes specifying the paths to the required JAR files, determining
the number of instances to execute the program, setting up the logging pathway, and other essential configuration

## Step 1
The purpose of the first step is to group all the occurrences of the 2-grams by decade, and calculate the total number of 2-grams n

### Mapper:
Input:
1) Key = lineId (LongWritable) Value = 2-gram \t year \t occurrences \t books

Output:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w1 w2 > Value = < occurrences >

### Reducer:
Input:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w1 w2 > Value = < occurrences >

Output:
1) decade \t occurrences      (N for this decade)
2) decade w1 w2 \t occurrences

### Combiner (optional):
Input:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w1 w2 > Value = < occurrences >

Output:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w1 w2 > Value = < occurrences >


## Step 2
The purpose of step 2 is to calculate c(w1), which represents the number of occurrences of w1 as the first word in a 2-gram
within a specific decade.

### Mapper:
Input:
1) Key = lineId (LongWritable) Value = decade \t occurrences
2) Key = lineId (LongWritable) Value = decade w1 w2 \t occurrences

Output:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w1 * > Value = < occurrences >
3) Key = < decade w1 ** > Value = < w2 occurrences >


### Reducer:
Input:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w1 * > Value = < occurrences >
3) Key = < decade w1 ** > Value = < w2 occurrences >

Output:
1)decade \t occurrences
2)decade w1 w2 \t occurrences c(w1)


### Combiner (optional):
Input:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w1 * > Value = < occurrences >
3) Key = < decade w1 ** > Value = < w2 occurrences >

Output:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w1 * > Value = < occurrences >
3) Key = < decade w1 ** > Value = < w2 occurrences >



## Step 3
The purpose of step 3 is to calculate c(w2), which represents the number of occurrences of w2 as the second word in a 2-gram
within a specific decade.

### Mapper:
Input:
1) Key = lineId (LongWritable) Value = decade \t occurrences
2) Key = lineId (LongWritable) Value = decade w1 w2 \t occurrences c(w1)

Output:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w2 * > Value = < occurrences >
3) Key = < decade w2 ** > Value = < w1 occurrences c(w1) >


### Reducer:
Input:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w2 * > Value = < occurrences >
3) Key = < decade w2 ** > Value = < w1 occurrences c(w1) >

Output:
1)decade \t occurrences
2)decade w1 w2 \t occurrences c(w1) c(w2)


### Combiner (optional):
Input:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w2 * > Value = < occurrences >
3) Key = < decade w2 ** > Value = < w1 occurrences c(w1) >

Output:
1) Key = < decade > Value = < occurrences >
2) Key = < decade w2 * > Value = < occurrences >
3) Key = < decade w2 ** > Value = < w1 occurrences c(w1) >


## Step 4
The purpose of step 4 is to calculate the PMI and NPMI for every 2-gram across all decades.

### Mapper:
Input:
1) Key = lineId (LongWritable) Value = decade \t occurrences
2) Key = lineId (LongWritable) Value = decade w1 w2 \t occurrences c(w1) c(w2)

Output:
1) Key = < decade * > Value = < occurrences >
2) Key = < decade ** > Value = < w1 w2 occurrences c(w1) c(w2) >


### Reducer:
Input:
1) Key = < decade * > Value = < occurrences >
2) Key = < decade ** > Value = < w1 w2 occurrences c(w1) c(w2) >

Output:
1) decade w1 w2 /t npmi (The npmi of the potential collocation "w1 w2")
2) decade /t sumNpmi        (Sum of all npmi's of the decade)


## Step 5
The primary objective of Step 5 is to employ the compare function to sort the 2-grams and their corresponding probabilities as required,
thereby selecting the collocations for each decade.

### Mapper:
Input:
1) Key = lineId (LongWritable) Value = decade w1 w2 /t npmi
2) Key = lineId (LongWritable) Value = decade /t sumNpmi

Output:
1) Key = < decade MAX_VAL * >  Value = < sumNpmi >
2) Key = < decade npmi > Value = < w1 w2 >


### Reducer:
Input:
1) Key = < decade Integer.MAX_VALUE >  Value =  < sumNpmi >
2) Key = < decade npmi > Value = < w1 w2 >

Output:
1) decade npmi /t w1 w2 (Only collocations)

