--------------------------------------------TASK 1 - Instructions----------------------------------------------------------------

TASK1 - Word Count on Tweets (Hashtags) & WordCloud - FOLDER:(Task1 - Word Count on Tweets)

Details about files in the folders:
1. test.txt - Contains output from tweets (which contain only hashtags).
	      test.txt is the input to the MR WordCount Program.

2. outputMapTweet.txt - It is the output of the MR WordCount Program.
			It contains word with its frequency.
3. TweetsData.csv - This is the input to the wordcloud, it contains outputMapTweet.txt data but in readable format for the wordcloud.
4. Lab4 - Wordcount on Tweets (Python Notebook) contains the wordcloud and preprocessing.

--------------------------------------------TASK 2 - Instructions----------------------------------------------------------------

1. INPUT - "Input Tweets" folder - "tweets.txt" - This is the input given to both Pairs and Stripes (Word Cooccurrence) Programs

2. PAIRS - a. JAVA Files: WordCooccurrencePairs.java & WordPair.java
           b. JAR Files: wopair.jar
	       c. Class Files: WordCooccurrencePairs.class & WordPair.class
	       d. Output Folder: task2PairsOutput

2. STRIPES - a. JAVA Files: WordCooccurrenceStripes.java & myMapWritable.java (To be Compiled Together)
             b. JAR File: wostripes.jar
	         c. Class Files: WordCooccurrenceStripes.class & myMapWritable.class
	     	 d. Output Folder: task2StripesOutput

3. Execution Steps:

A. Pairs: hadoop jar wopair.jar WordCooccurrencePairs /DesiredInputdirectory /DesiredOutputDirectory

B. Stripes: hadoop jar wostripes.jar WordCooccurrenceStripes /DesiredInputdirectory /DesiredOutputDirectory

--------------------------------------------TASK 3 - Featured Activity 1 - Instructions------------------------------------------

1. INPUT - Latin Text Files - lucan.bellum_civile.part.1.tess & vergil.aeneid.tess

2. JAR File - act1.jar 
3. Class Files - activity1.class
4. JAVA File - activity1.java
5. Files Needed - new_lemmatizer.csv

Note: Provide new_lemmatizer.csv in the same file where the java,jar and class files are kept (same folder)

6. Execution Steps:

Activity1 - Word count:
hadoop jar act1.jar activity1 /DesiredInputdirectory /DesiredOutputDirectory

7. Output Folder: Activity1 output - 
   Output Format: Word <Loc1> <Loc2> <...> Count 

--------------------------------------------TASK 4 - Featured Activity 2A-2Gram Instructions-------------------------------------

1. INPUT - Latin Text Files - Number of Input Files 1,2,3,4,5,10,15,20,50 respectively

2. JAR File - act2.jar
3. Class Files - activity2.class & WordPair.class
4. JAVA File - activity2.java & WordPair.java
5. Files Needed - new_lemmatizer.csv

Note: Provide new_lemmatizer.csv in the same file where the java,jar and class files are kept (same folder)

6. Execution Steps:

Activity2A - Word cooccurrence 2gram:
hadoop jar act2.jar activity2 /DesiredInputdirectory /DesiredOutputDirectory

7. Output Folder: Activity2A 2gram - output file -
   Output Format: Word1,Word2 <Loc1> <Loc2> <...> Count
   				  Lemma1,Lemma2 <Loc1> <Loc2> <...> Count
   				  Lemma1,Word2 <Loc1> <Loc2> <...> Count
   				  Word1,Lemma2 <Loc1> <Loc2> <...> Count

Note: Only one output file is provided due to limited memory available (larger output files upto 500mb were generated but not added)

--------------------------------------------TASK 4 - Featured Activity 2B-3Gram Instructions-------------------------------------

1. INPUT - Latin Text Files - Number of Input Files 1,2 respectively

Note: Works on larger inputs too takes a lot of time

2. JAR File - act3.jar
3. Class Files - activity3.class & WordPair.class
4. JAVA File - activity3.java & WordPair.java
5. Files Needed - new_lemmatizer.csv

Note: Provide new_lemmatizer.csv in the same file where the java,jar and class files are kept (same folder)

6. Execution Steps:

Activity2B - Word cooccurrence 3gram:
hadoop jar act3.jar activity3 /DesiredInputdirectory /DesiredOutputDirectory

OUTPUT CASE 1: (Working on Latin Data from 482Latin Files provided)
   
Output Folder: Activity2B 3gram - output200lines -
Output Format: Word1,Word2,Word3 <Loc1> <Loc2> <...> Count
               Lemma1,Lemma2,Lemma3 <Loc1> <Loc2> <...> Count
      		   Lemma1,Word2,Lemma3 <Loc1> <Loc2> <...> Count
			   Word1,Lemma2,Word3 <Loc1> <Loc2> <...> Count
			   and so on. (total 8 possibilities)

Note: Only one output file of 200lines is provided as original output file size was 530mb(5lakh+ lines) and due to limited memory available only upto 2 input files were considered (output for 2 files upto 7gbs were generated but not added)

OUTPUT CASE 2:(Working on Latin Data from vergil.aeneid.tess file) 
The data was divided into smaller files so that execution can take place and the corresponding input and output is provided in the form of "tess" files
Input: tess1,tess2,tess3,tess5 - 1,2,3&5 input files were provided
Output: tess1,tess2,tess3,tess5 - 1,2,3&5 output files were generated.

The input size is very small 30-200 kbs so output generated was 2-30mb and execution took place really fast. (This happened due to number of words per location were less and so less execution was required)

---------------------------------------------------------------------------------------------------------------------------------

GRAPHICAL ANALYSIS:

Graphical Analysis is shown in the pdf file "Analysis"