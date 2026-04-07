# MTUOC-corpusFromWikipedia

Tool to create parallel corpora from Wikipedia.

<!-- 1. Get started  
Clone the tool using `git clone https://github.com/gon-zalo/MTUOC-corpusFromWikipedia.git`. Afterwards, open the directory of the repo in your terminal and run the tool with the following command: `python -m corpusFromWikipedia.cli -h`, which will show the help message. 

The tool contains six subcommands: create, segment, align, rescore, select, and pipeline. The pipeline streamlines the whole corpora creation process, running all the other commands in one go. 

To create a corpus you will need the dump file from Wikipedia and a categories database. The dumps can be downloaded from [here](https://dumps.wikimedia.org/backup-index-bydb.html). The name of the dumps is the two letter ISO language code followed by 'wiki', e.g., 'cawiki' for the Catalan Wikipedia. Once found, you need to download the 'pages-articles.xml.bz2' file and place it in the 'dumps' folder of the repo.

## Pipeline command

This command streamlines the whole process, so it is recommended to be used most of the times. It will run with one simple line such as `python -m corpusFromWikipedia.cli pipeline ca en "Biochemistry, Neuroscience" 3`. 

A directory will be created in the outputs folder, inside the tool folder, with a name and the ISO codes of the desired language or languages, e.g.: 'corpora-en-es' or 'corpus-es'. Once the whole process has been completed, you will find some folders: 'pages' contain text of each of the pages, 'segments' contain the segmented text. There are also several text files that contain the list of the articles, the unique segments in each language, the aligned segments, the rescored segments, and the selected segments, i.e., the final file.


`create` Initiate the pipeline by extracting pages from the dumps based on the specified categories.  

`segment` Activate the creation of a segmented version of the extracted text.  

`align` Perform the bitext mining (alignment) process between the L1 and L2 segmented corpora.  

`rescore` Trigger the rescoring phase, where alignments are evaluated using more computationally expensive models (Language Detection and SBERT).  

`select` Filter the rescored segments based on the defined quality thresholds.  

`pipeline` Execute all the previous commands in one go. -->