#!/usr/bin/python3

from .commands.create import create_corpora
from .commands.segment import segment_corpus
from .commands.align import align_corpora
from .commands.rescore import rescore_corpus
from .commands.select import select_corpus
from .commands.pipeline import pipeline

import argparse
from pathlib import Path

# cli function
def cli():          
    parser = argparse.ArgumentParser(
        description=
        '''
        Tool that allows the creation, segmentation, alignment, rescoring and segment selection of parallel corpora from Wikipedia. It supports step-by-step execution and full pipeline execution. That is, commands may be run individually, e.g., in order to inspect results between steps, or execute the entire workflow at once using the 'pipeline' command.
        '''
        )
    subparsers = parser.add_subparsers(required=True)
    
# CREATE SUBPARSER
    create_parser = subparsers.add_parser("create", help="Create parallel corpora from Wikipedia.", description="Create parallel corpora from Wikipedia dumps")
    create_parser.add_argument('lang1', help='Name or two letter ISO code of the source language.')
    create_parser.add_argument('lang2', help='Name or two letter ISO code of the target language.')
    create_parser.add_argument("categories", action="store", help='Wikipedia categories to search. Must be in between quotation marks (""). If there is more than one, they must be separated by a comma (,).')
    create_parser.add_argument('depth', type=int, help='The category level depth.')
    create_parser.add_argument('--restrict', action='store_true', help='Restrict L2 pages to equivalents to L1 pages.', required=False)
    create_parser.add_argument('--database', action="store", dest="database", help='The CCW sqlite database to use. Default: database/CCWikipedia-20251201.sqlite',required=False)
    create_parser.add_argument('--dumps', help='Wikipedia dumps path. Default: dumps/', required=False)   
    create_parser.add_argument('--outdir', help='Name of the output directory. Default: corpora-lang1-lang2/. Language codes will be added automatically.',required=False)
    create_parser.set_defaults(func=create_corpora)

# SEGMENT SUBPARSER
    segment_parser = subparsers.add_parser("segment", help="Segment the extracted texts.", description="Segment the corpora. Can be used on its own to segment all the files in one directory.")
    segment_parser.add_argument("indir", help="Folder where the corpus to segment is stored, i.e., the 'pages-lang' folder. The folder must have the two letter ISO code at the end.")
    segment_parser.add_argument("--srxfile", type=str, help="The SRX file to use. Default: segment.srx", required=False)
    # segment_parser.add_argument("-l", "--srxlang", type=str, help="The language as stated in the SRX file, i.e. the name of the language.", required=True)
    segment_parser.add_argument("--paramark", action="store_true", help="Add the <p> paragraph mark (useful for Hunalign).", required=False)
    segment_parser.add_argument("--outdir", type=str, help="The output directory in which to save the segmented files. If it doesn't exist, it will be created", required=False)
    segment_parser.set_defaults(func=segment_corpus)

# ALIGN SUBPARSER
    align_parser = subparsers.add_parser("align", help="Perform bitext mining (alignment) between both corpora.", description="Mine parallel (translated) sentences from two lists of monolingual sentences.")
    align_parser.add_argument("-i", "--input", nargs=2, metavar=("FILE1", "FILE2"), dest="input_files", help="Path to the two file paths that will be aligned", required=True)
    align_parser.add_argument("-o", "--output", help="The output file path", required=True)
    align_parser.add_argument("-dev", "--device", default="cpu", dest="device", help="Device used (GPU or CPU). Default is CPU.", required=False)
    # align_parser.add_argument("--file-by-file", help="Align segments file by file, as opposed to in bulk" , default=True, action="store_true", required=False) # one or the other argument? check docs
    align_parser.set_defaults(func=align_corpora)

# RESCORE SUBPARSER
    rescore_parser = subparsers.add_parser("rescore", help="Rescore the corpora using more computationally expensive models.", description="Score parallel corpora. The parallel corpus file should be a TSV file with a source column, target column and, optionally, a score column. It creates a text file that should be used with the select command to filter the segments.")
    rescore_parser.add_argument("-i","--input", type=str, help="Path to the input file", required=True)
    rescore_parser.add_argument("-o","--output", type=str, help="Path to the output file", required=True)
    rescore_parser.add_argument("--SEmodel",type=str, help="Sentence Transformers embeddings model. Default model: LaBSE", required=False, default="LaBSE")
    rescore_parser.add_argument("--LDmodel",type=str, help="The fastText language detection model. Default model: lid.176.bin", required=False, default="lid.176.bin")
    rescore_parser.set_defaults(func=rescore_corpus)

# SELECT SUBPARSER
    select_parser = subparsers.add_parser("select", help="Filter the rescored parallel segments", description="Select parallel segments from a rescored text file created with rescore")
    select_parser.add_argument("-i","--input", type=str, help="Path to the input file.  This file is meant to be the resulting one from the rescore function", required=True)
    select_parser.add_argument("-o","--output", type=str, help="Path to the output", required=True)
    select_parser.add_argument("--sl", help="The source language two letter code (e.g.: en, es, ca)", required=True)
    select_parser.add_argument("--sldc", type=float, help="The minimum source language detection confidence. Default value is 0.75", required=False, default=0.75)
    select_parser.add_argument("--tl", help="The target language two letter code.", required=True)
    select_parser.add_argument("--tldc", type=float, help="The minimum target language detection confidence. Default value is 0.75", required=False, default=0.75)
    select_parser.add_argument("--minSBERT", type=float, help="The minimum value for SBERT cosine similarity score to select a segment pair. Default value is 0.75", required=False, default=0.75)
    select_parser.set_defaults(func=select_corpus)

# PIPELINE SUBPARSER WIP
    pipeline_parser = subparsers.add_parser("pipeline", help="Execute the whole pipeline: create > segment > align > rescore > select", formatter_class=argparse.RawDescriptionHelpFormatter, description=
    ''' Run the following pipeline:

        1. Create parallel corpora from Wikipedia dumps.
        2. Segment the content of both corpora in sentences.
        3. Perform bitext mining (alignment) on both corpora.
        4. Rescore the corpora.
        5. Filter the rescored parallel segments. ''')
    
    pipeline_parser.add_argument('lang1', help='Name or two letter ISO code of the source language.')
    pipeline_parser.add_argument('lang2', help='Name or two letter ISO code of the target language.')
    pipeline_parser.add_argument("--outdir", help="Name of the output directory, default is: corpora. Language codes will be added after it, i.e.: corpora-lang1-lang2/", required=False)
    # pipeline_parser.add_argument('-mono', '--monolingual', help='Create and segment a monolingual corpus.')
    
    # CREATE OPTIONS
    create_group = pipeline_parser.add_argument_group("Create options")
    create_group.add_argument('categories', help='Wikipedia categories to search. Must be in between quotation marks (""). If there is more than one, they must be separated by a comma (,).')
    create_group.add_argument('depth', type=int, help='The category level depth.')
    create_group.add_argument('--restrict', action='store_true', help='Restrict L2 pages to equivalent L1 pages.')
    create_group.add_argument("--database", help='The CCW sqlite database to use. Default: database/CCWikipedia-20251201.sqlite', required=False)
    create_group.add_argument('--dumps', help='Wikipedia dumps path. Default: dumps/', required=False)    

    # SEGMENT OPTIONS
    segment_group = pipeline_parser.add_argument_group("Segment options")
    segment_group.add_argument("--srxfile", type=str, help="The SRX file to use. Default: segment.srx", required=False)
    segment_group.add_argument("-p", "--paramark", action="store_true", help="Add the <p> paragraph mark (useful for Hunalign).", required=False)

    # ALIGN OPTIONS
    align_group = pipeline_parser.add_argument_group("Align options")
    align_group.add_argument("-dev", "--device", default="cpu", help="The device used to align segments (GPU or CPU). Default: CPU.", required=False)

    # RESCORE OPTIONS
    rescore_group = pipeline_parser.add_argument_group("Rescore options")
    rescore_group.add_argument("--SEmodel", help="Sentence Transformers embeddings model. Default: LaBSE", required=False, default="LaBSE")
    rescore_group.add_argument("--LDmodel", help="The fastText language detection model. Default: lid.176.bin", required=False, default="lid.176.bin")

    # SELECT OPTIONS
    select_group = pipeline_parser.add_argument_group("Select options")
    select_group.add_argument("--sldc", type=float, help="The minimum source language detection confidence. Default: 0.75", required=False, default=0.75)
    select_group.add_argument("--tldc", type=float, help="The minimum target language detection confidence. Default: 0.75", required=False, default=0.75)
    select_group.add_argument("--minSBERT", type=float, help="The minimum value for SBERT cosine similarity score to select a segment pair. Default: 0.75", required=False, default=0.75)

    pipeline_parser.set_defaults(func=pipeline)

# parsing all args
    args = parser.parse_args() 
    args.func(args)

if __name__ == "__main__":

    cli()