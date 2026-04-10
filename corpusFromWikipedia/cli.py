#!/usr/bin/python3
# Copyright (C) 2021  Antoni Oliver
# Copyright (C) 2026  Gonzalo López Sánchez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from .commands.create import create_corpora
from .commands.segment import segment_corpus
from .commands.align import align_corpora
from .commands.rescore import rescore_corpus
from .commands.select import select_corpus
from .commands.pipeline import pipeline

import argparse

# cli function
def cli():          
    parser = argparse.ArgumentParser(
        description=
        '''
        Tool that allows the creation, segmentation, alignment, rescoring and segment selection of parallel corpora from Wikipedia. It supports step-by-step execution and full pipeline execution. That is, commands may be run individually, e.g., in order to inspect results between steps, or execute the entire workflow at once using the 'pipeline' command. The tool also allows the creation and segmentation of monolingual corpora.
        '''
        )
    subparsers = parser.add_subparsers(required=True)
    
# CREATE SUBPARSER
    create_parser = subparsers.add_parser("create", help="Create parallel corpora from Wikipedia.", description="Create parallel corpora from Wikipedia dumps.")
    create_parser.add_argument('lang1', help='Name or ISO code of the source language.')
    create_parser.add_argument('lang2', help='Name or ISO code of the target language. Keep empty to create a monolingual corpus.', nargs="?", default=None)
    create_parser.add_argument("categories", action="store", nargs="?", help='Wikipedia categories to search. Must be in between quotation marks (""). If there is more than one, they must be separated by a comma (,). Keep empty to create a corpus of the whole dump.')
    create_parser.add_argument('--depth', type=int, help='The category level depth.', required=False)
    create_parser.add_argument('--restrict', action='store_true', help='Restrict L2 pages to equivalents to L1 pages.', required=False)
    create_parser.add_argument('--database', action="store", dest="database", help='The CCW sqlite database to use. Default: database/CCWikipedia-20251201.sqlite', default= 'database/CCWikipedia-20251201.sqlite', required=False)
    create_parser.add_argument('--dumps', help='Wikipedia dumps path. Default: dumps/', default="dumps", required=False)   
    create_parser.add_argument('--outdir', help='Name of the output directory. Default: corpora-lang1-lang2/. Language codes will be added automatically.',required=False)
    create_parser.set_defaults(func=create_corpora)

# SEGMENT SUBPARSER
    segment_parser = subparsers.add_parser("segment", help="Segment the extracted text.", description="Segment all text files in a folder. The tool looks for a folder named 'pages' with the language code at the end, e.g.: 'pages-en'. The output is a 'segments' folder and a 'unique-segments' text file with the language ISO code at the end.")
    segment_parser.add_argument("indir", help="Folder where the corpus to segment is stored, i.e., the 'pages' folder.")
    segment_parser.add_argument("--srxfile", type=str, help="The SRX file to use. Default: segment.srx", default='segment.srx', required=False)
    # segment_parser.add_argument("-l", "--srxlang", type=str, help="The language as stated in the SRX file, i.e. the name of the language.", required=True)
    segment_parser.add_argument("--paramark", action="store_true", help="Add the <p> paragraph mark (useful for Hunalign).", required=False)
    segment_parser.add_argument("--outdir", type=str, help="Output directory in which to save the segmented files. If not specified, it will be saved in the same directory as the input file.", required=False)
    segment_parser.set_defaults(func=segment_corpus)

# ALIGN SUBPARSER
    align_parser = subparsers.add_parser("align", help="Perform bitext mining (alignment) between both corpora.", description="Align parallel sentences from two lists of monolingual sentences. The input files should be text files of segmented text named: unique-segments and containing the language ISO code at the end, e.g.: 'unique-segments-en.txt. The output is an aligned segments text file.")
    align_parser.add_argument("indir", help="Path to the folder that contains the unique segments files.")
    align_parser.add_argument("-dev", "--device", choices=["gpu", "cpu"], default="gpu", dest="device", help="Device used (GPU or CPU). Default: GPU.", required=False)
    # align_parser.add_argument("--file-by-file", help="Align segments file by file, as opposed to in bulk" , default=True, action="store_true", required=False) # not implemented
    align_parser.add_argument("--outdir", help="Output directory in which to save the aligned segments files. If not specified, it will be saved in the same directory as the input file.", required=False)
    align_parser.set_defaults(func=align_corpora)

# RESCORE SUBPARSER
    rescore_parser = subparsers.add_parser("rescore", help="Rescore the corpora using more computationally expensive models.", description="Rescore previously aligned corpora. The aligned segments are evaluated using more computationally expensive models. The input file should be a text file of aligned segments containing the ISO language codes, e.g.: 'aligned-segments-en-es.txt'. The output file is a rescored segments text file.")
    rescore_parser.add_argument("indir", help="Path to the folder that contains an aligned segments file.")
    rescore_parser.add_argument("--SEmodel", help="Sentence Transformers embeddings model. Default model: LaBSE", required=False, default="LaBSE")
    rescore_parser.add_argument("--LDmodel", help="The fastText language detection model. Default model: lid.176.bin", required=False, default="lid.176.bin")
    rescore_parser.add_argument("--outdir", help="Output directory in which to save the rescored segments file. If not specified, it will be saved in the same directory as the input file.")   
    rescore_parser.set_defaults(func=rescore_corpus)

# SELECT SUBPARSER
    select_parser = subparsers.add_parser("select", help="Filter the rescored parallel segments", description="Filter the rescored segments based on the defined quality thresholds. The input file should be a text file of rescored segments containing the ISO language codes, e.g.: 'rescored-segments-en-es.txt'. The output file is a selected segments text file.")
    select_parser.add_argument("indir", help="Path to the folder that contains a rescored segments file.  This file is meant to be the resulting one from the rescore function")
    select_parser.add_argument("--sldc", type=float, help="The minimum source language detection confidence. Default value is 0.75", required=False, default=0.75)
    select_parser.add_argument("--tldc", type=float, help="The minimum target language detection confidence. Default value is 0.75", required=False, default=0.75)
    select_parser.add_argument("--minSBERT", type=float, help="The minimum value for SBERT cosine similarity score to select a segment pair. Default value is 0.75", required=False, default=0.75)
    select_parser.add_argument("--outdir", type=str, help="Output directory in which to save the selected segments file. If not specified, it will be saved in the same directory as the input file.")
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
    pipeline_parser.add_argument('lang2', help='Name or two letter ISO code of the target language. Keep empty for monolingual corpus.', nargs="?", default=None)
    pipeline_parser.add_argument("--outdir", help="Name of the output directory, default is: corpora. Language codes will be added after it, i.e.: corpora-lang1-lang2/", required=False)
    
    # CREATE OPTIONS
    create_group = pipeline_parser.add_argument_group("Create options")
    create_group.add_argument('categories', help='Wikipedia categories to search. Must be in between quotation marks (""). If there is more than one, they must be separated by a comma (,).')
    create_group.add_argument('depth', type=int, help='The category level depth.')
    create_group.add_argument('--restrict', action='store_true', help='Restrict L2 pages to equivalent L1 pages.')
    create_group.add_argument("--database", help='The CCW sqlite database to use. Default: database/CCWikipedia-20251201.sqlite', default='database/CCWikipedia-20251201.sqlite', required=False)
    create_group.add_argument('--dumps', help='Wikipedia dumps path. Default: dumps/', default='dumps',required=False)    

    # SEGMENT OPTIONS
    segment_group = pipeline_parser.add_argument_group("Segment options")
    segment_group.add_argument("--srxfile", type=str, help="The SRX file to use. Default: segment.srx", default='segment.srx', required=False)
    segment_group.add_argument("-p", "--paramark", action="store_true", help="Add the <p> paragraph mark (useful for Hunalign).", required=False)

    # ALIGN OPTIONS
    align_group = pipeline_parser.add_argument_group("Align options")
    align_group.add_argument("-dev", "--device", choices=['gpu', 'cpu'], default="gpu", help="The device used to align segments (GPU or CPU). Default: GPU.", required=False, dest='device')

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