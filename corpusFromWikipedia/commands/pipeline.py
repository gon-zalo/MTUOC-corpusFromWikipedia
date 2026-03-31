# pipeline function
from .create import create_corpora, get_language
from .segment import segment_corpus
from .align import align_corpora
from .rescore import rescore_corpus
from .select import select_corpus
from pathlib import Path
import argparse

def pipeline(args):

    outdir = args.outdir
    lang1 = args.lang1
    lang2 = args.lang2

    lang1_name, lang1_code = get_language(lang1)
    lang2_name, lang2_code = get_language(lang2)

    if not outdir:
        outdir = f'corpora-{lang1_code}-{lang2_code}'
    if outdir:
        outdir = f'{outdir}-{lang1_code}-{lang2_code}'
        
    root = Path(outdir)
    if not root.exists():
        root.mkdir(parents=True, exist_ok=True)

    print(f"\nCreating parallel corpora in {lang1_name} and {lang2_name}")

    # create args
    database = args.database
    if not database:
        database ='database/CCWikipedia-20251201.sqlite'
    dumps = args.dumps
    if not dumps:
        dumps = 'dumps/'
    categories = args.categories
    level = args.depth
    restrict = args.restrict
    
    # segment args
    srxfile = args.srxfile
    if not srxfile:
        srxfile = 'segment.srx'
    paramark = args.paramark

    # align args
    device = args.device

    # rescore args
    SEmodel = args.SEmodel
    LDmodel = args.LDmodel

    # select args
    sldc = args.sldc
    tldc = args.tldc
    minSBERT = args.minSBERT

    # create corpora
    create_args = argparse.Namespace(
        database=database, 
        lang1=lang1_code, 
        lang2=lang2_code, 
        dumps=dumps, 
        categories=categories, 
        level=level, 
        restrict=restrict,
        outdir=outdir)
    create_corpora(create_args)

    # segment corpora
    segment_args_lang1 = argparse.Namespace(
        srxfile=srxfile, 
        srxlang=lang1_name, 
        paramark=paramark, 
        indir=root / f'pages-{lang1_code}', 
        outdir=root / f'segments-{lang1_code}')
    segment_args_lang2 = argparse.Namespace(
        srxfile=srxfile, 
        srxlang=lang2_name, 
        paramark=paramark, 
        indir=root / f'pages-{lang2_code}', 
        outdir=root / f'segments-{lang2_code}')
    segment_corpus(segment_args_lang1)
    segment_corpus(segment_args_lang2)

    # align segments
    align_args = argparse.Namespace(
        device=device, 
        input_files=(root / f'segments-{lang1_code}',root / f'segments-{lang2_code}'), 
        output=root / f'aligned_segments-{lang1_code}-{lang2_code}')
    align_corpora(align_args)

    # rescore segments
    rescore_args = argparse.Namespace(
        input= root / f'aligned_segments-{lang1_code}-{lang2_code}', 
        output= root / f'rescored_segments-{lang1_code}-{lang2_code}', 
        SEmodel=SEmodel, 
        LDmodel=LDmodel)
    rescore_corpus(rescore_args)

    # select segments
    select_args = argparse.Namespace(
        input=f'rescored_segments-{lang1_code}-{lang2_code}', 
        output=f'selected_segments-{lang1_code}-{lang2_code}', 
        sl=lang1_code, 
        tl=lang2_code, 
        sldc=sldc, 
        tldc=tldc, 
        minSBERT=minSBERT)
    select_corpus(select_args)
