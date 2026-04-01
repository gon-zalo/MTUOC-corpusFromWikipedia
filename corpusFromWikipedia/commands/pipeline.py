# pipeline function
from ..utils.get_language import get_language
from .create import create_corpora
from .segment import segment_corpus
from .align import align_corpora
from .rescore import rescore_corpus
from .select import select_corpus
from pathlib import Path
import argparse
# add monolingual/bilingual

def pipeline(args):

    outdir = args.outdir
    lang1 = args.lang1
    lang2 = args.lang2

    lang1_name, lang1_code = get_language(lang1)
    lang2_name, lang2_code = get_language(lang2)

    if not outdir:
        outdir = f'corpora'
    else:
        outdir = f'{outdir}'

    outputs_folder = Path('outputs')
    outdir = Path(outdir)
    corpora_folder = outputs_folder / f'{outdir}-{lang1_code}-{lang2_code}'

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
    
    # create corpora
    create_args = argparse.Namespace(
        database=database, 
        lang1=lang1_code, 
        lang2=lang2_code, 
        dumps=dumps, 
        categories=categories, 
        depth=level, 
        restrict=restrict,
        outdir=outdir)
    create_corpora(create_args)


    # segment args
    srxfile = args.srxfile
    if not srxfile:
        srxfile = 'segment.srx'
    paramark = args.paramark

    # segment corpora
    segment_args = argparse.Namespace(
        srxfile=srxfile,  
        paramark=paramark, 
        indir=corpora_folder,
        outdir=corpora_folder)
    segment_corpus(segment_args)


    # align args
    device = args.device

    # align segments
    align_args = argparse.Namespace(
        device=device, 
        indir=corpora_folder,
        output=corpora_folder)
    align_corpora(align_args)

    # rescore args
    SEmodel = args.SEmodel
    LDmodel = args.LDmodel

    # rescore segments
    rescore_args = argparse.Namespace(
        input= root / f'aligned_segments-{lang1_code}-{lang2_code}', 
        output= root / f'rescored_segments-{lang1_code}-{lang2_code}', 
        SEmodel=SEmodel, 
        LDmodel=LDmodel)
    rescore_corpus(rescore_args)


    # select args
    sldc = args.sldc
    tldc = args.tldc
    minSBERT = args.minSBERT

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
