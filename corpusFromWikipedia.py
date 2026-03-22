#!/usr/bin/python3
#    createCCWCorpus
#    Copyright (C) 2021  Antoni Oliver
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.

#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
import regex 
import lxml.etree
import sys
import argparse
import time
import lzma
import mwparserfromhell
import urllib
import pycountry

from pathlib import Path
from typing import (List, Set, Tuple, Dict, Optional)

category_namespaces = {
    "en": "Category",
    "ceb": "Kategoriya",
    "de": "Kategorie",
    "fr": "Catégorie",
    "sv": "Kategori",
    "nl": "Categorie",
    "ru": "Категория",
    "it": "Categoria",
    "es": "Categoría",
    "pl": "Kategoria",
    "ja": "カテゴリ",
    "vi": "Thể loại",
    "war": "Kaarangay",
    "zh": "分类",
    "uk": "Категорія",
    "ar": "تصنيف",
    "pt": "Categoria",
    "fa": "رده",
    "ca": "Categoria",
    "sr": "Категорија",
    "id": "Kategori",
    "ko": "분류",
    "no": "Kategori",
    "fi": "Luokka",
    "hu": "Kategória",
    "cs": "Kategorie",
    "ro": "Categorie",
    "tr": "Kategori",
    "eu": "Kategoria",
    "eo": "Kategorio",
    "da": "Kategori",
    "bg": "Категория",
    "sk": "Kategória",
    "kk": "Санат",
    "he": "קטגוריה",
    "lt": "Kategorija",
    "hr": "Kategorija",
    "az": "Kateqoriya",
    "sl": "Kategorija",
    "et": "Kategooria",
    "el": "Κατηγορία",
    "gl": "Categoría",
    "simple": "Category",
    "th": "หมวดหมู่",
    "sh": "Kategorija",
    "be": "Катэгорыя",
    "ms": "Kategori",
    "ka": "კატეგორია",
    "hi": "श्रेणी",
    "mk": "Категорија",
    "bs": "Kategorija",
    "af": "Kategorie",
    "uz": "Turkum",
    "bn": "বিষয়শ্রেণী",
    "lv": "Kategorija",
    "hy": "Կատեգորիա",
    "tt": "Төркем",
    "ur": "زمرہ",
    "azb": "بؤلمه",
    "ta": "பகுப்பு",
    "be-tarask": "Катэгорыя",
    "zh-min-nan": "分類",
    "te": "వర్గం",
    "tl": "Kategorya",
    "jv": "Kategori",
    "oc": "Categoria",
    "tg": "Гурӯҳ",
    "su": "Kategori",
    "kn": "ವರ್ಗ",
    "mg": "Sokajy",
    "mi": "Rōpū",
    "arz": "تصنيف",
    "scn": "Categoria",
    "sa": "वर्गः",
    "ne": "श्रेणी",
    "ckb": "پۆل",
    "gd": "Roinn-seòrsa",
    "ht": "Kategori",
    "mr": "वर्ग",
    "sq": "Kategori",
    "is": "Flokkur",
    "so": "Qeyb",
    "cy": "Categori",
    "br": "Rummad",
    "co": "Categoria",
    "szl": "Kategoria",
    "tk": "Kategoriýa",
    "pnb": "زمرہ",
    "sw": "Jamii",
    "fj": "Wase",
    "lrc": "پۆل",
    "dv": "ޤިސްމު",
    "nah": "Neneuhcāyōtl",
    "bat-smg": "Kateguorėjė",
    "bug": "Kategori",
    "cu": "Катигорїꙗ",
    "kw": "Class",
    "gv": "Ronney",
    "lez": "Категория",
    "ab": "Категориа",
    "bm": "Catégorie",
    "tyv": "Категория",
    "ve": "Konḓwa",
    "sn": "Chikamu",
    "pi": "विभागो",
    "iu": "ᑎᑎᕋᐅᓯᔭᖅ",
    "ny": "Gulu",
    "min": "Kategori",
    "zu": "Isigaba",
    "qu": "Katiguriya",
    "fy": "Kategory",
    "sah": "Категория",
    "kl": "Sumut ataqatigiissut",
    "kab": "Awrir",
    "haw": "Māhele",
    "ln": "Catégorie",
    "ug": "تۈر",
    "an": "Categoría",
    "mwl": "Categoria",
    "bi": "Kategori",
    "st": "Sehlopha",
    "li": "Categorie",
    "mt": "Kategorija",
    "tpi": "Kategri",
    "hsb": "Kategorija",
    "to": "Vahe",
    "ki": "Kĩrĩ",
    "yo": "Ẹ̀ka",
    "tw": "Nkyekyɛmu",
    "mg": "Sokajy",
    "tyv": "Категория",
    "ve": "Konḓwa",
    "tum": "Tchingwe",
    "lo": "ປະເພດ",
    "lad": "Kateggoría",
    "csb": "Kategòrëjô",
    "as": "শ্ৰেণী",
    "rw": "Icyiciro",
    "xh": "Udidi",
    "ts": "Xikategoria",
    "tn": "Setlhopha",
    "tk": "Kategoriýa",
    "tw": "Nkyekyɛmu",
    "wa": "Categoreye",
    "wo": "Wàll",
    "wuu": "分类",
    "xh": "Udidi",
    "yi": "קאַטעגאָריע",
    "yo": "Ẹ̀ka",
    "diq": "Kategoriye",
    "zap": "Ninyakayu",
    "sn": "Chikamu",
    "za": "分類",
    "zu": "Isigaba",
    "ast": "Categoría"
}

def get_language(language):
    # input is iso 2 letter code or full name of the language, output is name and 2 letter iso code
    if len(language) == 2: # if language is 2 letter iso code
        language_pyc = pycountry.languages.get(alpha_2=language)
        language_code = language_pyc.alpha_2
        language_name = language_pyc.name
    elif len(language) > 2: # if language is the name of the language
        for lang in pycountry.languages:
            if lang.name.lower() == language.lower():
                language_pyc = pycountry.languages.get(name=language)
                language_code = language_pyc.alpha_2
                language_name = language_pyc.name

    return language_name, language_code

# create functions
def extract_text_from_wikitext(wikitext):
    import mwparserfromhell
    wikicode = mwparserfromhell.parse(wikitext)
    return wikicode.strip_code()
    
def create_corpora(args):
    import sqlite3
    import mwxml
    import bz2
    
    # accessing args
    # lang1 = args.lang1
    # lang2 = args.lang2

    lang1_name, lang1_code = get_language(args.lang1)
    lang2_name, lang2_code = get_language(args.lang2)

    langs = [lang1_code]

    if lang2_code is not None:
        langs.append(lang2_code)

    dumps = args.dumps
    dumps_path = Path(dumps)

    dumpL1 = next(dumps_path.glob(f'{lang1_code}*'), None)
    if dumpL1:
        print(f'Dump in {lang1_name} found: {str(dumpL1)}')
    dumpL2 = next(dumps_path.glob(f'{lang2_code}*'), None)
    if dumpL2:
        print(f'Dump in {lang2_name} found: {str(dumpL2)}')

    outdir = args.outdir
    categories = args.categories
    level = args.level
    restrict = args.restrict

    categories_list = []
    categoriesTEMP = []

    database = args.database
    if not database:
        database ='database/CCWikipedia-20251201.sqlite'
    print('Database found!')
    conn = sqlite3.connect(database)
    cur = conn.cursor() 

    for cat in categories.split(","):
        cat = cat.strip()
        categories_list.append(cat)
        categoriesTEMP.append(cat)
    categoriesAUX=[]
    while level>0:
        while(len(categoriesTEMP))>0:
            categories=categoriesTEMP.pop(0)
            cur.execute('SELECT categoryREL from categoryrelations WHERE category=?', (categories,))
            data=cur.fetchall()
            for d in data:
                categories_list.append(d[0])
                categoriesAUX.append(d[0])
        categoriesTEMP.extend(categoriesAUX)
        categoriesAUX=[]
        level-=1
           
    print("TOTAL CATEGORIES",len(categories_list))
    
    contlang=0
    restrictedIdentsKeys=[]
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    for lang in langs:
        contlang+=1
        idents={}
        
        articlelist="articlelist-"+lang+".txt"
        articlelistpath = os.path.join(outdir, articlelist)
        alf=open(articlelistpath,"w",encoding="utf-8")
        
        selectcategories=True
        
        if contlang==2 and restrict:
            selectcategories=False
        
        if selectcategories:
            for category in categories_list:
                cur.execute('SELECT ident from categories WHERE category=?', (category,))
                data=cur.fetchall()
                for d in data:
                    idents[d[0]]=1
                
        if restrict and contlang==2:
            identskeys=restrictedIdentsKeys
        else:
            identskeys=idents.keys()
    
        todownload=[]
        
        if contlang==1:
            restrictedIdentsKeys=[]
        if not lang=="en":
            for ident in identskeys:
                cur.execute('SELECT title from langlinks WHERE ident=? and lang=?', (ident,lang))
                data=cur.fetchone()
                if not data==None:
                    if contlang==1: restrictedIdentsKeys.append(ident)
                    todownload.append(data[0])
                    alf.write(data[0]+"\n")
        else:
            for ident in identskeys:
                cur.execute('SELECT title from titles WHERE ident=?', (str(ident),))
                data=cur.fetchone()
                if not data==None:
                    if contlang==1: restrictedIdentsKeys.append(ident)
                    todownload.append(data[0])
                    alf.write(data[0]+"\n")
        print("TOTAL PAGES",lang,len(todownload)) # this takes into account images (File:...), so its not accurate
        # write here code thats inside create_corpora to reflect the true number of articles
        alf.close() 
    
    print("Creating corpora from dumps")
    contlang=0
    for lang in langs:
        contlang+=1
        titlesfile="articlelist-"+lang+".txt"
        titlesfilepath = os.path.join(outdir, titlesfile)
        usertitles=[]
        entrada=open(titlesfilepath,"r",encoding="utf-8")
        for linia in entrada:
            linia=linia.rstrip()
            if linia.startswith("File:"): # remove images (File:...) from usertitles, having a more accurate total number of pages
                pass
            else:
                usertitles.append(linia)
        entrada.close()

        usertitles_set = set(usertitles) # transforming list into a set for faster lookup

        print(f"\nTitles of the pages to process in {lang.upper()}: {usertitles_set}\n") # for testing purposes
        print(f"Number of pages to process: {len(usertitles_set)}")
        print("Accessing pages files...")

        pages_processed = 0 # counter to keep track of the number of pages processed
        pagesdir="pages-"+lang
        pagesdirpath = os.path.join(outdir, pagesdir) # change to use Path library!
        if not os.path.exists(pagesdirpath):
            os.makedirs(pagesdirpath) 
            
        if contlang==1: dump_path=dumpL1           
        if contlang==2: dump_path=dumpL2
        
        print(f"Opening bz2 file for language: {lang.upper()}")
        with bz2.open(dump_path, 'rb') as f:
            # Parse the dump file
            print("Parsing dump file...")
            dump = mwxml.Dump.from_file(f)

            # Iterate over each page in the dump
            print("\nIterating over each page in the dump file...")
            

            for page in dump:
                if pages_processed == len(usertitles_set): # added so that the parsing stops once all the necessary pages have been processed which saves time and fixes some pages being processed more than once
                    print(f"\nAll pages in {lang.upper()} processed!")
                    print("----------------------\n")
                    break

                else:
                    if not page.redirect:  # Skip redirect pages
                        if page.title in usertitles_set: # using set for faster lookup
                            print(f"\nProcessing page: {page.title}")
                            for revision in page:
                                # Extract categories from the wikitext
                                #categories = extract_categories_from_wikitext(revision.text, category_namespace)
                                
                                print("Extracting text...")
                                text = extract_text_from_wikitext(revision.text)
                                                                
                                filename=page.title.replace(" ","_")+".txt"
                                full_path = os.path.join(pagesdirpath, filename)
                                try:
                                    sortida=open(full_path,"w",encoding="utf-8")
                                    print("Writing text to file...")
                                    sortida.write(page.title+"\n")
                                    linies=text.split("\n")
                                    for linia in linies:
                                        linia=linia.strip()
                                        
                                        if not linia.startswith(category_namespaces[lang]) and not linia.startswith("|") and not linia.startswith("<") and not linia.startswith("!") and not linia.startswith("{")and len(linia)>0:
                                            sortida.write(linia+"\n")
                                    sortida.close()
            

                                except:
                                    print("ERROR:",sys.exc_info())
                            print("Page processed!")
                            pages_processed += 1
                            print(f"Processed {pages_processed} out of {len(usertitles_set)}")
    # return pagesdirpath # this return is doing nothing

# segment functions
class SrxSegmenter:
    """Handle segmentation with SRX regex format.
    """
    def __init__(self, rule: Dict[str, List[Tuple[str, Optional[str]]]], source_text: str) -> None:
        self.source_text = source_text
        self.non_breaks = rule.get('non_breaks', [])
        self.breaks = rule.get('breaks', [])

    def _get_break_points(self, regexes: List[Tuple[str, str]]) -> Set[int]:
        return set([
            match.span(1)[1]
            for before, after in regexes
            for match in regex.finditer('({})({})'.format(before, after), self.source_text)
        ])

    def get_non_break_points(self) -> Set[int]:
        """Return segment non break points
        """
        return self._get_break_points(self.non_breaks)

    def get_break_points(self) -> Set[int]:
        """Return segment break points
        """
        return self._get_break_points(self.breaks)

    def extract(self) -> Tuple[List[str], List[str]]:
        """Return segments and whitespaces.
        """
        non_break_points = self.get_non_break_points()
        candidate_break_points = self.get_break_points()

        break_point = sorted(candidate_break_points - non_break_points)
        source_text = self.source_text

        segments = []  # type: List[str]
        whitespaces = []  # type: List[str]
        previous_foot = ""
        for start, end in zip([0] + break_point, break_point + [len(source_text)]):
            segment_with_space = source_text[start:end]
            candidate_segment = segment_with_space.strip()
            if not candidate_segment:
                previous_foot += segment_with_space
                continue

            head, segment, foot = segment_with_space.partition(candidate_segment)

            segments.append(segment)
            whitespaces.append('{}{}'.format(previous_foot, head))
            previous_foot = foot
        whitespaces.append(previous_foot)

        return segments, whitespaces

def parse(srx_filepath: str) -> Dict[str, Dict[str, List[Tuple[str, Optional[str]]]]]:
    """Parse SRX file and return it.
    :param srx_filepath: is soruce SRX file.
    :return: dict
    """
    tree = lxml.etree.parse(srx_filepath)
    namespaces = {
        'ns': 'http://www.lisa.org/srx20'
    }

    rules = {}

    for languagerule in tree.xpath('//ns:languagerule', namespaces=namespaces):
        rule_name = languagerule.attrib.get('languagerulename')
        if rule_name is None:
            continue

        current_rule = {
            'breaks': [],
            'non_breaks': [],
        }

        for rule in languagerule.xpath('ns:rule', namespaces=namespaces):
            is_break = rule.attrib.get('break', 'yes') == 'yes'
            rule_holder = current_rule['breaks'] if is_break else current_rule['non_breaks']

            beforebreak = rule.find('ns:beforebreak', namespaces=namespaces)
            beforebreak_text = '' if beforebreak.text is None else beforebreak.text

            afterbreak = rule.find('ns:afterbreak', namespaces=namespaces)
            afterbreak_text = '' if afterbreak.text is None else afterbreak.text

            rule_holder.append((beforebreak_text, afterbreak_text))

        rules[rule_name] = current_rule

    return rules

def segmenta(cadena, srxfile, srxlang):
    srxfile=srxfile
    srxlang= srxlang
    rules = parse(srxfile)
    
    segmenter = SrxSegmenter(rules[srxlang],cadena)
    segments=segmenter.extract()
    resposta=[]
    for segment in segments[0]:
        segment=segment.replace("’","'")
        resposta.append(segment)
    resposta="\n".join(resposta)
    return(resposta)

def detect_encoding(file_path):
    from charset_normalizer import from_path

    result = from_path(file_path).best()
    return result.encoding if result else 'utf-8'

def segment_corpus(args):
    import subprocess
    # accessing args
    srxfile=args.srxfile
    srxlang= args.srxlang
    paramark=args.paramark

    indir=args.indir
    outdir=args.outdir

    srxlang = srxlang.title() # capitalizing the first letter since that's how it's written in the srx file

    rules = parse(srxfile)
    languages = list(rules.keys())
    if not srxlang in languages:
        print("Language ",srxlang," not available in ", srxfile)
        print("Available languages:",", ".join(languages))
        sys.exit()

    print("Segmenting files...")
    # print(indir)
    for r, d, f in os.walk(indir):
        for file in f:
            # print("File found")
            if file.endswith('.txt'):
                fullpath = os.path.join(r, file)
                # print(fullpath)

                encoding = detect_encoding(fullpath)
                entrada = open(fullpath, "r", encoding=encoding, errors="ignore")

                outfile = fullpath.replace(indir, outdir)
                os.makedirs(os.path.dirname(outfile), exist_ok=True)

                sortida = open(outfile, "w", encoding="utf-8")
                for linia in entrada:
                    segments = segmenta(linia, srxfile, srxlang)
                    if len(segments) > 0:
                        if paramark:
                            sortida.write("<p>\n")
                        sortida.write(segments + "\n")

                entrada.close()
                sortida.close()

                cmd = f'cat "{outfile}" | sort | uniq | shuf' # shouldnt this be outdir not outfile? we want to concatenate all the files in the output directory and the output file needs a name
                resultat = subprocess.run(
                    cmd,
                    shell=True,
                    capture_output=True,
                    text=True
                )

                with open(outfile, "w", encoding="utf-8") as f_out:
                    f_out.write(resultat.stdout)
                

# align functions
def score(x, y, fwd_mean, bwd_mean, margin):
    return margin(x.dot(y), (fwd_mean + bwd_mean) / 2)

def score_candidates(x, y, candidate_inds, fwd_mean, bwd_mean, margin):
    import numpy as np

    scores = np.zeros(candidate_inds.shape)
    for i in range(scores.shape[0]):
        for j in range(scores.shape[1]):
            k = candidate_inds[i, j]
            scores[i, j] = score(x[i], y[k], fwd_mean[i], bwd_mean[k], margin)
    return scores

def kNN(x, y, k, use_ann_search=False, ann_num_clusters=32768, ann_num_cluster_probe=3, device="cpu"):
    import faiss
    start_time = time.time()
    
    if use_ann_search:
        # Mantinc la lògica GPU només per a la cerca aproximada (ANN)
        print("Perform approx. kNN search (GPU)")
        res = faiss.StandardGpuResources() 
        n_cluster = min(ann_num_clusters, int(y.shape[0]/1000))
        quantizer = faiss.IndexFlatIP(y.shape[1])
        index = faiss.IndexIVFFlat(quantizer, y.shape[1], n_cluster, faiss.METRIC_INNER_PRODUCT)
        gpu_index = faiss.index_cpu_to_gpu(res, 0, index)
        #sim, ind = index.search(x, k)
        #index.nprobe = ann_num_cluster_probe
        #index.train(y)
        #index.add(y)
        gpu_index.nprobe = ann_num_cluster_probe
        gpu_index.train(y)
        gpu_index.add(y)
        sim, ind = gpu_index.search(x, k)
    elif device == "gpu":
        res = faiss.StandardGpuResources()
        print("Perform exact search (GPU mode)")
        idx = faiss.IndexFlatIP(y.shape[1])
        gpu_index = faiss.index_cpu_to_gpu(res, 0, idx)
        #idx.add(y)
        #sim, ind = idx.search(x, k)
        gpu_index.add(y)
        sim, ind = gpu_index.search(x, k)
    else:
        # MODIFICACIÓ: Forcem l'ús de la CPU per a la cerca exacta
        # Això evita l'error 'cublas failed' per falta de memòria VRAM
        print("Perform exact search (CPU Mode)")
        
        # Creem l'índex directament a la CPU
        index = faiss.IndexFlatIP(y.shape[1])
        
        # Afegim els vectors i busquem
        index.add(y)
        sim, ind = index.search(x, k)

    print("Done: {:.2f} sec".format(time.time()-start_time))
    return sim, ind

def file_open(filepath):
    #Function to allowing opening files based on file extension
    import gzip
    if filepath.endswith('.gz'):
        return gzip.open(filepath, 'rt', encoding='utf-8')
    elif filepath.endswith('xz'):
        return lzma.open(filepath, 'rt', encoding='utf-8')
    else:
        return open(filepath, 'r', encoding='utf-8')

def align_corpora(args):
    print("Initializing corpora alignment...")
    from sentence_transformers import SentenceTransformer, models
    import tqdm
    from sklearn.decomposition import PCA
    import torch
    import lzma
    import numpy as np

    device = args.device
    source_file = args.input_files[0]
    target_file = args.input_files[1]
    output = args.output

    #Model we want to use for bitext mining. LaBSE achieves state-of-the-art performance
    model_name = 'LaBSE'
    model = SentenceTransformer(model_name)

    # Only consider sentences that are between min_sent_len and max_sent_len characters long
    min_sent_len = 10
    max_sent_len = 200

    # We base the scoring on k nearest neighbors for each element
    knn_neighbors = 4

    # Min score for text pairs. Note, score can be larger than 1
    min_threshold = 1

    #Do we want to use exact search of approximate nearest neighbor search (ANN)
    #Exact search: Slower, but we don't miss any parallel sentences
    #ANN: Faster, but the recall will be lower
    use_ann_search = False #True

    #Number of clusters for ANN. Each cluster should have at least 10k entries
    ann_num_clusters = 32768

    #How many cluster to explorer for search. Higher number = better recall, slower
    ann_num_cluster_probe = 3

    #To save memory, we can use PCA to reduce the dimensionality from 768 to for example 128 dimensions
    #The encoded embeddings will hence require 6 times less memory. However, we observe a small drop in performance.
    use_pca = False #True
    pca_dimensions = 128


    if use_pca:
        print("Using PCA!")
        # We use a smaller number of training sentences to learn the PCA
        train_sent = []
        num_train_sent = 20000

        with file_open(source_file) as fSource, file_open(target_file) as fTarget:
            for line_source, line_target in zip(fSource, fTarget):
                if min_sent_len <= len(line_source.strip()) <= max_sent_len:
                    sentence = line_source.strip()
                    train_sent.append(sentence)

                if min_sent_len <= len(line_target.strip()) <= max_sent_len:
                    sentence = line_target.strip()
                    train_sent.append(sentence)

                if len(train_sent) >= num_train_sent:
                    break

        print("Encode training embeddings for PCA")
        train_matrix = model.encode(train_sent, show_progress_bar=True, convert_to_numpy=True)
        pca = PCA(n_components=pca_dimensions)
        pca.fit(train_matrix)

        dense = models.Dense(in_features=model.get_sentence_embedding_dimension(), out_features=pca_dimensions, bias=False, activation_function=torch.nn.Identity())
        dense.linear.weight = torch.nn.Parameter(torch.tensor(pca.components_))
        model.add_module('dense', dense)


    print("Read source file")
    source_sentences = set()
    with file_open(source_file) as fIn:
        for line in tqdm.tqdm(fIn):
            line = line.strip()
            if len(line) >= min_sent_len and len(line) <= max_sent_len:
                source_sentences.add(line)

    print("Read target file")
    target_sentences = set()
    with file_open(target_file) as fIn:
        for line in tqdm.tqdm(fIn):
            line = line.strip()
            if len(line) >= min_sent_len and len(line) <= max_sent_len:
                target_sentences.add(line)

    print("Source Sentences:", len(source_sentences))
    print("Target Sentences:", len(target_sentences))


    ### Encode source sentences
    source_sentences = list(source_sentences)


    print("Encode source sentences")
    source_embeddings = model.encode(source_sentences, show_progress_bar=True, convert_to_numpy=True)


    ### Encode target sentences
    target_sentences = list(target_sentences)

    print("Encode target sentences")
    target_embeddings = model.encode(target_sentences, show_progress_bar=True, convert_to_numpy=True)


    # Normalize embeddings
    x = source_embeddings
    x = x / np.linalg.norm(x, axis=1, keepdims=True)

    y = target_embeddings
    y = y / np.linalg.norm(y, axis=1, keepdims=True)

    # Perform kNN in both directions
    x2y_sim, x2y_ind = kNN(x, y, knn_neighbors, use_ann_search, ann_num_clusters, ann_num_cluster_probe, device=device)
    x2y_mean = x2y_sim.mean(axis=1)

    y2x_sim, y2x_ind = kNN(y, x, knn_neighbors, use_ann_search, ann_num_clusters, ann_num_cluster_probe, device=device)
    y2x_mean = y2x_sim.mean(axis=1)

    # Compute forward and backward scores
    margin = lambda a, b: a / b
    fwd_scores = score_candidates(x, y, x2y_ind, x2y_mean, y2x_mean, margin)
    bwd_scores = score_candidates(y, x, y2x_ind, y2x_mean, x2y_mean, margin)
    fwd_best = x2y_ind[np.arange(x.shape[0]), fwd_scores.argmax(axis=1)]
    bwd_best = y2x_ind[np.arange(y.shape[0]), bwd_scores.argmax(axis=1)]

    indices = np.stack([np.concatenate([np.arange(x.shape[0]), bwd_best]), np.concatenate([fwd_best, np.arange(y.shape[0])])], axis=1)
    scores = np.concatenate([fwd_scores.max(axis=1), bwd_scores.max(axis=1)])
    seen_src, seen_trg = set(), set()

    #Extact list of parallel sentences
    print("Write sentences to disc")
    sentences_written = 0
    #with gzip.open(sys.argv[3], 'wt', encoding='utf8') as fOut:
    with open(output, 'w', encoding='utf-8') as fOut:

        for i in np.argsort(-scores):
            src_ind, trg_ind = indices[i]
            src_ind = int(src_ind)
            trg_ind = int(trg_ind)

            if scores[i] < min_threshold:
                break

            if src_ind not in seen_src and trg_ind not in seen_trg:
                seen_src.add(src_ind)
                seen_trg.add(trg_ind)
                #fOut.write("{:.4f}\t{}\t{}\n".format(scores[i], source_sentences[src_ind].replace("\t", " "), target_sentences[trg_ind].replace("\t", " ")))
                fOut.write("{}\t{}\t{:.4f}\n".format(source_sentences[src_ind].replace("\t", " "), target_sentences[trg_ind].replace("\t", " "),scores[i]))

                sentences_written += 1

    print(f"Done. {sentences_written} sentences written")
    
# rescore functions

def process(sources, targets, scores, sortida, SEmodel, LDmodel):
    print("Processing corpus...")
    from sentence_transformers import util

    SEmodel = SEmodel
    LDmodel = LDmodel
    data = []
    embeddings1 = SEmodel.encode(sources, convert_to_tensor=False)
    embeddings2 = SEmodel.encode(targets, convert_to_tensor=False)
    cosine_scores = util.cos_sim(embeddings1, embeddings2)

    for i in range(len(sources)):
        try:
            source = sources[i]
            target = targets[i]
            score = float(scores[i]) if scores[i] != 0 else 0.0
            cosine_score = cosine_scores[i][i].item()
            
            # Language detection predictions
            DL1 = LDmodel.predict(source, k=5)
            DL2 = LDmodel.predict(target, k=5)
            
            # Validate DL1 and DL2 have enough predictions
            if len(DL1[0]) < 1 or len(DL2[0]) < 1:
                print(f"WARNING: Insufficient labels for line {i}")
                continue
            
            predL1 = []
            for j in range(len(DL1)-1):
                L1 = DL1[0][j].replace("__label__", "")
                confL1 = float(DL1[1][j])
                predL1.append(f"{L1}:{confL1}")
            predL1 = ";".join(predL1)

            predL2 = []
            for j in range(len(DL2)-1):
                L2 = DL2[0][j].replace("__label__", "")
                confL2 = float(DL2[1][j])
                predL2.append(f"{L2}:{confL2}")
            predL2 = ";".join(predL2)

            # Write the output line
            cadena = f"{source}\t{target}\t{predL1}\t{predL2}\t{cosine_score}"
            sortida.write(cadena + "\n")

        except Exception as e:
            print(f"ERROR processing line {i}: {e}")

def rescore_corpus(args):
    print("Rescoring corpus...")
    import fasttext
    from sentence_transformers import SentenceTransformer, util

    input_file = args.input
    output_file = args.output
    SEmodel = args.SEmodel
    LDmodel = args.LDmodel
    maxlines = 10000

    LDmodel_url = 'https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin'
    LDmodel_path = Path('lid.176.bin')

    if LDmodel_path.exists():
        try:
            LDmodel = fasttext.load_model(LDmodel)
            print("\nLanguage detection model loaded successfully")
        except Exception as e:
            print(f"Error loading model: {e}")
    else:
        print(f"\nModel not found in path {LDmodel_path}")
        print("Downloading model")
        try:
            LDmodel_path.parent.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(LDmodel_url, str(LDmodel_path))
            LDmodel = fasttext.load_model(LDmodel)
            print("\nLanguage detection model downloaded and loaded successfully")
        except Exception as e:
            print(f"Error downloading model: {e}")

    SEmodel = SentenceTransformer(SEmodel)
    print("\nSentence embeddings model loaded successfully\n")

    entrada = open(input_file,"r",encoding="utf-8")
    sortida = open(output_file,"w",encoding="utf-8")

    cont = 0
    cont2 = 0
    sources = []
    targets = []
    scores = []

    for linia in entrada:
        linia=linia.rstrip()
        camps=linia.split("\t")
        try:
            sources.append(camps[0])
            targets.append(camps[1])
            if len(camps)>=3:
                scores.append(camps[2])
            else:
                scores.append(0)
        except:
            pass
        cont+=1
        if cont%maxlines==0:
            print("CONT: ",cont2*maxlines)
            cont2+=1
        if cont==maxlines:
            process(sources,targets,scores,sortida, SEmodel, LDmodel)
            cont=0
            sources=[]
            targets=[]
            scores=[]
    process(sources,targets,scores,sortida, SEmodel, LDmodel)

# select functions
def select_corpus(args):
    print("Selecting segments from file...")
    input_file = args.input
    output_file = args.output
    sl = args.sl
    tl = args.tl
    sldc = args.sldc
    tldc = args.tldc
    minSBERT = args.minSBERT

    if minSBERT == None:
        minSBERT = -1000000
    else:
        minSBERT = float(minSBERT)


    sortida=open(output_file,"w",encoding="utf-8")
    entrada=open(input_file,"r",encoding="utf-8")

    for linia in entrada:
        linia=linia.rstrip()
        camps=linia.split("\t")
        slsegment=camps[0]
        tlsegment=camps[1]
        slinfolangs=camps[2]
        slinfolang1=slinfolangs.split(";")[0]
        sllang=slinfolang1.split(":")[0]
        slconf=float(slinfolang1.split(":")[1])
        
        tlinfolangs=camps[3]
        tlinfolang1=tlinfolangs.split(";")[0]
        tllang=tlinfolang1.split(":")[0]
        tlconf=float(tlinfolang1.split(":")[1])
        
        sbert=float(camps[4])
        
        if sllang==sl and slconf>=sldc and tllang==tl and tlconf>=tldc and sbert>=minSBERT:
            cadena=slsegment+"\t"+tlsegment
            #print(cadena)
            sortida.write(cadena+"\n")

# pipeline function
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
    level = args.level
    restrict = args.restrict
    
    # segment args
    srxfile = args.srxfile
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

# main func
def go():          
    parser = argparse.ArgumentParser(
        description=
        '''
        Script that allows the creation, segmentation, alignment, rescoring and segment selection of parallel corpora from Wikipedia. It supports step-by-step execution and full pipeline execution. That is, commands may be run individually, e.g., in order to inspect results between steps, or execute the entire workflow at once using the 'pipeline' command.
        '''
        )
    subparsers = parser.add_subparsers(required=True)
    
# CREATE SUBPARSER
    create_parser = subparsers.add_parser("create", help="Create parallel corpora from Wikipedia.", description="Create parallel corpora from Wikipedia dumps")
    create_parser.add_argument("-d",'--database', action="store", dest="database", help='The CCW sqlite database to use.',required=True)
    create_parser.add_argument('--dumps', help='Wikipedia dumps path', required=True)   
    create_parser.add_argument("-c",'--categories', action="store", help='The categories to search for (a category or a list of categories separated by ",".',required=True)
    create_parser.add_argument('--depth', action="store", dest="level", type=int, help='The category level depth.',required=True)
    create_parser.add_argument('--lang1', action="store", dest="lang1", help='The language 1 code (two letter ISO code used in Wikipedia.)',required=True)
    create_parser.add_argument('--lang2', action="store", dest="lang2", help='The language 2 code (two letter ISO code used in Wikipedia.)',required=False)
    create_parser.add_argument("-o",'--outdir', action="store", dest="outdir", help='The path to the output directory, where all the results will be stored.',required=True)
    create_parser.add_argument('--restrict', action='store_true', help='Restrict L2 pages to equivalents to L1 pages.')
    create_parser.set_defaults(func=create_corpora)

# SEGMENT SUBPARSER
    segment_parser = subparsers.add_parser("segment", help="Create a segmented version of the extracted texts.", description="Create a segmented version of the corpora. Can be used on its own to segment all the files in one directory.")
    segment_parser.add_argument("-i", "--indir", type=str, help="Path to the input directory.", required=False)
    segment_parser.add_argument("-o", "--outdir", type=str, help="The output directory in which to save the segmented files. If it doesn't exist, it will be created", required=True)
    segment_parser.add_argument("-s", "--srxfile", type=str, help="The SRX file to use", required=True)
    segment_parser.add_argument("-l", "--srxlang", type=str, help="The language as stated in the SRX file, i.e. the name of the language.", required=True)
    segment_parser.add_argument("-p", "--paramark", action="store_true", help="Add the <p> paragraph mark (useful for Hunalign).", required=False)
    segment_parser.set_defaults(func=segment_corpus)

# ALIGN SUBPARSER
    align_parser = subparsers.add_parser("align", help="Perform bitext mining (alignment) between both corpora.", description="Mine parallel (translated) sentences from two lists of monolingual sentences.")
    align_parser.add_argument("-i", "--input", nargs=2, metavar=("FILE1", "FILE2"), dest="input_files", help="Path to the two file paths that will be aligned", required=True)
    align_parser.add_argument("-o", "--output", help="The output file path", required=True)
    align_parser.add_argument("-dev", "--device", default="cpu", dest="device", help="The deviced used (gpu or cpu). Default is cpu.", required=False)
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
    select_parser = subparsers.add_parser("select", help="Filters the rescored parallel segments", description="Select parallel segments from a rescored text file created with rescore")
    select_parser.add_argument("-i","--input", type=str, help="Path to the input file.  This file is meant to be the resulting one from the rescore function", required=True)
    select_parser.add_argument("-o","--output", type=str, help="Path to the output", required=True)
    select_parser.add_argument("--sl", help="The source language two letter code (e.g.: en, es, ca)", required=True)
    select_parser.add_argument("--sldc", type=float, help="The minimum source language detection confidence. Default value is 0.75", required=False, default=0.75)
    select_parser.add_argument("--tl", help="The target language two letter code.", required=True)
    select_parser.add_argument("--tldc", type=float, help="The minimum target language detection confidence. Default value is 0.75", required=False, default=0.75)
    select_parser.add_argument("--minSBERT", type=float, help="The minimum value for SBERT cosine similarity score to select a segment pair. Default value is 0.75", required=False, default=0.75)
    select_parser.set_defaults(func=select_corpus)

# PIPELINE SUBPARSER WIP
    pipeline_parser = subparsers.add_parser("pipeline", help="Run the whole pipeline: create > segment > align > rescore > select", formatter_class=argparse.RawDescriptionHelpFormatter, description=
    ''' Run the following pipeline:

        1. Create parallel corpora from Wikipedia dumps.
        2. Segment the content of both corpora in sentences.
        3. Perform bitext mining (alignment) on both corpora.
        4. Rescore the corpora.
        5. Filter the rescored parallel segments. ''')
    
    pipeline_parser.add_argument('lang1', help='Name or two letter ISO code of the source (first) language.')
    pipeline_parser.add_argument('lang2', help='Name or two letter ISO code of the target (second) language.')
    pipeline_parser.add_argument("--outdir", help="Name of the output directory, default is: corpora. Language codes will be added after it, i.e.: corpora-lang1-lang2/", required=False)
    
    # CREATE OPTIONS
    create_group = pipeline_parser.add_argument_group("Create options")
    create_group.add_argument('categories', help='Wikipedia categories to search for. Must be in between quotation marks ("") and separated by a comma (,) if there are more than one.')
    create_group.add_argument('-d', '--depth', dest="level", type=int, help='The category level depth (required).',required=True)
    create_group.add_argument("--database", help='The CCW sqlite database to use. Default: database/CCWikipedia-20251201.sqlite', required=False)
    create_group.add_argument('--dumps', help='Wikipedia dumps path. Default: dumps/.', required=False)    
    create_group.add_argument('--restrict', action='store_true', help='Restrict L2 pages to equivalents to L1 pages.')

    # SEGMENT OPTIONS
    segment_group = pipeline_parser.add_argument_group("Segment options")
    segment_group.add_argument("--srxfile", type=str, help="The SRX file to use (required)", required=True)
    segment_group.add_argument("-p", "--paramark", action="store_true", help="Add the <p> paragraph mark (useful for Hunalign).", required=False)

    # ALIGN OPTIONS
    align_group = pipeline_parser.add_argument_group("Align options")
    align_group.add_argument("-dev", "--device", default="cpu", help="The deviced used to align segments (GPU or CPU). Default: CPU.", required=False)

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

    go()