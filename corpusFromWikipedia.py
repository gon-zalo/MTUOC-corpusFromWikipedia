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

import sqlite3
import os
import gzip
import re
from bz2 import BZ2File as bzopen
import codecs
from lxml import etree as et
import sys
import argparse

import mwxml
import mwparserfromhell
import bz2


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

def extract_text_from_wikitext(wikitext):
    wikicode = mwparserfromhell.parse(wikitext)
    return wikicode.strip_code()
    
def go():
    global categoria
    global level
    global articlelist
    categories=[]
    categoriesTEMP=[]

    for cat in categoria.split(","):
        cat=cat.strip()
        categories.append(cat)
        categoriesTEMP.append(cat)
    categoriesAUX=[]
    while level>0:
        while(len(categoriesTEMP))>0:
            categoria=categoriesTEMP.pop(0)
            cur.execute('SELECT categoryREL from categoryrelations WHERE category=?', (categoria,))
            data=cur.fetchall()
            for d in data:
                categories.append(d[0])
                categoriesAUX.append(d[0])
        categoriesTEMP.extend(categoriesAUX)
        categoriesAUX=[]
        level-=1
           
    print("TOTAL CATEGORIES",len(categories))
    
    contlang=0
    restrictedIdentsKeys=[]
    if not os.path.exists(outdir):
        os.makedirs(outdir) 
    
    for lang in langs:
        contlang+=1
        idents={}
        
        articlelist="articlelist-"+lang+".txt"
        articlelistpath = os.path.join(outdir, articlelist)
        alf=codecs.open(articlelistpath,"w",encoding="utf-8")
        
        selectcategories=True
        
        if contlang==2 and restrict:
            selectcategories=False
        
        if selectcategories:
            for category in categories:
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
        print("TOTAL PAGES",lang,len(todownload))
        alf.close()
        
    '''
    a=input("Proceed? (Y/N) ")
    
    if not (a=="Y" or a=="y"):
        sys.exit()
    '''
    if create:
        print("Creating corpora from dumps")
        contlang=0
        for lang in langs:
            contlang+=1
            titlesfile="articlelist-"+lang+".txt"
            titlesfilepath = os.path.join(outdir, titlesfile)
            usertitles=[]
            entrada=codecs.open(titlesfilepath,"r",encoding="utf-8")
            for linia in entrada:
                linia=linia.rstrip()
                usertitles.append(linia)
            entrada.close()

            pagesdir="pages-"+lang
            pagesdirpath = os.path.join(outdir, pagesdir)
            if not os.path.exists(pagesdirpath):
                os.makedirs(pagesdirpath) 
                
            if contlang==1: dump_path=dumpL1           
            if contlang==2: dump_path=dumpL2
            with bz2.open(dump_path, 'rb') as f:
                # Parse the dump file
                dump = mwxml.Dump.from_file(f)
                
                # Iterate over each page in the dump
                for page in dump:
                    if not page.redirect:  # Skip redirect pages
                        if page.title in usertitles:
                            print(f"Title: {page.title}")
                            for revision in page:
                                # Extract categories from the wikitext
                                #categories = extract_categories_from_wikitext(revision.text, category_namespace)
                                
                                text = extract_text_from_wikitext(revision.text)
                                                                
                                filename=page.title.replace(" ","_")+".txt"
                                full_path = os.path.join(pagesdirpath, filename)
                                try:
                                    sortida=codecs.open(full_path,"w",encoding="utf-8")
                                    sortida.write(page.title+"\n")
                                    linies=text.split("\n")
                                    for linia in linies:
                                        linia=linia.strip()
                                        
                                        if not linia.startswith(category_namespaces[lang]) and not linia.startswith("|") and not linia.startswith("<") and not linia.startswith("!") and not linia.startswith("{")and len(linia)>0:
                                            sortida.write(linia+"\n")
                                    sortida.close()
                                except:
                                    print("ERROR:",sys.exc_info())
        
    else:
        print("Not creating corpora from dumps.")


if __name__ == "__main__":        
    parser = argparse.ArgumentParser(description='Script for the creation of specialized parallel corpora from Wikipedia.')
    
    parser.add_argument("-d",'--database', action="store", dest="filename", help='The CCW sqlite database to use.',required=True)
    
    parser.add_argument('--dumpL1', action="store", dest="dumpL1", help='The Wikipedia dump for language 1.',required=True)  
    
    parser.add_argument('--dumpL2', action="store", dest="dumpL2", help='The Wikipedia dump for language 2.',required=True)   

    parser.add_argument("-c",'--categories', action="store", dest="categoria", help='The categories to search for (a category or a list of categories separated by ",".',required=True)

    parser.add_argument('--depth', action="store", dest="level", type=int, help='The category level depth.',required=True)
    
    parser.add_argument('--lang1', action="store", dest="lang1", help='The language 1 code (two letter ISO code used in Wikipedia.',required=True)
    
    parser.add_argument('--lang2', action="store", dest="lang2", help='The language 2 code (two letter ISO code used in Wikipedia.',required=False)
    
    parser.add_argument("-o",'--outdir', action="store", dest="outdir", help='The path to the outdir where all the results will be stored..',required=True)
    
    parser.add_argument('--restrict', action='store_true', help='Restric L2 pages to equivalents to L1 pages.')
    
    parser.add_argument('--create', action='store_true', help='Create the corpora from the dumps.')
    
    parser.add_argument('--segment', action='store_true', help='Create a segmented version of the corpora.')
    
    parser.add_argument('--align', action='store_true', help='Perform the bitext mining.')
    
    parser.add_argument('--rescore', action='store_true', help='Perform the rescoring process.')
    
    parser.add_argument('--select', action='store_true', help='Select the rescored segments.')
    
    parser.add_argument("-LDmodel",type=str, help="The fasttext language detection model for rescoring. Default model: lid.176.bin", required=False, default="lid.176.bin")
    
    parser.add_argument("-SEmodel",type=str, help="The SentenceTransformer model to calculate cosinus similarity for rescoring. Default model: LaBSE", required=False, default="LaBSE")
        
    parser.add_argument("--sldc", type=float, help="The minimum source language detection confidence for selection (default 0.75).", required=False, default=0.75)
        
    parser.add_argument("--tldc", type=float, help="The minimum target language detection confidence for selection (default 0.75).", required=False, default=0.75)
    
    parser.add_argument("-m","--minSBERT", type=float, help="The minimum value for SBERT score for selection (default 0.75).", required=False, default=0.75)

    


  
        
    
    
        
    args = parser.parse_args()  
    
    
    filename=args.filename

    conn=sqlite3.connect(filename)
    cur = conn.cursor() 

    categoria=args.categoria
    level=args.level
    lang1=args.lang1
    lang2=args.lang2
    outdir=args.outdir
    
    langs=[]
    langs.append(lang1)
    if not lang2==None:
        langs.append(lang2)
    restrict=args.restrict
    dumpL1=args.dumpL1
    dumpL2=args.dumpL2
    create=args.create
    
    go()
