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
from ..utils.get_language import get_language

# create functions
def extract_text_from_wikitext(wikitext):
    import mwparserfromhell
    wikicode = mwparserfromhell.parse(wikitext)
    return wikicode.strip_code()
    
def create_corpora(args):
    import sqlite3
    import mwxml
    import bz2
    from pathlib import Path
    import os
    import sys

    lang1_name, lang1_code = get_language(args.lang1)
    lang2_name, lang2_code = get_language(args.lang2)

    langs = [lang1_code]

    if lang2_code is not None:
        langs.append(lang2_code)

    dumps = args.dumps
    if not dumps:
        dumps = 'dumps/'
    dumps_path = Path(dumps)

    dumpL1 = next(dumps_path.glob(f'{lang1_code}*'), None)
    if dumpL1:
        print(f'Dump in {lang1_name} found: {str(dumpL1)}')
    else:
        print(f'{lang1_name} dump not found in directory.')

    dumpL2 = next(dumps_path.glob(f'{lang2_code}*'), None)
    if dumpL2:
        print(f'Dump in {lang2_name} found: {str(dumpL2)}')
    else:
        print(f'{lang2_name} dump not found in directory.')


    outdir = args.outdir
    if not outdir:
        outdir = f'corpora-{lang1_code}-{lang2_code}'
    if outdir:
        outdir = f'{outdir}-{lang1_code}-{lang2_code}'

    categories = args.categories
    level = args.depth
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