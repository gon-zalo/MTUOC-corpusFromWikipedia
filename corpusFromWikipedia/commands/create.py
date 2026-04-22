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
    "ast": "Categoría",
    "fur": "Categorie",
    "lij": "Categoria",
    "la": "Categoria",
    "lld": "Categoria",
    "lmo": "Categoria",
    "pms": "Categoria",
    "rm": "Categoria"
}
from ..utils.get_language import get_language

# create functions
def extract_text_from_wikitext(wikitext):
    import mwparserfromhell
    wikicode = mwparserfromhell.parse(wikitext)
    return wikicode.strip_code()
    
def find_dumps(dumps_path, lang_code, lang_name):
    import sys
    dump = next(dumps_path.glob(f'{lang_code}*'), None)
    if dump:
        print(f'\nDump in {lang_name} found: {str(dump)}')
    else:
        print(f'{lang_name} dump not found in directory.')
        sys.exit()

    return dump


def create_corpora(args):
    import sqlite3
    import mwxml
    import bz2
    from pathlib import Path
    import os
    import sys
    print("\nRunning create command")
    
    lang1 = args.lang1
    lang2 = args.lang2

    dumps = args.dumps or 'dumps'
    dumps_path = Path(dumps)

    outdir = args.outdir

    if lang2: # if bilingual
        print("Creating bilingual corpus")
        lang1_name, lang1_code = get_language(lang1)
        lang2_name, lang2_code = get_language(lang2)
        langs = [lang1_code, lang2_code]

        dumpL1 = find_dumps(dumps_path, lang1_code, lang1_name)
        dumpL2 = find_dumps(dumps_path, lang2_code, lang2_name)

        if not outdir:
            outdir = f'../corpora/corpus-{lang1_code}-{lang2_code}'
        else:
            outdir = f'../corpora/{outdir}-{lang1_code}-{lang2_code}'

    else: # if monolingual
        print("Creating monolingual corpus")
        lang1_name, lang1_code = get_language(lang1)
        langs = [lang1_code]

        dumpL1 = find_dumps(dumps_path, lang1_code, lang1_name)

        if not outdir:
            outdir = f'../corpora/corpus-{lang1_code}'
        else:
            outdir = f'../corpora/{outdir}-{lang1_code}'


    outdir = Path(outdir)
    if outdir.exists():
        print("Directory already exists, choose another name")
        sys.exit()
    else:
        outdir.mkdir(parents=True)
        print(f"Creating folder {outdir}")
    
    categories = args.categories
    database = args.database
    print(f'Database found: {database}')
    conn = sqlite3.connect(database)
    cur = conn.cursor() 
    selectcategories = False
    if categories: # if you want to fetch specific categories
        print("Fetching categories")
        level = args.depth
        if categories and not level:
            print("Error: '--depth' is required when categories is provided")
            sys.exit(1)

        categories_list = []

        categoriesTEMP = []

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
        selectcategories=True
        print("\nTotal categories:",len(categories_list))

    restrict = args.restrict
    contlang=0
    restrictedIdentsKeys=[]

    for lang in langs:
        lang_name, lang_code = get_language(lang)

        contlang+=1
        idents={}
        
        articlelist="articlelist-"+lang_code+".txt"
        articlelistpath = os.path.join(outdir, articlelist)
        alf=open(articlelistpath,"w",encoding="utf-8")
        
        if contlang==2 and restrict:
            selectcategories=False

        if selectcategories:
            print("Categories found. The selected categories will be processed.")
            for category in categories_list:
                cur.execute('SELECT ident from categories WHERE category=?', (category,))
                data=cur.fetchall()
                for d in data:
                    idents[d[0]]=1
                
        else:
            print("No categories selected. The whole dump will be processed.")
            cur.execute('SELECT ident FROM titles')
            data = cur.fetchall()
            for d in data:
                idents[d[0]] = 1

        if restrict and contlang==2:
            identskeys=restrictedIdentsKeys
        else:
            identskeys=idents.keys()
    
        todownload=[]
        
        if contlang==1:
            restrictedIdentsKeys=[]

        if not lang_code=="en":
            for ident in identskeys:
                cur.execute('SELECT title from langlinks WHERE ident=? and lang=?', (ident,lang_code))
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
        print(f"Total number of pages in {lang_name}: {len(todownload)}") # this takes into account images (File:...), so its not accurate
        # write here code thats inside create_corpora to reflect the true number of articles
        alf.close() 
    
    print("\nCreating corpora from dumps!")
    
    contlang=0
    for lang in langs:

        lang_name, lang_code = get_language(lang)

        contlang+=1
        titlesfile="articlelist-"+lang_code+".txt"
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

        # print(f"\nTitles of the pages to process in {lang_name}: {usertitles_set}\n") # for testing purposes
        print(f"\nProcessing pages in {lang_name}")
        print(f"Number of pages to process: {len(usertitles_set)}")
        # print("Accessing pages files...")

        pages_processed = 0 # counter to keep track of the number of pages processed
        pagesdir="pages-"+lang_code
        pagesdirpath = os.path.join(outdir, pagesdir) # change to use Path library!

        processed_articles_set = set()
        processed_articles_file = f'processed-articles-{lang_code}.txt'
        processed_articles_path = outdir / processed_articles_file

        if not processed_articles_path.exists():
            print(f"processed-articles{lang_code}.txt file created")
            processed_articles_path.touch()

        else: # if it exists open it, read the titles and add them to the set that it's getting checked later on
            with open(processed_articles_path, "r", encoding='utf-8') as f:
                for line in f:
                    processed_articles_set.add(line.strip())
                print(f"Number of processed articles: {len(processed_articles_set)}")

        if not os.path.exists(pagesdirpath):
            os.makedirs(pagesdirpath) 
            
        if contlang==1: dump_path=dumpL1           
        if contlang==2: dump_path=dumpL2
        
        with bz2.open(dump_path, 'rb') as f:
            # Parse the dump file
            # print("Parsing dump file...")
            dump = mwxml.Dump.from_file(f)

            # Iterate over each page in the dump
            # print("\nIterating over each page in the dump file...")
            

            for page in dump:
                if pages_processed == len(usertitles_set): # added so that the parsing stops once all the necessary pages have been processed which saves time and fixes some pages being processed more than once
                    print(f"\nAll pages in {lang_name} processed!")
                    print("----------------------\n")
                    break

                else:
                    if not page.redirect:  # Skip redirect pages

                        if page.title in processed_articles_set:
                            continue

                        if page.title in usertitles_set: # using set for faster lookup
                            print(f"Processing page: {page.title}")
                            for revision in page:
                                # Extract categories from the wikitext
                                #categories = extract_categories_from_wikitext(revision.text, category_namespace)
                                
                                # print("Extracting text...")
                                text = extract_text_from_wikitext(revision.text)
                                                                
                                filename=page.title.replace(" ","_")+".txt"
                                full_path = os.path.join(pagesdirpath, filename)
                                try:
                                    sortida=open(full_path,"w",encoding="utf-8")
                                    # print("Writing text to file...")
                                    sortida.write(page.title+"\n")
                                    linies=text.split("\n")
                                    for linia in linies:
                                        linia=linia.strip()
                                        
                                        if not linia.startswith(category_namespaces[lang_code]) and not linia.startswith("|") and not linia.startswith("<") and not linia.startswith("!") and not linia.startswith("{")and len(linia)>0:
                                            sortida.write(linia+"\n")
                                    sortida.close()

                                    processed_articles_set.add(page.title)
                                    with open(processed_articles_path, "a", encoding='utf-8') as log_file: # adding titles to processed articles file and set
                                        log_file.write(page.title + "\n")
                                    
                                    print(f"Processed articles: {len(processed_articles_set)}")
            
                                except:
                                    print("ERROR:",sys.exc_info())
                                    print(f"Category namespace for {lang_name} is missing.")
                            # print("Page processed!")
                            pages_processed += 1
                            print(f"Processed {pages_processed} out of {len(usertitles_set)}")

    # return pagesdirpath # this return is doing nothing