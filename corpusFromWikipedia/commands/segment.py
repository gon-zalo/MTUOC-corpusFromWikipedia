# segment functions
from typing import (List, Set, Tuple, Dict, Optional)
from ..utils.get_language import get_language

class SrxSegmenter:
    """Handle segmentation with SRX regex format.
    """
    def __init__(self, rule: Dict[str, List[Tuple[str, Optional[str]]]], source_text: str) -> None:
        self.source_text = source_text
        self.non_breaks = rule.get('non_breaks', [])
        self.breaks = rule.get('breaks', [])

    def _get_break_points(self, regexes: List[Tuple[str, str]]) -> Set[int]:
        import regex
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
    import lxml.etree

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

def sort_uniq_shuf(segments_folder, lang_code, outdir):
    import subprocess
    print("Saving unique segments file")
    cmd = f'find {segments_folder}/ -type f -exec cat {{}} + | sort | uniq | shuf > {outdir}/unique-segments-{lang_code.lower()}.txt' 
    subprocess.run(
        cmd,
        shell=True,
        check=True
    )
    print("Segmenting done. Unique segments file saved.")

def load_external_segmenter(lang_name, lang_code, external_segmenter):
    if external_segmenter.lower() == 'stanza':
        import stanza
        print(f"Running Stanza segmenter in {lang_name}")
        return stanza.Pipeline(lang=lang_code, processors='tokenize', use_gpu=False)
    
    elif external_segmenter.lower() == 'spacy':
        import spacy
        print(f"Running spaCy segmenter in {lang_name}")
        return spacy.load(lang_code)

def external_segmenter(nlp, text, external_segmenter):
    if external_segmenter.lower() == 'stanza':
        doc = nlp(text)
        return [sentence.text for sentence in doc.sentences]
    
    elif external_segmenter.lower() == 'spacy':
        doc = nlp(text)
        return [sent.text for sent in doc.sents]
    
def segment_corpus(args):
    import sys
    from pathlib import Path
    print("\nRunning segment command")

    # accessing args
    srxfile=args.srxfile
    rules = parse(srxfile)
    languages = list(rules.keys())
    force_srx_lang = args.force_srx_lang
    force_segmenter = args.force_segmenter

    indir = args.indir # should be the corpora folder inside outputs
    indir = Path(indir)

    paramark=args.paramark

    outdir = args.outdir or indir

    for folder in indir.iterdir(): # look for 'pages' folders inside the input directory

        if folder.is_dir() and folder.name.startswith("pages"):
            print(f"\nFolder {folder.name} found")

            ending = folder.name.split("-") # splitting folder name to get the language code
            ending = ending[-1]

            if force_srx_lang:
                print(f"Overriding SRX language configuration.")
                srxlang_name = force_srx_lang.capitalize()
                print(f"{srxlang_name} chosen")

            if not force_srx_lang and not force_segmenter: # if we want to use the srx file
                srxlang_name, srxlang_code = get_language(ending)
                if not srxlang_name in languages:
                    print("Language ",srxlang_name," not available in ", srxfile)
                    print("Available languages:",", ".join(languages))
                    sys.exit()

            if force_segmenter: # basically if else
                srxlang_name, srxlang_code = get_language(ending)

            print(f"Segmenting files in {srxlang_name}")

            if not force_srx_lang:
                segments_folder = indir / f'segments-{srxlang_code}'
                segments_folder.mkdir(parents=True, exist_ok=True)
            else:
                segments_folder = indir / f'segments-force-{srxlang_name.lower()}'
                segments_folder.mkdir(parents=True, exist_ok=True)

            if force_segmenter: # if using stanza or spacy
                nlp = load_external_segmenter(srxlang_name, srxlang_code, force_segmenter.lower())

            for text_file in folder.rglob("*.txt"): # accessing all txt files
                encoding = detect_encoding(text_file)
                # reading the file
                with open(text_file, "r", encoding=encoding, errors="ignore") as entrada:
                    
                    outfile = segments_folder / text_file.name
                    # writing segments in a new file of segmented sentences
                    with open(outfile, "w", encoding="utf-8") as sortida:
                        # process file here
                        for linia in entrada:
                            if force_segmenter:
                                segments = external_segmenter(nlp, linia, force_segmenter.lower())
                            else:
                                segments = segmenta(linia, srxfile, srxlang_name)

                            if len(segments) > 0:
                                if paramark:
                                    sortida.write("<p>\n")

                                if not force_segmenter:
                                    sortida.write(segments + "\n")
                                else:
                                    sortida.write("\n".join(segments) + "\n")

            if not force_srx_lang:
                sort_uniq_shuf(segments_folder, srxlang_code, outdir)

            else:
                srxlang_code = f'force-{srxlang_name}-' + srxlang_code
                sort_uniq_shuf(segments_folder, srxlang_code, outdir)