# segment functions
from typing import (List, Set, Tuple, Dict, Optional)
from utils.get_language import get_language

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
    import lxml

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
    import sys
    import os
    import subprocess
    from pathlib import Path
    # accessing args
    srxfile=args.srxfile
    if not srxfile:
        srxfile = 'segment.srx'
    indir=args.indir
    

    indir_path_name = Path(indir).name
    ending = indir_path_name[-2:]

    srxlang_name, srxlang_code = get_language(ending)

    paramark=args.paramark

    outdir=args.outdir

    # srxlang = srxlang.title() # capitalizing the first letter since that's how it's written in the srx file

    rules = parse(srxfile)
    languages = list(rules.keys())
    if not srxlang_name in languages:
        print("Language ",srxlang_name," not available in ", srxfile)
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
                    segments = segmenta(linia, srxfile, srxlang_name)
                    if len(segments) > 0:
                        if paramark:
                            sortida.write("<p>\n")
                        sortida.write(segments + "\n")

                entrada.close()
                sortida.close()

                cmd = f'cat {outdir}* | sort | uniq | shuf > unique-segments.txt' 
                
                subprocess.run(
                    cmd,
                    shell=True,
                    check=True
                )
                