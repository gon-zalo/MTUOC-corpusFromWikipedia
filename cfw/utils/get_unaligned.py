import subprocess
from pathlib import Path
from get_language import get_language
import sys

corpus_folder = input("Corpus folder to get unaligned segments from: ")
corpus_folder = Path(corpus_folder).expanduser()
print(f"Chosen folder: {corpus_folder}")

corpus_folder_name = corpus_folder.name.split("-")
ending1 = corpus_folder_name[-2]
ending2 = corpus_folder_name[-1]

lang1_name, lang1_code = get_language(ending1)
lang2_name, lang2_code = get_language(ending2)

chosen_lang = input(f"Choose {lang1_name} ({lang1_code}) or {lang2_name} ({lang2_code}): ")

valid_choices = [lang1_name, lang1_code, lang2_name, lang2_code]

if chosen_lang not in valid_choices:
    print("Chosen language not valid.")
    sys.exit()
else:
    if chosen_lang == lang1_name or chosen_lang == lang1_code:
        column_num = str(1)
    else:
        column_num = str(2)
    chosen_lang_name, chosen_lang_code = get_language(chosen_lang)

print(f"Getting unaligned segments in {chosen_lang_name}. Position: {column_num}")

for file in corpus_folder.iterdir():
    # print(file)
    if file.name.startswith("unique-segments-") and file.name.endswith(f"{chosen_lang_code}.txt") and "sorted" not in file.name:
        unique_segments_lang = file
        print(f"{unique_segments_lang.name} found")

if not unique_segments_lang:
    print(f"Unique segments file not found")
    sys.exit()

aligned_lang_file = corpus_folder / f"aligned-{chosen_lang_code}.txt"
aligned_segments_lang1_lang2 = corpus_folder / f"aligned-segments-{lang1_code}-{lang2_code}.txt"
# unique_segments_lang = corpus_folder / f"unique-segments-{chosen_lang_code}.txt"
unique_segments_sorted = corpus_folder / f"unique-segments-sorted-{chosen_lang_code}.txt"

unaligned_file = corpus_folder / f"unaligned-{chosen_lang_code}.txt"

cut_aligned_segments = subprocess.run(
    f'cut -f{column_num} {aligned_segments_lang1_lang2} | sort > {aligned_lang_file}', 
    shell=True, 
    text=True, 
    capture_output=True)

sort_unique = subprocess.run(
    f'sort {unique_segments_lang} > {unique_segments_sorted}',
    shell=True,
    text=True,
    capture_output=True)

extract_unaligned = subprocess.run(
    f'comm -23 {unique_segments_sorted} {aligned_lang_file} > {unaligned_file}',
    shell=True,
    text=True,
    capture_output=True)

print(f"Unaligned segments extracted. Saved in: {unaligned_file}")
print()

for file in [unaligned_file, aligned_lang_file, unique_segments_lang]:

    wc_l = subprocess.run(
        ["wc", "-l", file],
        capture_output=True,
        text=True,
        check=True,
    )

    line_count = wc_l.stdout.split()[0]
    print(f"Number of lines in {file.name}: {line_count}")
    

# removing temp files
aligned_lang_file.unlink()
unique_segments_sorted.unlink()