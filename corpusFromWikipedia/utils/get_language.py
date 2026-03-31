def get_language(language):
    import pycountry
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