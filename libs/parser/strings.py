import re
import unicodedata


def remove_parentheses_in(string: str):
    return re.sub(r"\(.*?\)", "", string).strip()


def transform_to_db_name(string: str):
    string = re.sub(r"\([^)]*\)", "", string.lower())
    string = re.sub(r"\s*[,:\-/]\s*", "_", string)
    string = re.sub(r"\s+", "_", string)
    string = re.sub(r"(^_)|(_$)", "", string)
    return string


def remove_diacritics(text):
    nfkd_form = unicodedata.normalize("NFKD", text)
    ascii_string = nfkd_form.encode("ASCII", "ignore")
    return ascii_string.decode("utf-8")
