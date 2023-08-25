from lxml import etree
import re
import numpy as np
import pandas as pd


def get_pages(text: str):  # uses RegEx to get page numbers from doc in amc
    try:  # there won't be matches for online newspapers

        start_page_pattern = r"(?<=s\. )\d+"  # RegEx to match the starting page number(s)
        pages_pattern = r"((?<=s\. )(\d+)( \d+)*)"  # RegEx for the whole page expression - necessary because the
        # positive lookbehind would require a fixed length

        start_page = int(re.findall(start_page_pattern, text)[0])
        page_expr = re.findall(pages_pattern, text)[0][0]

        if " " in page_expr:  # -> more than 1 page
            pages = page_expr.split()
            start_page = pages[0]
            end_page = pages[-1]

        else:
            end_page = start_page

    except IndexError:  # no page numbers
        start_page = np.nan
        end_page = np.nan

    return start_page, end_page


def df_from_xml(xml_path: str, doc_id: bool=False, date: bool=True, source: bool=True, pages: bool=True, region: bool=False, mediatype: bool=False,
                length: bool=False, ressorts: bool=True, mutation: bool=False, keys: bool=False, title: bool=True, content: bool=True) -> pd.DataFrame:
    # creates a pd.DataFrame from an amc xml

    tree = etree.parse(xml_path)
    root = tree.getroot()

    data = []
    for doc in root.findall("doc"):  # finding all articles

        doc_id_value = doc.get("id")  # getting doc id
        date_value = pd.to_datetime(doc.get("datum"))
        source_data = doc.get("docsrc_name")
        start_page, end_page = get_pages(doc.get("bibl"))  # getting page numbers
        region_data = doc.get("region")
        mediatype_data = doc.get("mediatype")
        length_value = int(doc.get("tokens"))
        ressort_data = set(doc.get("ressort2").split(" "))  # make set out of ressorts (so that intersections
        # can be made efficiently)})
        mutation_data = doc.get("mutation")
        key_value = doc.get("keys")
        for field in doc.findall("field"):  # selecting fields
            if field.get("name") == "titel":  # if title: add up paragraphs and make title
                paragraphs = field.findall("p")
                paragraphs = [p.text.strip() for p in paragraphs]
                title_data = "\n".join(paragraphs)
            elif field.get("name") == "inhalt":  # if content: add up paragraphs to create the full text
                paragraphs = field.findall("p")
                paragraphs = [p.text.strip() for p in paragraphs]
                content_data = "\n".join(paragraphs)
        try:
            data.append({"Doc ID": doc_id_value, "Date": date_value, "Source": source_data, "Start Page": start_page,
                     "End Page": end_page, "Region": region_data, "Mediatype": mediatype_data, "Length": length_value,
                     "Ressorts": ressort_data, "Mutation": mutation_data, "Keys": key_value,
                     "Title": title_data, "Content": content_data})
        except NameError:
            print("Missing Data, Article Skipped")
            continue
    df = pd.DataFrame(data)  # create df from dict

    # removing all unwanted columns - there would definitely be a less dirty way to implement this
    # - feel free to make suggestions!
    if not doc_id:
        df.drop("Doc ID", axis=1, inplace=True)
    if not date:
        df.drop("Date", axis=1, inplace=True)
    if not source:
        df.drop("Source", axis=1, inplace=True)
    if not pages:
        df.drop(["Start Page", "End Page"], axis=1, inplace=True)
    if not region:
        df.drop("Region", axis=1, inplace=True)
    if not mediatype:
        df.drop("Mediatype", axis=1, inplace=True)
    if not length:
        df.drop("Length", axis=1, inplace=True)
    if not ressorts:
        df.drop("Ressorts", axis=1, inplace=True)
    if not mutation:
        df.drop("Mutation", axis=1, inplace=True)
    if not keys:
        df.drop("Keys", axis=1, inplace=True)
    if not title:
        df.drop("Title", axis=1, inplace=True)
    if not content:
        df.drop("Content", axis=1, inplace=True)

    return df


def df_from_xmls(xml_paths: list[str], doc_id: bool = False, date: bool = True, source: bool = True, pages: bool = True,
                 region: bool = False, mediatype: bool = False, length: bool = False, ressorts: bool = True,
                 mutation: bool = False, keys: bool = False, title: bool = True, content: bool = True) -> pd.DataFrame:
    # creates a pd.DataFrame out of a list of amc xmls

    # it is probably more RAM efficient to concatenate the dfs one by one but so far there haven't been any issues
    return pd.concat([df_from_xml(path, doc_id=doc_id, date=date, source=source, pages=pages,
                                  region=region, mediatype=mediatype, length=length, ressorts=ressorts,
                                  mutation=mutation, keys=keys, title=title, content=content) for path in xml_paths])


def get_topics(df: pd.DataFrame):  # returns all unique topics from df
    topics = set()
    for topic in df["Ressorts"]:
        topics.update(topic)
    return sorted(list(topics))
