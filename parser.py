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


def df_from_xml(xml_path: str, doc_id: bool, date: bool, source: bool, pages: bool, region: bool, mediatype: bool,
                length: bool, ressorts: bool, mutation: bool, keys: bool, title: bool, content: bool) -> pd.DataFrame:
    # creates a pd.DataFrame from an amc xml

    tree = etree.parse(xml_path)
    root = tree.getroot()

    data = []
    for doc in root.findall("doc"):  # finding all articles

        if doc_id:
            data.append({"doc id": doc.get("doc id")})  # getting doc id

        if date:
            data.append({"Date": pd.to_datetime(doc.get("datum"))})

        if source:
            data.append({"Source": doc.get("docsrc_name")})

        if pages:
            start_page, end_page = get_pages(doc.get("bibl"))  # getting page numbers
            data.append({"Start Page": start_page, "End Page": end_page})

        if region:
            data.append({"Region": doc.get("region")})

        if mediatype:
            data.append({"Mediatype": doc.get("mediatype")})

        if length:
            data.append({"Length": int(doc.get("tokens"))})

        if ressorts:
            data.append({"Ressorts": set(doc.get("ressort2").split(
                " "))})  # make set out of ressorts (so that intersections can be made efficiently)})

        if mutation:
            data.append({"Mutation": doc.get("mutation")})

        if keys:
            data.append({"Keys": doc.get("keys")})

        if title or content:
            for field in doc.findall("field"):  # selecting fields
                if title:
                    if field.get("name") == "titel":  # if title: add up paragraphs and make title
                        paragraphs = field.findall("p")
                        paragraphs = [p.text.strip() for p in paragraphs]
                        data.append({"Title": "\n".join(paragraphs)})
                elif field.get("name") == "inhalt":  # if content: add up paragraphs to create the full text
                    if content:
                        paragraphs = field.findall("p")
                        paragraphs = [p.text.strip() for p in paragraphs]
                        data.append({"Content": "\n".join(paragraphs)})

    return pd.DataFrame(data)  # create df from dict


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
