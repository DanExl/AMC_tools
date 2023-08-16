from lxml import etree
import re
import numpy as np
import pandas as pd


def get_pages(text:str):
  try:    # there won't be matches for online newspapers

    start_page_pattern = r"(?<=s\. )\d+" # RegEx to match the starting page number(s)
    pages_pattern = r"((?<=s\. )(\d+)( \d+)*)" # RegEx for the whole page expression, necessary because the positive lookbehind would require a fixed length

    start_page = int(re.findall(start_page_pattern, text)[0])
    page_expr = re.findall(pages_pattern, text)[0][0]

    if " " in page_expr: # -> more than 1 page
      pages = page_expr.split()
      start_page = pages[0]
      end_page = pages[-1]

    else:
      end_page = start_page

  except IndexError:    # no page numbers
    start_page = np.nan
    end_page = np.nan

  return start_page, end_page


def df_from_xml(xml_path: str):
  tree = etree.parse(xml_path)
  root = tree.getroot()

  data = []
  for doc in root.findall("doc"):       # finding all articles
    source = doc.get("docsrc_name")       # getting source medium
    date = doc.get("datum")               # getting date
    start_page, end_page = get_pages(doc.get("bibl")) # getting page numbers
    for field in doc.findall("field"):  # selecting fields
      if field.get("name") == ("titel"):# if title: add up paragraphs and make title
        paragraphs = field.findall("p")
        paragraphs = [p.text.strip() for p in paragraphs]
        title = "\n".join(paragraphs)
      elif field.get("name") == ("inhalt"): # if content: add up paragraphs to create the full text
        paragraphs = field.findall("p")
        paragraphs = [p.text.strip() for p in paragraphs]
        content = "\n".join(paragraphs)
    ressorts = set(doc.get("ressort2").split(" ")) # make set out of ressorts (so that intersections can be made efficiently)

    data.append({"Source": source, "Date": date, "Start Page": start_page, "End Page": end_page, "Title": title, # create dict
                "Content": content, "Ressorts": ressorts})


  df = pd.DataFrame(data)           # create df from dict
  return df
