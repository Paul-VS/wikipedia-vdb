import xml.etree.ElementTree as ET
import pandas as pd
import pypandoc
import os


def parse_wiki_xml(file_path):
    # Parse the XML file
    tree = ET.parse(file_path)

    # Get the root of the XML document
    root = tree.getroot()

    pages_data = []

    # Iterate over each 'page' tag
    for page in root.iter('page'):
        page_data = {}

        # Get the title of the page
        title = page.find('title')
        if title is not None:
            page_data['title'] = title.text

        # Get the latest 'revision' of the page
        revision = page.find('revision')
        if revision is not None:
            # Get the timestamp of the revision
            timestamp = revision.find('timestamp')
            if timestamp is not None:
                page_data['timestamp'] = timestamp.text

            # Get the text of the revision
            text = revision.find('text')
            if text is not None:
                cleaned_text = text.text
                page_data['text'] = cleaned_text

        pages_data.append(page_data)

    return pages_data


# Parse the MediaWiki XML dump and load the data into a DataFrame
pages_data = parse_wiki_xml('test_data.xml')
df = pd.DataFrame(pages_data)

title = df.loc[1, 'title']
test_article = df.loc[1, 'text']


# Convert MediaWiki markup to plain text or markdown
plain_text = pypandoc.convert_text(
    test_article,
    # "markdown",
    "plain",
    format="mediawiki",
    filters=[os.path.join('.', 'pandoc_filter.py')],
)

with open("converted_article.md", "w") as file:
    file.write('# ' + title + '\n\n' + plain_text)
