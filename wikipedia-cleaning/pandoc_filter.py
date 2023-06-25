import panflute as pf
import sys
import logging
import re


logger = logging.getLogger('custom_filter')
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def parse_citation(citation):
    citation_parameters = re.findall(
        r'\|([^=]+)=(.*?)(?=\||$)', citation, re.DOTALL)
    parameters = {k.strip(): v.strip()
                  for k, v in citation_parameters if k and v}
    return parameters


def generate_citation(parameters):
    citation = ''

    # Start with the author if available, otherwise skip
    if parameters.get('first', '') or parameters.get('last', ''):
        citation += parameters.get('last', '') + \
            ', ' + parameters.get('first', '') + '. '
    elif parameters.get('organization', ''):
        citation += parameters.get('organization', '') + '. '

    # Add title if available
    if 'title' in parameters:
        citation += parameters['title'] + ' '

    # Add year if available
    if 'year' in parameters:
        citation += '(' + parameters['year'] + '). '

    # If journal is available, it's a journal article
    if 'journal' in parameters:
        citation += 'In: ' + parameters['journal'] + '. '

    # If publisher is available, it's a book or chapter
    if 'publisher' in parameters:
        citation += parameters['publisher'] + '. '

    # Add volume and issue if available
    if 'volume' in parameters:
        citation += 'Vol. ' + parameters['volume']
        if 'issue' in parameters:
            citation += ', no. ' + parameters['issue'] + '. '
        else:
            citation += '. '

    # Add pages if available
    if 'pages' in parameters:
        citation += 'pp. ' + parameters['pages'] + '. '

    # Add URL if available
    if 'url' in parameters:
        citation += 'Available at: ' + parameters['url'] + '. '

    # Add access-date if available
    if 'access-date' in parameters:
        citation += 'Accessed: ' + parameters['access-date'] + '. '

    # Ensure citation ends with a period
    citation = citation.rstrip('. ')
    citation += '.'

    # Remove any unwanted characters
    citation = citation.replace('[', '')
    citation = citation.replace(']', '')
    citation = citation.replace('{', '')
    citation = citation.replace('}', '')

    return citation


def transform_citation(elem, doc):
    citation = elem.text
    parameters = parse_citation(citation)
    readable_text = generate_citation(parameters)
    return pf.Plain(pf.Str(readable_text))


def custom_filter(elem, doc):
    # Prepend a number of hashtags to headings depending on the heading level
    if isinstance(elem, pf.Header):
        hash_tags = '#' * elem.level
        elem.content = [
            pf.Str(f"{hash_tags} {pf.stringify(elem.content)}")]
        return elem
    # Handle citation blocks
    elif isinstance(elem, pf.RawBlock) and 'cite' in elem.text:
        return transform_citation(elem, doc)
    # Leave table elements as is
    elif isinstance(elem, pf.Table):
        return elem


def log_raw_inlines(elem, doc):
    if isinstance(elem, pf.RawInline):
        logger.debug(
            f"Unhandled RawInline Detected: {elem.text} ({elem.format})")
    elif isinstance(elem, pf.RawBlock):
        logger.debug(
            f"Unhandled RawBlock Detected: {elem.text} ({elem.format})")


def convert_raw_inline_to_text(elem, doc):
    if isinstance(elem, pf.RawInline):
        plain_text = elem.text.replace('_', '')
        return [pf.Str(plain_text)]


def convert_raw_block_to_text(elem, doc):
    if isinstance(elem, pf.RawBlock):
        plain_text = elem.text.replace('_', '')
        converted_blocks = pf.convert_text(plain_text)
        para = converted_blocks[0] if converted_blocks else None
        return para


def main(doc=None):
    return pf.run_filters([custom_filter,
                           #    log_raw_inlines,
                           #    convert_raw_block_to_text,
                           #    convert_raw_inline_to_text
                           ], doc=doc)


if __name__ == "__main__":
    main()
