import panflute as pf
import sys
import logging


logger = logging.getLogger('custom_filter')
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def headings_and_tables_filter(elem, doc):
    # Prepend a number of hashtags to headings depending on the heading level
    if isinstance(elem, pf.Header):
        hash_tags = '#' * elem.level
        elem.content = [
            pf.Str(f"{hash_tags} {pf.stringify(elem.content)}")]
        return elem
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
    return pf.run_filters([headings_and_tables_filter,
                           #    log_raw_inlines,
                           #    convert_raw_block_to_text,
                           #    convert_raw_inline_to_text
                           ], doc=doc)


if __name__ == "__main__":
    main()
