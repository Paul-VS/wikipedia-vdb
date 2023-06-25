import json

input_file = "converted_article.md"


def extract_elements(markdown_text):
    elements = []
    lines = markdown_text.split('\n')
    current_heading = None
    current_paragraph = ''

    for line in lines:
        line = line.strip()
        if line.startswith('#'):
            if current_paragraph and current_heading is not None:
                paragraph = {
                    'text': current_paragraph,
                    'type': 'paragraph',
                    'level': current_heading['level'] + 1,
                    'word_count': len(current_paragraph.split())
                }
                current_heading['children'].append(paragraph)
                current_paragraph = ''

            level = line.count('#')
            text = line.strip('#').strip()
            heading = {
                'text': text,
                'type': 'heading',
                'level': level,
                'children': []
            }
            elements.append(heading)
            current_heading = heading
        elif line:
            current_paragraph += line + '\n'
        elif current_paragraph and current_heading is not None:
            paragraph = {
                # Remove trailing newline.
                'text': current_paragraph.rstrip('\n'),
                'type': 'paragraph',
                'level': current_heading['level'] + 1,
                'word_count': len(current_paragraph.split())
            }
            current_heading['children'].append(paragraph)
            current_paragraph = ''

    # Catch any remaining paragraph after the last heading.
    if current_paragraph and current_heading is not None:
        paragraph = {
            'text': current_paragraph.rstrip('\n'),  # Remove trailing newline.
            'type': 'paragraph',
            'level': current_heading['level'] + 1,
            'word_count': len(current_paragraph.split())
        }
        current_heading['children'].append(paragraph)

    return elements


def build_hierarchy(elements):
    root = {
        'text': 'Root',
        'level': 0,
        'children': []
    }
    stack = [root]

    for element in elements:
        while element['level'] <= stack[-1]['level']:
            stack.pop()

        stack[-1]['children'].append(element)
        stack.append(element)

    return root


def main():
    with open(input_file, 'r') as file:
        markdown_text = file.read()

    elements = extract_elements(markdown_text)
    hierarchy = build_hierarchy(elements)

    json_output = json.dumps(hierarchy, indent=4)
    with open("structured_markdown.json", "w") as file:
        file.write(json_output)


if __name__ == "__main__":
    main()
