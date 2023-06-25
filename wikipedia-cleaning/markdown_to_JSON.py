import json

input_file = "converted_article.md"


def extract_elements(markdown_text):
    """
    Takes markdown text and returns a list of 'elements' which are either headings or paragraphs.
    """
    elements = []
    lines = markdown_text.split('\n')  # Split the markdown text by newlines
    current_heading = None
    current_paragraph = ''

    for line in lines:  # Process each line
        line = line.strip()
        if line.startswith('#'):  # If line is a heading
            # Store the current paragraph as a child of the current heading
            if current_paragraph and current_heading is not None:
                paragraph = {
                    'text': current_paragraph,
                    'type': 'paragraph',
                    'level': current_heading['level'] + 1,
                    'word_count': len(current_paragraph.split())
                }
                current_heading['children'].append(paragraph)
                current_paragraph = ''

            level = line.count('#')  # Determine the heading level
            text = line.strip('#').strip()
            heading = {
                'text': text,
                'type': 'heading',
                'level': level,
                'children': []
            }
            elements.append(heading)  # Add the heading to the list of elements
            current_heading = heading
        elif line:  # If line is part of a paragraph
            current_paragraph += line + '\n'
        # If line is empty and a paragraph has been started
        elif current_paragraph and current_heading is not None:
            # Store the current paragraph as a child of the current heading
            paragraph = {
                'text': current_paragraph.rstrip('\n'),
                'type': 'paragraph',
                'level': current_heading['level'] + 1,
                'word_count': len(current_paragraph.split())
            }
            current_heading['children'].append(paragraph)
            current_paragraph = ''

    # Append the last paragraph if it hasn't been appended yet
    if current_paragraph and current_heading is not None:
        paragraph = {
            'text': current_paragraph.rstrip('\n'),
            'type': 'paragraph',
            'level': current_heading['level'] + 1,
            'word_count': len(current_paragraph.split())
        }
        current_heading['children'].append(paragraph)

    return elements


def build_hierarchy(elements):
    """
    Takes a list of elements and builds a hierarchical structure of the elements.
    """
    root = {
        'text': 'Root',
        'level': 0,
        'children': []
    }
    stack = [root]

    for element in elements:  # Process each element
        while element['level'] <= stack[-1]['level']:
            stack.pop()  # Pop elements from stack until finding the parent of the current element

        # Add the current element as a child of the top element in the stack
        stack[-1]['children'].append(element)
        stack.append(element)  # Push the current element onto the stack

    return root


def main():
    """
    The main function of the script. Reads a markdown file, extracts the elements, builds the hierarchical structure, and writes the result as a JSON document.
    """
    with open(input_file, 'r') as file:
        markdown_text = file.read()  # Read the markdown text from the file

    # Extract elements from the markdown text
    elements = extract_elements(markdown_text)
    # Build the hierarchy from the elements
    hierarchy = build_hierarchy(elements)

    # Convert the hierarchy to a JSON string
    json_output = json.dumps(hierarchy, indent=4)
    with open("structured_markdown.json", "w") as file:
        file.write(json_output)  # Write the JSON string to a file


if __name__ == "__main__":
    main()
