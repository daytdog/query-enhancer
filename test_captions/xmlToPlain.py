#!/user/bin/python
"""
xmlToPlain.py
"""

from xml.sax.saxutils import unescape

with open('test_output.txt', 'r') as myfile:
    data=myfile.read().replace('\n', ' ').replace('.', '').replace('!', '').replace('?', '')

data = unescape(data, {"&#39;": "'", "&quot;": '"'})

f = open("parsed_test_output.txt", "w")
f.write(data)
myfile.close()
f.close()
