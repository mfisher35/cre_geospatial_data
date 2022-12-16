from html.parser import HTMLParser

class DataHTMLParser(HTMLParser):
    #def handle_starttag(self, tag, attrs):
    #    print("Encountered a start tag:", tag)

    #def handle_endtag(self, tag):
    #    print("Encountered an end tag :", tag)

    def handle_data(self, data):
        print("Encountered some data  :", data)

parser = DataHTMLParser()
html = open("./html/52845.html",'r').read()
parser.feed(html)
