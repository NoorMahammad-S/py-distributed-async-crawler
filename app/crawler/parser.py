from selectolax.parser import HTMLParser


class Parser:
    def parse(self, html: str):
        tree = HTMLParser(html)
        links = [n.attributes.get("href") for n in tree.css("a") if n.attributes.get("href")]
        text = tree.text()
        title_node = tree.css_first("title")
        title = title_node.text() if title_node else None

        return {
            "title": title,
            "text": text[:5000],
            "links": links
        }
