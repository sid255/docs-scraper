import json
import requests
from bs4 import BeautifulSoup

def generate_config(url):
    config = {
        "index_uid": "pytorch",
        "index_name": "pytorch",
        "app_id": "http://localhost:7700/",
        "api_key": "MASTER_KEY",
        "start_urls": [
            {
                "url": url,
                "tags": ["master"],
                "selectors_key": "code"
            }
        ],
        "stop_urls": [
            "/_",
            "\\?"
        ],
        "selectors": {
            "code": {
                "lvl0": {
                "selector": ".jumbotron h1",
                "global": True,
                "default_value": "Documentation"
              }
            }
        }
    }

    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract headings
        headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        heading_selectors={}
        heading_done=set()
        for level, heading in enumerate(headings):
            if heading.name in heading_done:
                continue
            selector_name = f"lvl{heading.name[-1]}"
            selector = get_selector(heading)
            heading_selectors[selector_name] = selector
            heading_done.add(heading.name)
        
            
        config["selectors"]["code"].update(heading_selectors)

        # Extract text content
        paragraphs = soup.find_all(['p'])
        text_selectors = get_selectors(paragraphs)
        config["selectors"]["code"]["text"] = ", ".join(text_selectors)

        # Generate the code selector
        pre_all = soup.find_all(['pre'])
        code_selectors = get_selectors(pre_all)

        config["selectors"]["code"]["code"] = ", ".join(code_selectors)

        return config

    else:
        print("Failed to retrieve website content.")
        return None

def get_selectors(elements):

    if not elements:
        return
    selectors = {}
    
    for element in elements:
        selector = get_selector(element)
        selectors[selector]= selectors.get(selector, 0) +1
    
    selectors = sorted(selectors.items(), key=lambda item: item[1], reverse=True)
    # returning 2 max frequency selectors
    if len(selectors) > 1:
        selectors= [selectors[0][0], selectors[1][0]]
    elif len(selectors) == 1:
        selectors=[selectors[0][0]]
    else:
        selectors = [elements[0].name]
    return selectors

def get_selector(element):
    parent = element.parent
    selector = element.name
    if parent:
        selector = parent.name + ' ' + selector
    return selector

def save_config(config, filename):
    with open(filename, 'w') as f:
        json.dump(config, f, indent=4)
    print(f"Configuration file '{filename}' generated successfully.")



def __main__():

    url = input("Enter the website URL: ")
    config = generate_config(url)
    if config:
        filename = input("Enter the filename for the configuration file: ")
        save_config(config, filename)
