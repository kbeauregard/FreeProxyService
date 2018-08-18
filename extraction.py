import requests
from lxml import html

def extract_freeproxy_page():
    headers = {
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36')
    }
    resp = requests.get('https://free-proxy-list.net/', headers)
    proxies = []
    doc = html.fromstring(resp.content)
    ip_elements = doc.xpath('//tr')
    for element in ip_elements:
        data = element.xpath('td')
        if len(data) != 8:
            continue
        proxies += [{
            'ip': data[0].text_content(),
            'port': data[1].text_content(),
            'code': data[2].text_content(),
            'country': data[3].text_content(),
            'anonymity': data[4].text_content(),
            'google': data[5].text_content(),
            'https': data[6].text_content(),
            'last_checked': '',
        }]
    return proxies
