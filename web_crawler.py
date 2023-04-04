import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import mysql.connector
import json
import re
import unicodedata
import psycopg2

def truncate_tables(connection, table_names):
    cursor = connection.cursor()
    for table_name in table_names:
        cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
    connection.commit()
    print("Tables truncated successfully.")

# Database connection
def connect_to_database(host, user, password, database):
    print("Connecting to the database...")
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    print("Connected to the database.")
    return connection


def insert_page_data(connection, seo_data, crawl_request_id):
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO crawl_pages (url, status_code, content_type, content_length, word_count, title, meta_description,
            meta_keywords, canonical, hreflang, protocol, hostname, pathname, url_queries, fragment, has_forms,
            has_social_tags, has_iframes, noindex, nofollow, headings, internal_links, external_links, images,
            has_structured_data, meta_tags, has_meta_noindex, has_meta_nofollow, text_on_page, all_meta_tags, h1,
            h2, h3, h4, pagination, crawl_request_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        seo_data["url"],
        seo_data["status_code"],
        seo_data["content_type"],
        seo_data["content_length"],
        seo_data["word_count"],
        seo_data["title"],
        seo_data["meta_description"],
        seo_data["meta_keywords"],
        seo_data["canonical"],
        seo_data["hreflang"], #json.dumps(seo_data["hreflang"]) if isinstance(seo_data["hreflang"], (list, dict)) else '',
        seo_data["protocol"],
        seo_data["hostname"],
        seo_data["pathname"],
        seo_data["url_queries"],
        seo_data["fragment"],
        seo_data["has_forms"],
        seo_data["has_social_tags"],
        seo_data["has_iframes"],
        seo_data["noindex"],
        seo_data["nofollow"],
        seo_data["headings"], #json.dumps(seo_data["headings"]) if isinstance(seo_data["headings"], (list, dict)) else '',
        seo_data["internal_links"],
        seo_data["external_links"], #json.dumps(seo_data["external_links"]) if isinstance(seo_data["external_links"], (list, dict)) else '',
        seo_data["images"], #json.dumps(seo_data["images"]) if isinstance(seo_data["images"], (list, dict)) else '',
        seo_data["has_structured_data"],
        #seo_data["structured_data"] if isinstance(seo_data["structured_data"], (list, dict)) else '',
        seo_data["meta_tags"], #json.dumps(seo_data["meta_tags"]) if isinstance(seo_data["meta_tags"], (list, dict)) else '',
        seo_data["has_meta_noindex"],
        seo_data["has_meta_nofollow"],
        seo_data["text_on_page"], #json.dumps(seo_data["text_on_page"]),
        seo_data["all_meta_tags"], #json.dumps(seo_data["all_meta_tags"]) if isinstance(seo_data["all_meta_tags"], (list, dict)) else '',
        seo_data["h1"], json.dumps(seo_data["h1"]),
        seo_data["h2"], #json.dumps(seo_data["h2"]),
        seo_data["h3"], #json.dumps(seo_data["h3"]),
        seo_data["h4"], #json.dumps(seo_data["h4"]),
        seo_data["pagination"], #json.dumps(seo_data["pagination"]) if isinstance(seo_data["pagination"], (list, dict)) else '',
        crawl_request_id
    )
    
    #values = tuple(seo_data.values())
    #values = tuple(seo_data[key] for key in seo_data)
    #values = tuple(json.dumps(value) if isinstance(value, dict) else value for value in seo_data.values())
    #values = tuple(json.dumps(value) if isinstance(value, (list, dict)) else value for value in seo_data.values())
    

    #print(values)
    
    cursor.execute(insert_query, values)
    connection.commit()
    print(f"Inserted SEO data for {seo_data['url']} into the database.")
    
def insert_external_links(connection, page_id, external_links):
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO crawl_external_links (page_id, link)
    VALUES (%s, %s)
    """
    for link in external_links:
        cursor.execute(insert_query, (page_id, link))
    connection.commit()

def insert_pagination(connection, page_id, pagination_data):
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO crawl_pagination (page_id, rel_next, rel_prev, self_canonical)
    VALUES (%s, %s, %s, %s)
    """
    pagination_str = json.dumps(pagination_data)
    cursor.execute(insert_query, (page_id, pagination_str, pagination_str, pagination_str))
    connection.commit()

def insert_structured_data(connection, page_id, structured_data):
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO crawl_structured_data (page_id, data)
    VALUES (%s, %s)
    """
    #for json_ld in structured_data:
    cursor.execute(insert_query, (page_id, structured_data))
    connection.commit()

def insert_images(connection, page_id, images):
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO crawl_images (page_id, src, alt, title)
    VALUES (%s, %s, %s, %s)
    """
    for image in images:
        cursor.execute(insert_query, (page_id, image["src"], image["alt"], image["title"]))
    connection.commit()

def insert_internal_links(connection, page_id, internal_links):
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO crawl_internal_links (page_id, link)
    VALUES (%s, %s)
    """
    for link in internal_links:
        cursor.execute(insert_query, (page_id, link))
    connection.commit()

def create_crawl_request(connection, start_url):
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO crawl_requests (start_url, status)
    VALUES (%s, %s)
    """
    values = (start_url, 'pending')
    cursor.execute(insert_query, values)
    connection.commit()
    crawl_id = cursor.lastrowid
    return crawl_id

def update_crawl_request(connection, crawl_id, end_url, status):
    cursor = connection.cursor()
    update_query = """
    UPDATE crawl_requests
    SET end_url = %s, status = %s
    WHERE crawl_id = %s
    """
    values = (end_url, status, crawl_id)
    cursor.execute(update_query, values)
    connection.commit()
    
def extract_seo_data(url, crawl_request_id):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        error_msg = f"Error while requesting {url}: {e}"
        print(error_msg)
        # Insert the error message into the database
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO crawl_errors (crawl_request_id, url, error_msg)
        VALUES (%s, %s, %s)
        """
        values = (crawl_request_id, url, error_msg)
        cursor.execute(insert_query, values)
        connection.commit()
        return None

    status_code = response.status_code
    if status_code != 200:
        error_msg = f"Failed to retrieve {url}: {status_code}"
        print(error_msg)
        # Insert the error message into the database
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO crawl_errors (crawl_request_id, url, error_msg)
        VALUES (%s, %s, %s)
        """
        values = (crawl_request_id, url, error_msg)
        cursor.execute(insert_query, values)
        connection.commit()
        return None

    soup = BeautifulSoup(response.content, "html.parser")

    content_type = response.headers.get("Content-Type")
    content_length = response.headers.get("Content-Length")

    words_on_page = len(soup.get_text().split())
    
    # Meta noindex and nofollow
    meta_robots = soup.find("meta", attrs={"name": "robots"})
    has_meta_noindex = False
    has_meta_nofollow = False
    if meta_robots and meta_robots["content"]:
        meta_robots_content = meta_robots["content"].lower()
        has_meta_noindex = "noindex" in meta_robots_content
        has_meta_nofollow = "nofollow" in meta_robots_content

    
     # Word count & text
    text_on_page = soup.get_text()
    text_on_page = "dette er teksten"
    word_count = len(text_on_page.split())
    
    # Title tag
    title = str(soup.title.string) if soup.title else None

    # Meta tags
    meta_tags = {tag["name"]: tag["content"] for tag in soup.find_all("meta", attrs={"name": True, "content": True})}
    meta_description = meta_tags.get("description")
    meta_keywords = meta_tags.get("keywords")

    # Canonical and hreflang
    canonical = soup.find("link", rel="canonical")
    canonical = canonical["href"] if canonical else None
    hreflang = {link["hreflang"]: link["href"] for link in soup.find_all("link", rel="alternate", hreflang=True)}

    # URL information
    parsed_url = urlparse(url)
    protocol = parsed_url.scheme
    hostname = parsed_url.hostname
    pathname = parsed_url.path
    url_queries = parsed_url.query
    fragment = parsed_url.fragment

    # Forms, iframes, and social tags
    has_forms = bool(soup.find_all("form"))
    has_iframes = bool(soup.find_all("iframe"))
    has_social_tags = any(tag in soup for tag in ["og:", "twitter:"])

    # Meta robots
    meta_robots = soup.find("meta", attrs={"name": "robots"})
    meta_robots = meta_robots["content"] if meta_robots else None
    noindex = "noindex" in meta_robots if meta_robots else False
    nofollow = "nofollow" in meta_robots if meta_robots else False
    
    # All meta tags
    all_meta_tags = []
    for meta in soup.find_all("meta"):
        meta_attrs = {}
        for attr in meta.attrs:
            meta_attrs[attr] = meta[attr]
        all_meta_tags.append(meta_attrs)
        
    # Headings
    headings = {}
    for i in range(1, 7):
        tag = f"h{i}"
        headings[tag] = [h.text for h in soup.find_all(tag)]

    # Internal and external links
    internal_links = set()
    external_links = set()
    for a in soup.find_all("a", href=True):
        link = urljoin(url, a["href"])
        #print(f'clean link: {link}')
        if not urlparse(link).fragment:  # Ignore anchor links
            if link.startswith(url):
                #print(f'added link: {link}')
                internal_links.add(link)
            else:
                external_links.add(link)
                
                #internal_loads = json.dumps(list(internal_links))
                #print(f'dumped: {internal_loads}')
    
    # H1-H4 tags
    h1 = [h.text for h in soup.find_all("h1")]
    h2 = [h.text for h in soup.find_all("h2")]
    h3 = [h.text for h in soup.find_all("h3")]
    h4 = [h.text for h in soup.find_all("h4")]
    
    # Pagination
    pagination = []
    for link in soup.find_all("link", attrs={"rel": ["next", "prev"]}):
        rel = link.get("rel")
        href = link.get("href")
        pagination.append({"rel": rel, "href": href})
    
    # Image tags
    images = [{"src": img["src"], "alt": img.get("alt"), "title": img.get("title")} for img in soup.find_all("img", src=True)]

    # Structured data
    has_structured_data = any(script for script in soup.find_all("script", attrs={"type": True}) if script["type"] in ["application/ld+json", "application/vnd.geo+json", "application/vnd.ms-excel", "application/microdata+json"])
    
    structured_data = {}

    # JSON-LD
    json_ld = soup.find('script', {'type': 'application/ld+json'})
    if json_ld:
        structured_data['json_ld'] = json_ld.string

    # Microdata
    microdata = soup.find_all(True, {'itemscope': True})
    if microdata:
        structured_data['microdata'] = []
        for item in microdata:
            structured_data['microdata'].append(str(item))

    # RDFa
    rdfa = soup.find_all(True, {'typeof': True})
    if rdfa:
        structured_data['rdfa'] = []
        for item in rdfa:
            structured_data['rdfa'].append(str(item))
 

    seo_data = {
        "url": url,
        "status_code": status_code,
        "content_type": content_type,
        "content_length": content_length,
        "word_count": word_count,
        "title": title,
        "meta_description": meta_description,
        "meta_keywords": meta_keywords,
        "canonical": canonical,
        "hreflang": hreflang, #json.dumps(hreflang) if hreflang else '',
        "protocol": protocol,
        "hostname": hostname,
        "pathname": pathname,
        "url_queries": url_queries,
        "fragment": fragment,
        "has_forms": has_forms,
        "has_social_tags": has_social_tags,
        "has_iframes": has_iframes,
        "noindex": noindex,
        "nofollow": nofollow,
        "headings": headings, #json.dumps(headings) if isinstance(headings, (list, dict)) else '',
        "internal_links": internal_links, # json.dumps(list(internal_links)) if isinstance(internal_links, (set, list, dict)) else internal_links,
        "external_links": external_links, #json.dumps(list(external_links)) if isinstance(external_links, (set, list, dict)) else external_links,
        "images": images, #json.dumps(images) if isinstance(images, (list, dict)) else '',
        "has_structured_data": has_structured_data,
        "structured_data": structured_data, #json.dumps(structured_data) if isinstance(structured_data, (list, dict)) else '',
        "meta_tags": meta_tags, #json.dumps(meta_tags) if isinstance(meta_tags, (list, dict)) else '',
        "has_meta_noindex": has_meta_noindex,
        "has_meta_nofollow": has_meta_nofollow,
        "text_on_page": text_on_page,
        "all_meta_tags": all_meta_tags, #json.dumps(all_meta_tags) if isinstance(all_meta_tags, (list, dict)) else '',
        "h1": h1,
        "h2": h2,
        "h3": h3,
        "h4": h4,
        "pagination" : pagination, #json.dumps(pagination) if isinstance(pagination, (list, dict)) else '',
        "crawl_request_id": crawl_request_id
    }
    #print(f"Internal links found in {url}:", internal_links)

    return seo_data


def normalize_url(base_url, url):
    # Join the base_url with the url to handle relative URLs
    absolute_url = urljoin(base_url, url)
    parsed_url = urlparse(absolute_url)
    
    # Remove query strings and fragments from the URL
    normalized_url = parsed_url.scheme + "://" + parsed_url.hostname + parsed_url.path.rstrip("/")
    return normalized_url


def crawl_website(start_url, crawl_request_id, max_pages=100):
    visited_urls = {}
    queue = [start_url]
    start_url_normalized = start_url

    while queue and len(visited_urls) < max_pages:
        current_url = queue.pop(0)
        if current_url not in visited_urls:
            print(f"Crawling {current_url}")
            seo_data = extract_seo_data(current_url, crawl_request_id)
            if seo_data:
                visited_urls[current_url] = True
                for link in seo_data["internal_links"]:
                    print(f'internal link: {link}')
                    if link not in visited_urls and link not in queue and link != start_url_normalized:
                        queue.append(link)

                #print(f"Queue: {queue}")

                # Insert the main SEO data and get the page_id
                insert_page_data(connection, seo_data, crawl_request_id)
                page_id = connection.cursor().lastrowid

                # Insert the additional data using the new functions and set the page_id

                # Insert external links
                insert_external_links(connection, page_id, seo_data["external_links"])

                # Insert pagination data
                insert_pagination(connection, page_id, seo_data["pagination"])

                # Insert structured data
                if seo_data["has_structured_data"]:
                    insert_structured_data(connection, page_id, seo_data["structured_data"])

                # Insert images
                # insert_images(connection, page_id, seo_data["images"])

                # Insert internal links
                insert_internal_links(connection, page_id, seo_data["internal_links"])

                # Set the page_id in the visited_urls dictionary
                visited_urls[current_url] = page_id

    return list(visited_urls.keys())




if __name__ == "__main__":
    # Connect to the database
    connection = connect_to_database("localhost", "root", "", "laravel")

    
    start_url = "https://krister-ross.no/featured/hvordan-kan-google-analytics-vaere-ulovlig/#Hva_med_Google_Analytics_4"
    max_pages = 10

    # Create a crawl request
    crawl_id = create_crawl_request(connection, start_url)
    print(f"Created crawl request with ID {crawl_id}")

    # Update the crawl request status to 'running'
    update_crawl_request(connection, crawl_id, None, 'running')

    try:
        # Crawl the website
        visited_urls = crawl_website(start_url, crawl_id, max_pages)

        # Print the visited URLs
        print(f"Visited URLs ({len(visited_urls)}):")
        for url in visited_urls:
            print(url)

        # Update the crawl request status to 'completed' and set the end_url
        end_url = visited_urls[-1] if visited_urls else None
        update_crawl_request(connection, crawl_id, end_url, 'completed')
    except Exception as e:
        print(f"Error during crawling: {e}")
        update_crawl_request(connection, crawl_id, None, 'failed')

    # Close the database connection
    connection.close()


    