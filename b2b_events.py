import asyncio
import aiohttp
import csv
import logging
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# List of event URLs
event_urls = [
    'https://www.salesforce.com/dreamforce/',
    'https://www.saastrannual.com/',
    'https://websummit.com/',
    'https://www.b2bmarketingexpo.co.uk/',
    'https://www.ces.tech/'
]

async def fetch(url, session):
    try:
        async with session.get(url) as response:
            return await response.text()
    except aiohttp.ClientError as e:
        logger.error(f"Error fetching {url}: {e}")
        return None

async def scrape_event_data(url, session):
    try:
        html = await fetch(url, session)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        event_name = soup.title.text.strip() if soup.title else 'N/A'
        event_date = soup.find('meta', {'name': 'date'})['content'].strip() if soup.find('meta', {'name': 'date'}) else 'N/A'
        location = soup.find('meta', {'name': 'location'})['content'].strip() if soup.find('meta', {'name': 'location'}) else 'N/A'
        description = soup.find('meta', {'name': 'description'})['content'].strip() if soup.find('meta', {'name': 'description'}) else 'N/A'
        key_speakers = [speaker.get_text(strip=True) for speaker in soup.select('.speaker-name')]
        agenda = [item.get_text(strip=True) for item in soup.select('.agenda-item')]
        registration_details = soup.find('a', class_='registration-link')['href'].strip() if soup.find('a', class_='registration-link') else 'N/A'
        pricing = soup.find('div', class_='pricing').get_text(strip=True) if soup.find('div', class_='pricing') else 'N/A'
        categories = [category.get_text(strip=True) for category in soup.select('.category')]
        audience_type = soup.find('meta', {'name': 'audience'})['content'].strip() if soup.find('meta', {'name': 'audience'}) else 'N/A'

        return {
            'Event Name': event_name,
            'Event Date(s)': event_date,
            'Location': location,
            'Website URL': url,
            'Description': description,
            'Key Speakers': ', '.join(key_speakers),
            'Agenda/Schedule': ', '.join(agenda),
            'Registration Details': registration_details,
            'Pricing': pricing,
            'Categories': ', '.join(categories),
            'Audience Type': audience_type
        }
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None

async def scrape_all_events(event_urls):
    async with aiohttp.ClientSession() as session:
        tasks = [scrape_event_data(url, session) for url in event_urls]
        return await asyncio.gather(*tasks)

def write_to_csv(data, filename='b2b_events.csv'):
    fieldnames = ['Event Name', 'Event Date(s)', 'Location', 'Website URL', 'Description', 'Key Speakers', 'Agenda/Schedule', 'Registration Details', 'Pricing', 'Categories', 'Audience Type']
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for event in data:
                writer.writerow(event)
        logger.info(f"Data scraped and saved to {filename}")
    except IOError as e:
        logger.error(f"Error writing to {filename}: {e}")

async def main():
    events_data = await scrape_all_events(event_urls)
    clean_events_data = [event for event in events_data if event is not None]
    write_to_csv(clean_events_data)

if __name__ == "__main__":
    asyncio.run(main())
