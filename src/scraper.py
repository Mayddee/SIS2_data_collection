import json
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE_URL = "https://sxodim.com/almaty/"
OUTPUT_FILE = Path("data/raw_events.json")


def create_driver():
    """Create a Chrome WebDriver instance."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    return driver


def wait_for_list_loaded(driver, timeout: int = 20):
    """Wait until the event list container and at least one card appear."""
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.impression-items"))
    )
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.impression-card"))
    )


def click_load_more_until(driver, min_cards: int = 200, max_clicks: int = 40):
    """
    Click the 'Load More' button until:
    - We collect at least min_cards,
    - OR we reach max_clicks,
    - OR the button disappears or stops being clickable.
    """
    clicks = 0

    while clicks < max_clicks:
        cards = driver.find_elements(By.CSS_SELECTOR, "div.impression-card")
        current_count = len(cards)
        print(f"[scroll] cards now: {current_count}")

        if current_count >= min_cards:
            print(f"[scroll] reached target: {current_count} cards")
            break

        # Try to find the 'Load More' button
        try:
            load_more_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.CSS_SELECTOR,
                        "div.impression-actions "
                        "button.impression-btn-secondary[data-action='loadMore']",
                    )
                )
            )
        except Exception:
            print("[scroll] Load More button not found or not clickable, exiting")
            break

        # Scroll to the button and click it using JavaScript, this is more reliable
        driver.execute_script("arguments[0].scrollIntoView(true);", load_more_btn)
        time.sleep(0.7)
        driver.execute_script("arguments[0].click();", load_more_btn)
        clicks += 1
        print(f"[scroll] clicked loadMore {clicks} time(s)")

        # Wait until the card count increases
        try:
            WebDriverWait(driver, 10).until(
                lambda d: len(
                    d.find_elements(By.CSS_SELECTOR, "div.impression-card")
                )
                > current_count
            )
        except Exception:
            print("[scroll] card count did not grow after clicking, exiting")
            break

        time.sleep(1.0)  # Give the page time to render more cards


def parse_list_cards(driver, limit: int | None = None):
    """Extract all event cards from the list page."""
    cards = driver.find_elements(By.CSS_SELECTOR, "div.impression-card")
    events: list[dict] = []

    for card in cards:
        try:
            link_el = card.find_element(By.CSS_SELECTOR, "a.impression-card-title")
            info_el = card.find_element(By.CSS_SELECTOR, "div.impression-card-info")
        except Exception:
            # Skip cards that have a different structure
            continue

        # Image element (may not exist)
        try:
            img_el = card.find_element(By.CSS_SELECTOR, "picture img")
            image_url = img_el.get_attribute("src")
        except Exception:
            image_url = None

        event = {
            "id": card.get_attribute("data-id"),
            "title": link_el.text.strip(),
            "url": link_el.get_attribute("href"),
            "category": card.get_attribute("data-category"),
            "min_price": card.get_attribute("data-minprice"),
            "partner": card.get_attribute("data-partner"),
            "short_info": info_el.text.strip(),
            "image_url": image_url,
            "views": None,
            "date": None,
            "time": None,
            "address": None,
            "price_full": None,
            "description": None,
            "publication_date": None,
            "tags": [],
        }
        events.append(event)

        if limit is not None and len(events) >= limit:
            break

    print(f"[list] collected {len(events)} events from list page")
    return events


def parse_detail_page(driver, event: dict):
    """Open the event detail page and enrich the event dictionary."""
    url = event.get("url")
    if not url:
        return

    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "main.impression-main, main#content")
            )
        )
    except Exception:
        print(f"[detail] cannot open {url}")
        return

    # Views (eye icon + number), extract digits
    try:
        views_el = driver.find_element(By.CSS_SELECTOR, "div.views")
        views_text = views_el.text.strip()
        digits = "".join(ch for ch in views_text if ch.isdigit())
        event["views"] = int(digits) if digits else None
    except Exception:
        pass

    # Date, address, time, price
    try:
        try:
            date_el = driver.find_element(
                By.CSS_SELECTOR, "div.info.date_picker_header div.date"
            )
            event["date"] = date_el.text.strip()
        except Exception:
            try:
                block = driver.find_element(By.CSS_SELECTOR, "div.event_date_block")
                raw_date = block.get_attribute("data-date")
                event["date"] = raw_date
            except Exception:
                pass

        event_block = driver.find_element(By.CSS_SELECTOR, "div.event_date_block")
        groups = event_block.find_elements(By.CSS_SELECTOR, "div.group")

        if len(groups) >= 1:
            try:
                event["address"] = groups[0].find_element(
                    By.CSS_SELECTOR, "div.text"
                ).text.strip()
            except Exception:
                pass

        if len(groups) >= 2:
            try:
                event["time"] = groups[1].find_element(
                    By.CSS_SELECTOR, "div.text"
                ).text.strip()
            except Exception:
                pass

        if len(groups) >= 3:
            try:
                event["price_full"] = groups[2].find_element(
                    By.CSS_SELECTOR, "div.str.bold"
                ).text.strip()
            except Exception:
                pass
    except Exception:
        pass

    # Description (left side text column)
    try:
        content_el = driver.find_element(By.CSS_SELECTOR, "div.content_wrapper")
        event["description"] = content_el.text.strip()
    except Exception:
        pass

    # Publication date
    try:
        pub_el = driver.find_element(By.CSS_SELECTOR, "div.publication")
        pub_text = pub_el.text.strip()
        if ":" in pub_text:
            event["publication_date"] = pub_text.split(":", 1)[1].strip()
        else:
            event["publication_date"] = pub_text
    except Exception:
        pass

    # Tags like 'concert', 'events Almaty', etc.
    try:
        tag_els = driver.find_elements(By.CSS_SELECTOR, "div.tags a.tag")
        event["tags"] = [t.text.strip() for t in tag_els if t.text.strip()]
    except Exception:
        pass


def save_events(events: list[dict], path: Path):
    """Save parsed events to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)
    print(f"[save] saved {len(events)} events to {path}")


def run_scraper(min_events: int = 200):
    """
    Main scraping function - called by Airflow.
    Returns True on success, raises exception on failure.
    """
    print("[scraper] starting scraping process")
    driver = create_driver()
    
    try:
        print("[scraper] opening list page")
        driver.get(BASE_URL)
        wait_for_list_loaded(driver)
        click_load_more_until(driver, min_cards=min_events, max_clicks=40)

        events = parse_list_cards(driver, limit=min_events)
        
        if not events:
            raise ValueError("No events collected from list page")

        print(f"[scraper] processing {len(events)} event details")
        for idx, ev in enumerate(events, start=1):
            print(f"[scraper] {idx}/{len(events)}: {ev['title']}")
            parse_detail_page(driver, ev)
            time.sleep(1)  # Delay to avoid spamming the website

        save_events(events, OUTPUT_FILE)
        print(f"[scraper] successfully scraped {len(events)} events")
        return True

    except Exception as e:
        print(f"[scraper] ERROR: {e}")
        raise
    finally:
        driver.quit()
        print("[scraper] browser closed")


def main():
    """Execute scraper manually for testing."""
    run_scraper()


if __name__ == "__main__":
    main()
