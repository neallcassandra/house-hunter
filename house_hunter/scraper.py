"""
Realtor API scraper with rate limiting.

Uses the Realtor API Data endpoint on RapidAPI. Caps at 40 API calls per run
to stay within the pro tier (10k calls/month, ~8 runs/day).

API: https://rapidapi.com/nusantaracodedotcom/api/realtor-api-data
"""

import json
import os
import random
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()


class RealtorAPIScraper:
    """Property scraper with 40-call per-run limit."""

    def __init__(self):
        self.rapidapi_key = os.getenv("RAPIDAPI_KEY")
        if not self.rapidapi_key:
            raise ValueError("RAPIDAPI_KEY not found in .env file")

        self.api_host = "realtor-api-data.p.rapidapi.com"
        self.headers = {
            "X-RapidAPI-Key": self.rapidapi_key,
            "X-RapidAPI-Host": self.api_host,
            "Content-Type": "application/json"
        }

        # Rate limiting: 40 calls/run keeps us under 10k/month
        self.MAX_CALLS = 40
        self.api_calls_made = 0
        self.delay_between_calls = 0.5

        # Search criteria
        self.min_price = int(os.getenv("HOUSE_HUNTER_MIN_PRICE", "200000"))
        self.max_price = int(os.getenv("HOUSE_HUNTER_MAX_PRICE", "350000"))
        self.min_baths = 1

        suburbs_str = os.getenv("HOUSE_HUNTER_SUBURBS", "Westlake")
        self.priority_cities = [city.strip() for city in suburbs_str.split(",")]

        self.state = os.getenv("HOUSE_HUNTER_STATE", "OH")

    def _make_api_call(self, method: str, url: str, **kwargs) -> dict[str, Any] | None:
        """Make API call with rate limiting. Returns None if limit reached."""
        if self.api_calls_made >= self.MAX_CALLS:
            print(f"\n⚠️  API CALL LIMIT REACHED ({self.MAX_CALLS} calls)")
            return None

        try:
            if self.api_calls_made > 0:
                time.sleep(self.delay_between_calls)

            if 'timeout' not in kwargs:
                kwargs['timeout'] = 30

            if method.upper() == "POST":
                response = requests.post(url, headers=self.headers, **kwargs)
            else:
                response = requests.get(url, headers=self.headers, **kwargs)

            self.api_calls_made += 1
            print(f"   [API Calls: {self.api_calls_made}/{self.MAX_CALLS}]", end=" ")

            if response.status_code == 200:
                return response.json()
            else:
                print(f"\n   ❌ Error: Status {response.status_code}")
                try:
                    error_body = response.json()
                    print(f"   ❌ Response: {error_body}")
                except Exception:
                    print(f"   ❌ Response: {response.text[:200]}")
                return None

        except Exception as e:
            self.api_calls_made += 1
            print(f"\n   ❌ Error: {str(e)}")
            return None

    def search_properties(self, max_details: int = 30) -> list[dict[str, Any]]:
        """
        Main search flow: list properties per city, filter by price, fetch
        details for survivors, check basement/pool criteria on the detail data.
        """
        print("\n" + "=" * 80)
        print("🏠 HOUSE HUNTER")
        print("=" * 80)
        print(f"\n📊 API LIMIT: {self.MAX_CALLS} calls per run")
        print(f"📍 Searching: {', '.join(self.priority_cities)}")
        print(f"💰 Price: ${self.min_price:,} - ${self.max_price:,}")

        all_properties = []

        # Step 1: Get property lists from priority cities
        print(f"\n{'=' * 80}")
        print("STEP 1: Fetching Property Lists")
        print(f"{'=' * 80}")

        for city in self.priority_cities:
            if self.api_calls_made >= self.MAX_CALLS:
                break

            print(f"\n🔍 Searching {city}, {self.state}...")

            properties = self._fetch_property_list(city)
            if properties:
                print(f"   ✅ Found {len(properties)} properties")
                all_properties.extend(properties)
            else:
                print("   ❌ No properties found")

        print(f"\n✅ Total properties from lists: {len(all_properties)}")

        # Step 2: Filter client-side
        print(f"\n{'=' * 80}")
        print("STEP 2: Client-Side Filtering")
        print(f"{'=' * 80}")

        filtered = self._filter_basic(all_properties)
        print(f"✅ After filtering: {len(filtered)} properties")

        random.shuffle(filtered)
        print(f"🔀 Randomized property order for variety")

        # Step 3: Fetch details for top properties
        print(f"\n{'=' * 80}")
        print(f"STEP 3: Fetching Details (max {max_details})")
        print(f"{'=' * 80}")

        matching = []
        calls_left = self.MAX_CALLS - self.api_calls_made
        to_fetch = min(len(filtered), max_details, calls_left)

        print(f"\n📥 Fetching details for {to_fetch} properties...")

        for i, prop in enumerate(filtered[:to_fetch], 1):
            if self.api_calls_made >= self.MAX_CALLS:
                print("\n⚠️  Stopping at API limit")
                break

            address = prop.get("location", {}).get("address", {}).get("line", "Unknown")
            property_id = prop.get("property_id") or prop.get("id") or prop.get("listing_id")

            print(f"\n[{i}/{to_fetch}] {address} (ID: {property_id})...", end=" ")

            details = self._fetch_property_details(property_id)
            if details:
                # dump first result for debugging
                if i == 1 and not matching:
                    try:
                        debug_path = Path(__file__).parent / "debug_property_details.json"
                        with open(debug_path, 'w') as f:
                            json.dump(details, f, indent=2)
                    except Exception:
                        pass

                print("Checking criteria...", end=" ")
                if self._check_final_criteria(details):
                    print("✅ MATCH!")
                    matching.append(details)
                else:
                    print("❌")
            else:
                print("No details returned")

        return matching

    def _fetch_property_list(self, city: str) -> list[dict[str, Any]]:
        url = f"https://{self.api_host}/properties/sale"

        params = {
            "query": f"{city}, {self.state}",
            "limit": 50,
            "offset": 0,
            "price_min": self.min_price,
            "price_max": self.max_price,
            "bath": self.min_baths
        }

        data = self._make_api_call("GET", url, params=params)

        if data and data.get("success"):
            properties = data.get("data", {}).get("home_search", {}).get("results", [])
            return properties

        return []

    def _fetch_property_details(self, property_id: str) -> dict[str, Any] | None:
        if not property_id:
            return None

        url = f"https://{self.api_host}/detail/properties"
        return self._make_api_call("GET", url, params={"id": property_id})

    def _filter_basic(self, properties: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Client-side price filter before we spend API calls on details."""
        filtered = []

        print(f"\n🔍 Filtering {len(properties)} properties by price...")

        for prop in properties:
            price = prop.get("list_price") or prop.get("price", 0)
            address = prop.get("location", {}).get("address", {}).get("line", "Unknown")

            print(f"\n   🏠 {address}")
            print(f"      Price: ${price:,}")

            if price < self.min_price or price > self.max_price:
                print("      ❌ Price out of range")
                continue

            print("      ✅ Passed price filter")
            filtered.append(prop)

        return filtered

    def _check_final_criteria(self, details: dict[str, Any]) -> bool:
        """
        Check property type, basement, and pool from the detail endpoint.

        The basement logic is annoying - the API puts basement info in different
        places depending on the listing. We check details[], features[], and the
        description text as a fallback. Only accept explicitly finished/partial.
        """
        if not details:
            print("(no details)", end=" ")
            return False

        home = details.get("data", {}).get("home", {})
        if not home:
            home = details

        description = home.get("description", {})

        # Single-family only
        prop_type = description.get("type", "").lower()
        if not any(t in prop_type for t in ["single", "family", "house", "residential"]):
            print(f"(not single-family: '{prop_type}')", end=" ")
            return False

        # Basement check: need FINISHED or PARTIAL
        has_finished_or_partial_basement = False
        basement_source = None

        basement_keywords = ["basement", "lower level", "walkout", "walk-out", "daylight basement"]
        finished_keywords = ["finished", "partially finished", "partial", "renovated", "remodeled"]
        unfinished_keywords = ["unfinished", "rough", "concrete floor", "bare concrete"]

        # 1. Check details array (most reliable)
        details_list = home.get("details", [])
        for detail_group in details_list:
            if isinstance(detail_group, dict):
                texts = detail_group.get("text", [])
                category = detail_group.get("category", "").lower()

                if isinstance(texts, list):
                    for text in texts:
                        if isinstance(text, str):
                            text_lower = text.lower()

                            has_basement_mention = any(kw in text_lower for kw in basement_keywords)

                            if has_basement_mention:
                                if any(kw in text_lower for kw in unfinished_keywords):
                                    print("(unfinished basement)", end=" ")
                                    return False

                                if any(kw in text_lower for kw in finished_keywords):
                                    has_finished_or_partial_basement = True
                                    basement_source = f"details: {text[:50]}"
                                    break

                if has_finished_or_partial_basement:
                    break

        # 2. Check features array
        if not has_finished_or_partial_basement:
            features = home.get("features", [])
            for feature_group in features:
                if isinstance(feature_group, dict):
                    texts = feature_group.get("text", [])
                    if isinstance(texts, list):
                        for text in texts:
                            if isinstance(text, str):
                                text_lower = text.lower()
                                has_basement_mention = any(kw in text_lower for kw in basement_keywords)

                                if has_basement_mention:
                                    if any(kw in text_lower for kw in unfinished_keywords):
                                        print("(unfinished basement)", end=" ")
                                        return False

                                    if any(kw in text_lower for kw in finished_keywords):
                                        has_finished_or_partial_basement = True
                                        basement_source = f"features: {text[:50]}"
                                        break

                    if has_finished_or_partial_basement:
                        break

        # 3. Description text as last resort
        if not has_finished_or_partial_basement:
            desc_text = description.get("text", "")
            if desc_text:
                desc_lower = desc_text.lower()
                has_basement_mention = any(kw in desc_lower for kw in basement_keywords)

                if has_basement_mention:
                    if any(kw in desc_lower for kw in unfinished_keywords):
                        print("(unfinished basement)", end=" ")
                        return False

                    if any(kw in desc_lower for kw in finished_keywords):
                        has_finished_or_partial_basement = True
                        basement_source = "description text"

        if not has_finished_or_partial_basement:
            print("(no finished/partial basement found)", end=" ")
            return False

        if basement_source:
            print(f"(basement from {basement_source})", end=" ")

        # Pool check (must NOT have)
        has_pool = description.get("pool") or False

        for detail_group in details_list:
            if isinstance(detail_group, dict):
                category = detail_group.get("category", "").lower()
                texts = detail_group.get("text", [])

                if "pool" in category:
                    has_pool = True
                    break

                if isinstance(texts, list):
                    for text in texts:
                        if isinstance(text, str) and "pool" in text.lower():
                            has_pool = True
                            break

        if has_pool:
            print("(has pool)", end=" ")
            return False

        return True

    def print_results(self, properties: list[dict[str, Any]]):
        print("\n" + "=" * 80)
        print(f"🎉 FOUND {len(properties)} MATCHING PROPERTIES!")
        print("=" * 80)

        if not properties:
            print("\n😔 No properties matched all criteria")
            return

        for i, prop_data in enumerate(properties, 1):
            home = prop_data.get("data", {}).get("home", {})
            if not home:
                home = prop_data

            location = home.get("location", {})
            address = location.get("address", {})
            description = home.get("description", {})

            print(f"\n{'=' * 80}")
            print(f"PROPERTY #{i}")
            print(f"{'=' * 80}")
            print(f"📍 Address: {address.get('line', 'N/A')}")
            print(f"💰 Price: ${home.get('list_price', 0):,}")
            print(f"🛏️  Beds: {description.get('beds', 'N/A')}")
            print(f"🚿 Baths: {description.get('baths', 'N/A')}")
            print(f"📐 Sqft: {description.get('sqft', 'N/A'):,}" if description.get('sqft') else "📐 Sqft: N/A")
            print(f"📅 Year: {description.get('year_built', 'N/A')}")
            print(f"🔗 URL: {home.get('href', 'N/A')}")

    def save_results(self, properties: list[dict[str, Any]], filename: str = "matching_properties"):
        try:
            filepath = Path(__file__).parent / f"{filename}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(properties, f, indent=2, ensure_ascii=False)
            print(f"\n💾 Saved to: {filename}.json")
        except Exception as e:
            print(f"\n❌ Failed to save: {str(e)}")

    def print_summary(self):
        print("\n" + "=" * 80)
        print("📊 API USAGE SUMMARY")
        print("=" * 80)
        print(f"✅ API Calls Used: {self.api_calls_made}/{self.MAX_CALLS}")
        print(f"✅ Calls Remaining: {self.MAX_CALLS - self.api_calls_made}")
        print(f"📈 Monthly Budget: ~{10000 - (self.api_calls_made * 240)}/10,000 estimated remaining")


def main():
    print("\n" + "=" * 80)
    print("🏠 HOUSE HUNTER")
    print("=" * 80)

    try:
        scraper = RealtorAPIScraper()
        matching = scraper.search_properties(max_details=30)
        scraper.print_results(matching)

        if matching:
            scraper.save_results(matching)

        scraper.print_summary()

        print("\n" + "=" * 80)
        print("✅ SEARCH COMPLETE!")
        print("=" * 80)

    except ValueError as e:
        print(f"\n❌ Configuration Error: {str(e)}")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
