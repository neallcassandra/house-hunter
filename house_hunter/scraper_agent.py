"""Adapter between the raw Realtor API scraper and the LangGraph workflow."""

import logging
from typing import Any

from .scraper import RealtorAPIScraper
from .state import PropertyData

logger = logging.getLogger(__name__)


class ScraperAgent:

    def __init__(self):
        self.scraper = RealtorAPIScraper()
        logger.info("ScraperAgent initialized")

    def search_properties(self) -> list[PropertyData]:
        """Run scraper and convert results to PropertyData format."""
        try:
            matching = self.scraper.search_properties(max_details=15)

            properties = []
            for prop in matching:
                property_data = self._convert_to_property_data(prop)
                if property_data:
                    properties.append(property_data)

            logger.info(f"Found {len(properties)} properties")
            return properties

        except Exception as e:
            logger.error(f"Error searching properties: {e}")
            return []

    def _convert_to_property_data(self, raw_data: dict[str, Any]) -> PropertyData:
        """Pull property fields out of the nested API response."""
        try:
            home = raw_data.get("data", {}).get("home", {})
            if not home:
                home = raw_data

            location = home.get("location", {})
            address = location.get("address", {})
            description = home.get("description", {})

            # Basement detection: check multiple places because the API is inconsistent
            has_basement = False
            basement_finished = False
            has_bathtub = False
            basement_source = None

            basement_keywords = ["basement", "lower level", "walkout", "walk-out", "daylight basement"]
            finished_keywords = ["finished", "finish", "partial", "partially finished", "renovated"]

            # 1. details array
            details = home.get("details", [])
            for detail_group in details:
                if isinstance(detail_group, dict):
                    texts = detail_group.get("text", [])
                    category = detail_group.get("category", "").lower()

                    for text in texts:
                        if isinstance(text, str):
                            text_lower = text.lower()

                            if any(kw in text_lower for kw in basement_keywords):
                                has_basement = True
                                if any(kw in text_lower for kw in finished_keywords):
                                    basement_finished = True
                                    basement_source = "details"

                            if "bathtub" in category or "bathroom" in category:
                                if "tub" in text_lower or "bathtub" in text_lower or "soaking" in text_lower:
                                    has_bathtub = True

            # 2. features array
            if not basement_finished:
                features = home.get("features", [])
                for feature_group in features:
                    if isinstance(feature_group, dict):
                        texts = feature_group.get("text", [])
                        for text in texts:
                            if isinstance(text, str):
                                text_lower = text.lower()
                                if any(kw in text_lower for kw in basement_keywords):
                                    has_basement = True
                                    if any(kw in text_lower for kw in finished_keywords):
                                        basement_finished = True
                                        basement_source = "features"
                                        break
                        if basement_finished:
                            break

            # 3. description text as fallback
            if not basement_finished:
                desc_text = description.get("text", "")
                if desc_text:
                    desc_lower = desc_text.lower()
                    if any(kw in desc_lower for kw in basement_keywords):
                        has_basement = True
                        if any(kw in desc_lower for kw in finished_keywords):
                            basement_finished = True
                            basement_source = "description"

            if basement_finished and basement_source:
                logger.info(f"Basement detected from {basement_source}: {home.get('property_id')}")

            return {
                "property_id": home.get("property_id", ""),
                "address": address.get("line", ""),
                "city": address.get("city", ""),
                "state": address.get("state_code", "OH"),
                "zip_code": address.get("postal_code", ""),
                "price": home.get("list_price", 0),
                "beds": description.get("beds"),
                "baths": description.get("baths"),
                "sqft": description.get("sqft"),
                "year_built": description.get("year_built"),
                "lot_size": description.get("lot_sqft"),
                "property_type": description.get("type", ""),
                "description": description.get("text", ""),
                "listing_url": home.get("href", ""),
                "photo_url": home.get("primary_photo", {}).get("href", ""),
                "has_basement": has_basement,
                "basement_finished": basement_finished,
                "has_pool": description.get("pool", False),
                "has_bathtub": has_bathtub,
                "raw_data": raw_data
            }
        except Exception as e:
            logger.error(f"Error converting property data: {e}")
            return None
