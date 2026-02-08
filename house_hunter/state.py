"""State definitions for the house hunter workflow."""

from typing import Any, TypedDict


class PropertyData(TypedDict):
    """A single property from the API."""
    property_id: str
    address: str
    city: str
    state: str
    zip_code: str
    price: int
    beds: int | None
    baths: float | None
    sqft: int | None
    year_built: int | None
    lot_size: int | None
    property_type: str
    description: str | None
    listing_url: str | None
    photo_url: str | None
    has_basement: bool | None
    basement_finished: bool | None
    has_pool: bool | None
    has_bathtub: bool | None
    raw_data: dict[str, Any]


class ReviewResult(TypedDict):
    """Output from the reviewer agent."""
    property_id: str
    passes: bool
    reasons: list[str]
    concerns: list[str]
    missing_info: list[str]
    review_timestamp: str


class HouseHunterState(TypedDict):
    """Main state passed through all LangGraph nodes."""
    # Workflow metadata
    run_id: str
    started_at: str
    completed_at: str | None

    # Search parameters
    min_price: int
    max_price: int
    cities: list[str]

    # Property data through the pipeline
    properties: list[PropertyData]
    reviewed_properties: list[ReviewResult]
    passed_properties: list[PropertyData]
    notified_properties: list[str]

    # Error tracking
    errors: list[dict[str, Any]]
    warnings: list[str]

    # API usage tracking
    api_calls_used: int
    api_calls_limit: int

    # Control flags
    should_notify: bool
    test_mode: bool
