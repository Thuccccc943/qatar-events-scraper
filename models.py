from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Any
from datetime import datetime


@dataclass
class Event:
    # Core fields (should be provided by all scrapers)
    title: str
    start_date: str
    source: str  # Which scraper collected this

    # Basic optional fields (most scrapers should provide these)
    end_date: Optional[str] = None
    time: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    description: Optional[str] = None
    location: Optional[str] = None
    link: Optional[str] = None

    # Detailed fields (only some scrapers will provide these)
    directions: Optional[str] = None
    category: Optional[str] = None
    price: Optional[str] = None
    tickets: Optional[str] = None
    image_url: Optional[str] = None
    age_restriction: Optional[str] = None
    organizer: Optional[str] = None
    tags: List[str] = field(default_factory=list)

    # Raw data storage for debugging/processing
    raw_data: Optional[Dict[str, Any]] = None

    def to_dict(self) -> dict:
        """Convert Event to dictionary for CSV export, excluding raw_data"""
        data = asdict(self)
        data.pop("raw_data", None)  # Remove raw_data from export
        return data

    @classmethod
    def get_field_names(cls) -> List[str]:
        """Get all field names for CSV header, excluding raw_data"""
        fields = list(cls.__annotations__.keys())
        fields.remove("raw_data")
        return fields

    def __post_init__(self):
        """Set end_date = start_date if not provided"""
        if self.end_date is None:
            self.end_date = self.start_date
