#############################################################
# helpers.py
#
# Transformation functions for the medallion architecture.
# Handles data conversion between Bronze -> Silver -> Gold layers:
#############################################################

from typing import Optional
from urllib.parse import urlparse
from datetime import datetime

from gold_models import GoldPageView, GoldProductEvent
from silver_model import SilverWebEvent
from bronze_model import BronzeWebEvent

###################
# BRONZE -> SILVER
###################

def bronze_to_silver(event: BronzeWebEvent) -> SilverWebEvent:
    parsed = urlparse(event.page_url)
    path_parts = [p for p in parsed.path.split("/") if p]

    page_section = path_parts[0] if len(path_parts) >= 1 else None
    page_category = path_parts[1] if len(path_parts) >= 2 else None
    page_item = path_parts[2] if len(path_parts) >= 3 else None

    scroll_depth = event.extra.get("scroll_depth")
    ab_group = event.extra.get("ab_group")

    return SilverWebEvent(
        event_time=datetime.fromisoformat(event.event_time),
        session_id=event.session_id,
        user_id=event.user_id,
        event_type=event.event_type,
        page_url=event.page_url,
        page_path=parsed.path,
        page_host=parsed.netloc,
        page_section=page_section,
        page_category=page_category,
        page_item=page_item,
        referrer_url=event.referrer_url,
        product_id=event.product_id,
        price=event.price,
        ab_group=ab_group,
        scroll_depth=scroll_depth,
        ip_address=event.ip_address,
        user_agent=event.user_agent,
    )

###################
# SILVER -> GOLD
###################

def silver_to_gold_page_view(e: SilverWebEvent) -> GoldPageView:
    """
    Extract page view data from Silver event for analytics layer.

    Selects only fields relevant for page view analysis,
    dropping technical metadata like IP and user agent.
    """
    return GoldPageView(
        event_time=e.event_time,
        session_id=e.session_id,
        user_id=e.user_id,
        page_url=e.page_url,
        page_category=e.page_category,
        page_item=e.page_item,
        scroll_depth=e.scroll_depth,
        ab_group=e.ab_group,
    )


def silver_to_gold_product(e: SilverWebEvent) -> Optional[GoldProductEvent]:
    """
    Extract product interaction data from Silver event.

    Returns None if event has no associated product.
    Used for product analytics and revenue tracking.
    """
    if not e.product_id:
        return None

    return GoldProductEvent(
        event_time=e.event_time,
        session_id=e.session_id,
        user_id=e.user_id,
        product_id=e.product_id,
        price=e.price,
        ab_group=e.ab_group,
        page_url=e.page_url,
    )