import json
import random
from typing import List, Dict, Optional

from faker import Faker

from .config import FakerConfig
from .helpers import generate_session_ids, decide_conversion, generate_base_time, \
    generate_event_count, choose_product_for_session, generate_session_context,\
    choose_event_type, advance_event_time, update_page_url_for_event, \
    build_extra_payload, build_raw_event



class SessionEventFaker:
    """
    OOP wrapper for Faker instance.
    Contains all  generation logic
    """
    def __init__(self, config: Optional[FakerConfig] = None):
        self.config = config or FakerConfig()
        self.faker = Faker()

        if self.config.seed is not None:
            Faker.seed(self.config.seed)
            random.seed(self.config.seed)

    def generate_session_events(self) -> List[Dict]:
        """
        Generate events for a single user session
        """
        session_id, user_id = generate_session_ids()
        will_convert = decide_conversion(self.config.conversion_probability)
        base_time = generate_base_time()
        n_events = generate_event_count(self.config.min_events, self.config.max_events)

        product_id, category, price = choose_product_for_session(
            will_convert=will_convert,
            categories=list(self.config.categories),
        )

        ctx = generate_session_context(self.faker, self.config)
        current_url = ctx["current_url"]
        referrer_url = ctx["referrer_url"]
        user_agent = ctx["user_agent"]
        ip_address = ctx["ip_address"]
        ab_group = ctx["ab_group"]

        events: List[Dict] = []

        for i in range(n_events):
            event_time = advance_event_time(base_time, i)

            event_type = choose_event_type(
                event_index=i,
                total_events=n_events,
                will_convert=will_convert,
            )

            current_url, referrer_url = update_page_url_for_event(
                base_url=self.config.base_url,
                current_url=current_url,
                event_type=event_type,
                product_id=product_id,
                faker=self.faker,
            )

            extra = build_extra_payload(
                event_type=event_type,
                ab_group=ab_group,
                price=price,
                payment_methods=list(self.config.payment_methods),
            )

            raw_event = build_raw_event(
                event_time=event_time,
                session_id=session_id,
                user_id=user_id,
                event_type=event_type,
                page_url=current_url,
                referrer_url=referrer_url,
                user_agent=user_agent,
                ip_address=ip_address,
                product_id=product_id,
                price=price,
                extra=extra,
            )

            events.append(raw_event)

        return events
