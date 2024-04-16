from typing import Any


def extract_past_events(results: dict) -> list[dict]:
    return [
        extract_event(event)
        for event in results["data"]["groupByUrlname"]["pastEvents"]["edges"]
    ]


def extract_curr_events(results: dict) -> list[dict[str, Any]]:
    return [
        extract_event(event)
        for event in results["data"]["groupByUrlname"]["upcomingEvents"][
            "edges"
        ]
    ]


def extract_event(event: dict) -> dict:
    return {
        "event_id": event["node"]["id"],
        "title": event["node"]["title"],
        "description": event["node"]["description"],
        "venue_name": event["node"]["venue"]["name"],
        "venue_address": event["node"]["venue"]["address"],
        "latitude": event["node"]["venue"]["lat"],
        "longitude": event["node"]["venue"]["lng"],
        "event_url": event["node"]["eventUrl"],
        "image_url": event["node"]["image"]["source"],
        "timestamp": event["node"]["dateTime"],
        "going_count": event["node"]["going"],
    }
