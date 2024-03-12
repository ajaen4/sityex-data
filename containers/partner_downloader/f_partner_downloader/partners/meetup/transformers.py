def extract_past_events(results: dict) -> list[list[str]]:
    return [
        {
            "title": event["node"]["title"],
            "description": event["node"]["description"],
            "venue_name": event["node"]["venue"]["name"],
            "venue_address": event["node"]["venue"]["address"],
            "event_url": event["node"]["eventUrl"],
            "image_url": event["node"]["image"]["source"],
            "timestamp": event["node"]["dateTime"],
            "going_count": event["node"]["going"],
        }
        for event in results["data"]["groupByUrlname"]["pastEvents"]["edges"]
    ]
