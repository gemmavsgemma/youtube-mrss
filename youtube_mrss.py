#!/usr/bin/env python3
"""
youtube_mrss.py — Generate an MRSS feed from a YouTube channel.

Usage:
    python youtube_mrss.py --channel CHANNEL_ID --api-key YOUR_KEY --output feed.xml

    # Or use environment variable for the key:
    export YOUTUBE_API_KEY=YOUR_KEY
    python youtube_mrss.py --channel UCb1XCmhBJpJKouf2ZMNdlyA --output feed.xml

    # Limit to most recent N videos:
    python youtube_mrss.py --channel UCb1XCmhBJpJKouf2ZMNdlyA --max 50 --output feed.xml

Dependencies:
    pip install google-api-python-client lxml
"""

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from email.utils import format_datetime

from googleapiclient.discovery import build
from lxml import etree


# ---------------------------------------------------------------------------
# YouTube API helpers
# ---------------------------------------------------------------------------

def get_uploads_playlist_id(channel_id: str) -> str:
    """Convert a channel ID to its uploads playlist ID."""
    if channel_id.startswith("UC"):
        return "UU" + channel_id[2:]
    raise ValueError(
        f"Expected a channel ID starting with 'UC', got '{channel_id}'"
    )


def get_channel_metadata(youtube, channel_id: str) -> dict:
    """Fetch channel title, description, and thumbnail."""
    resp = youtube.channels().list(
        part="snippet",
        id=channel_id,
    ).execute()

    if not resp.get("items"):
        raise ValueError(f"Channel not found: {channel_id}")

    snippet = resp["items"][0]["snippet"]
    return {
        "title": snippet["title"],
        "description": snippet.get("description", ""),
        "thumbnail": snippet["thumbnails"].get("high", {}).get("url", ""),
        "url": f"https://www.youtube.com/channel/{channel_id}",
    }


def fetch_video_ids(youtube, playlist_id: str, max_results: int = 0) -> list[str]:
    """Page through a playlist and collect video IDs."""
    video_ids = []
    next_page = None

    while True:
        resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page,
        ).execute()

        for item in resp.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])

        if max_results and len(video_ids) >= max_results:
            video_ids = video_ids[:max_results]
            break

        next_page = resp.get("nextPageToken")
        if not next_page:
            break

    return video_ids


def fetch_video_details(youtube, video_ids: list[str]) -> list[dict]:
    """Batch-fetch snippet + contentDetails for a list of video IDs."""
    videos = []

    # YouTube API allows 50 IDs per request
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        resp = youtube.videos().list(
            part="snippet,contentDetails",
            id=",".join(batch),
        ).execute()

        for item in resp.get("items", []):
            snippet = item["snippet"]
            content = item["contentDetails"]

            videos.append(
                {
                    "id": item["id"],
                    "title": snippet["title"],
                    "description": snippet.get("description", ""),
                    "published": snippet["publishedAt"],
                    "tags": snippet.get("tags", []),
                    "category_id": snippet.get("categoryId", ""),
                    "channel_title": snippet.get("channelTitle", ""),
                    "thumbnails": snippet.get("thumbnails", {}),
                    "duration_iso": content.get("duration", "PT0S"),
                }
            )

    return videos


# ---------------------------------------------------------------------------
# Duration parsing
# ---------------------------------------------------------------------------

_ISO_DURATION = re.compile(
    r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
)


def iso_duration_to_seconds(iso: str) -> int:
    """Convert ISO 8601 duration (PT1H2M3S) to total seconds."""
    m = _ISO_DURATION.match(iso)
    if not m:
        return 0
    h, mn, s = (int(g) if g else 0 for g in m.groups())
    return h * 3600 + mn * 60 + s


def seconds_to_hms(total: int) -> str:
    """Format seconds as HH:MM:SS."""
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


# ---------------------------------------------------------------------------
# MRSS generation
# ---------------------------------------------------------------------------

MRSS_NS = "http://search.yahoo.com/mrss/"
ATOM_NS = "http://www.w3.org/2005/Atom"
YT_NS = "http://www.youtube.com/xml/schemas/2015"

NSMAP = {
    "media": MRSS_NS,
    "atom": ATOM_NS,
    "yt": YT_NS,
}


def best_thumbnail(thumbnails: dict) -> tuple[str, int, int]:
    """Pick the highest-res thumbnail available. Returns (url, width, height)."""
    for key in ("maxres", "high", "medium", "default"):
        if key in thumbnails:
            t = thumbnails[key]
            return t["url"], t.get("width", 0), t.get("height", 0)
    return "", 0, 0


def build_mrss(channel_meta: dict, videos: list[dict]) -> bytes:
    """Build a complete MRSS XML document."""
    rss = etree.Element("rss", version="2.0", nsmap=NSMAP)
    channel = etree.SubElement(rss, "channel")

    # Channel metadata
    etree.SubElement(channel, "title").text = channel_meta["title"]
    etree.SubElement(channel, "link").text = channel_meta["url"]
    etree.SubElement(channel, "description").text = channel_meta["description"]
    etree.SubElement(
        channel, "lastBuildDate"
    ).text = format_datetime(datetime.now(timezone.utc))

    if channel_meta.get("thumbnail"):
        img = etree.SubElement(channel, "image")
        etree.SubElement(img, "url").text = channel_meta["thumbnail"]
        etree.SubElement(img, "title").text = channel_meta["title"]
        etree.SubElement(img, "link").text = channel_meta["url"]

    # Self-referencing Atom link (optional, but good practice)
    etree.SubElement(
        channel,
        "{%s}link" % ATOM_NS,
        rel="self",
        type="application/rss+xml",
        href=channel_meta["url"],
    )

    # Video items
    for v in videos:
        item = etree.SubElement(channel, "item")

        video_url = f"https://www.youtube.com/watch?v={v['id']}"
        guid = etree.SubElement(item, "guid", isPermaLink="true")
        guid.text = video_url

        etree.SubElement(item, "title").text = v["title"]
        etree.SubElement(item, "link").text = video_url
        etree.SubElement(item, "description").text = v["description"]

        # Parse and format pubDate
        try:
            pub = datetime.fromisoformat(v["published"].replace("Z", "+00:00"))
            etree.SubElement(item, "pubDate").text = format_datetime(pub)
        except (ValueError, AttributeError):
            pass

        etree.SubElement(item, "author").text = v.get("channel_title", "")

        # --- media:group (wraps all MRSS elements) ---
        group = etree.SubElement(item, "{%s}group" % MRSS_NS)

        etree.SubElement(group, "{%s}title" % MRSS_NS).text = v["title"]
        etree.SubElement(group, "{%s}description" % MRSS_NS).text = v["description"]

        # media:content
        duration_s = iso_duration_to_seconds(v["duration_iso"])
        content_attrs = {
            "url": video_url,
            "type": "video/mp4",
            "medium": "video",
            "expression": "full",
        }
        if duration_s:
            content_attrs["duration"] = str(duration_s)

        etree.SubElement(group, "{%s}content" % MRSS_NS, **content_attrs)

        # media:thumbnail — include all available sizes
        for size_key in ("maxres", "high", "medium", "default"):
            if size_key in v.get("thumbnails", {}):
                t = v["thumbnails"][size_key]
                thumb_attrs = {"url": t["url"]}
                if t.get("width"):
                    thumb_attrs["width"] = str(t["width"])
                if t.get("height"):
                    thumb_attrs["height"] = str(t["height"])
                etree.SubElement(
                    group, "{%s}thumbnail" % MRSS_NS, **thumb_attrs
                )

        # media:keywords
        if v.get("tags"):
            etree.SubElement(
                group, "{%s}keywords" % MRSS_NS
            ).text = ", ".join(v["tags"])

        # media:player — embeddable URL
        etree.SubElement(
            group,
            "{%s}player" % MRSS_NS,
            url=f"https://www.youtube.com/embed/{v['id']}",
        )

        # Human-readable duration as media:text (some platforms use this)
        if duration_s:
            dur_el = etree.SubElement(
                group, "{%s}text" % MRSS_NS, type="plain"
            )
            dur_el.text = seconds_to_hms(duration_s)

    return etree.tostring(
        rss,
        xml_declaration=True,
        encoding="UTF-8",
        pretty_print=True,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate an MRSS feed from a YouTube channel."
    )
    parser.add_argument(
        "--channel",
        required=True,
        help="YouTube channel ID (starts with UC).",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("YOUTUBE_API_KEY", ""),
        help="YouTube Data API v3 key. Falls back to YOUTUBE_API_KEY env var.",
    )
    parser.add_argument(
        "--output",
        default="feed.xml",
        help="Output file path (default: feed.xml).",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=0,
        help="Limit to N most recent videos. 0 = all (default).",
    )
    args = parser.parse_args()

    if not args.api_key:
        print(
            "Error: No API key. Use --api-key or set YOUTUBE_API_KEY.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not args.channel.startswith("UC"):
        print(
            f"Error: Channel ID should start with 'UC', got '{args.channel}'.",
            file=sys.stderr,
        )
        sys.exit(1)

    youtube = build("youtube", "v3", developerKey=args.api_key)

    print(f"Fetching channel metadata for {args.channel}...")
    channel_meta = get_channel_metadata(youtube, args.channel)
    print(f"  Channel: {channel_meta['title']}")

    playlist_id = get_uploads_playlist_id(args.channel)
    print(f"Fetching video IDs from playlist {playlist_id}...")
    video_ids = fetch_video_ids(youtube, playlist_id, max_results=args.max)
    print(f"  Found {len(video_ids)} videos.")

    print("Fetching video details...")
    videos = fetch_video_details(youtube, video_ids)
    print(f"  Got details for {len(videos)} videos.")

    print("Building MRSS feed...")
    xml_bytes = build_mrss(channel_meta, videos)

    with open(args.output, "wb") as f:
        f.write(xml_bytes)

    size_kb = len(xml_bytes) / 1024
    print(f"Done. Wrote {args.output} ({size_kb:.1f} KB, {len(videos)} videos).")


if __name__ == "__main__":
    main()
