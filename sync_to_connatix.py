#!/usr/bin/env python3
"""
sync_to_connatix.py — Sync YouTube videos to Connatix via GitHub release staging.

Downloads video from YouTube, uploads to a temporary GitHub release,
passes the public URL to Connatix for encoding, then cleans up.

The script determines "what's new" by querying the Connatix library directly,
NOT by reading a local tracking file. Connatix is the source of truth — if
a video is already in the library it's skipped, regardless of any local
state. This makes the script idempotent against bulk imports, manual
deletes, manual uploads via the Connatix dashboard, and tracking-file loss.

A tracking file (synced_videos.json) is still WRITTEN for each sync — but
purely as an audit log used by `--cleanup` to find GitHub release IDs to
delete. It is NEVER read to decide what to sync.

Usage:
    # Sync a single video (test mode):
    python sync_to_connatix.py --video-id 3Pv6aES9ruQ

    # Sync all new videos from MRSS feed (diffs against Connatix library):
    python sync_to_connatix.py --feed feeds/avforums.xml

    # Sync only videos listed in gaps file (uses feed for metadata):
    python sync_to_connatix.py --gaps gaps.txt --feed-for-gaps avforums.xml

    # Dry run (show what would be synced, don't upload):
    python sync_to_connatix.py --feed feeds/avforums.xml --dry-run

    # Clean up GitHub releases after Connatix has encoded:
    python sync_to_connatix.py --cleanup

    # Use browser cookies for YouTube auth (fixes bot detection):
    python sync_to_connatix.py --feed feeds/avforums.xml --cookies-from-browser chrome

Environment variables:
    CONNATIX_EMAIL       Connatix login email
    CONNATIX_PASSWORD    Connatix login password
    CONNATIX_ACCOUNT_ID  Connatix account ID (fetched automatically if not set)
    GITHUB_TOKEN         GitHub personal access token (needs repo scope)
    GITHUB_REPO          GitHub repo in owner/name format (e.g. gemmavsgemma/youtube-mrss)

Dependencies:
    pip install requests lxml yt-dlp python-dotenv
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from lxml import etree

# Load .env from the current working directory if python-dotenv is installed.
# Falls back silently if not — env vars set in the shell will still work.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CONNATIX_AUTH_URL = "https://auth.connatix.com/auth/login"
CONNATIX_GRAPHQL_URL = "https://conapi.connatix.com/graphql"
GITHUB_API = "https://api.github.com"
MRSS_NS = "http://search.yahoo.com/mrss/"
TRACKING_FILE = "synced_videos.json"

# ---------------------------------------------------------------------------
# Connatix auth
# ---------------------------------------------------------------------------


def get_jwt(email: str, password: str) -> str:
    """Authenticate with Connatix and return a JWT."""
    resp = requests.post(
        CONNATIX_AUTH_URL,
        json={"Email": email, "Password": password},
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    if not token:
        raise ValueError(f"No token in auth response: {resp.text}")
    return token


def get_account_id(jwt: str) -> str:
    """Fetch the Connatix account ID."""
    query = """
    query {
        accounts {
            search(pagination: {}, filtering: {state: ACTIVE}) {
                items {
                    id
                }
            }
        }
    }
    """
    resp = requests.post(
        CONNATIX_GRAPHQL_URL,
        data=query,
        headers={
            "Authorization": f"Bearer {jwt}",
            "Content-Type": "application/graphql",
        },
    )
    resp.raise_for_status()
    items = resp.json().get("data", {}).get("accounts", {}).get("search", {}).get("items", [])
    if not items:
        raise ValueError(f"No active accounts found: {resp.text}")
    return items[0]["id"]


# ---------------------------------------------------------------------------
# Connatix library snapshot (source of truth for what's already synced)
# ---------------------------------------------------------------------------


def get_existing_connatix_youtube_ids(jwt: str, account_id: str) -> set:
    """
    Fetch every active media item in the Connatix library and return the set
    of YouTube IDs derived from `sourceItemId`.

    This replaces tracking-file based diff logic. The tracking file was a
    cache of intent — what the script believed it had synced — and could
    drift away from Connatix reality through manual uploads, manual deletes,
    bulk imports, retried encodes, or anyone else editing the dashboard.

    Connatix is the only source of truth that matters for "is this video
    already in the library?", so we ask Connatix directly.

    Returns a set of bare YouTube IDs (no `yt:` prefix — that's stripped to
    match how the Worker's MRSS join normalises).
    """
    ids = set()
    offset = 0
    limit = 100

    while True:
        query = f"""
        query {{
            media {{
                search(
                    pagination: {{ offset: {offset}, limit: {limit} }},
                    filtering: {{ state: ACTIVE, type: VIDEO }}
                ) {{
                    items {{
                        sourceItemId
                    }}
                }}
            }}
        }}
        """
        resp = requests.post(
            CONNATIX_GRAPHQL_URL,
            data=query,
            headers={
                "Authorization": f"Bearer {jwt}",
                "Content-Type": "application/graphql",
                "AccountId": account_id,
            },
            timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()

        if "errors" in result:
            raise ValueError(f"GraphQL error during library fetch: {json.dumps(result['errors'])}")

        items = result.get("data", {}).get("media", {}).get("search", {}).get("items", []) or []
        if not items:
            break

        for item in items:
            sid = (item.get("sourceItemId") or "").strip()
            if not sid:
                continue
            # Strip the `yt:` prefix that some historical records carry
            # (Samsung S99H and DALI SONIK 5 had this). Matches the Worker's
            # MRSS join normalisation.
            if sid.lower().startswith("yt:"):
                sid = sid[3:]
            # Defensive: only keep the 11-char YouTube ID shape so we don't
            # accidentally pollute the set with stray non-YouTube sourceItemIds
            if re.fullmatch(r"[a-zA-Z0-9_-]{11}", sid):
                ids.add(sid)

        # Stop if the page was short (last page) or we've hit a sanity ceiling
        if len(items) < limit:
            break
        offset += limit
        if offset > 20000:
            print(f"  WARNING: library pagination hit safety ceiling at offset {offset}")
            break

    return ids


# ---------------------------------------------------------------------------
# Connatix media creation
# ---------------------------------------------------------------------------


def create_media(jwt: str, account_id: str, video: dict) -> dict:
    """Create a media item in Connatix via GraphQL mutation."""

    def esc(s: str) -> str:
        if not s:
            return ""
        s = re.sub(r'[^\u0000-\uFFFF]', '', s)
        s = s.replace("&", "and")
        return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "")

    title = esc(video["title"][:100])
    description = esc(video.get("description", ""))[:3000]  # Connatix 3k char limit

    keywords = video.get("keywords", [])
    # Connatix caps individual keywords at 40 chars — filter out longer ones
    keywords = [k for k in keywords if k and len(k) <= 40]
    keywords_str = ", ".join(f'"{esc(k)}"' for k in keywords) if keywords else ""

    mutation_parts = [
        f'accountId: "{account_id}"',
        f'title: "{title}"',
        f'inputVideoUrl: "{video["video_url"]}"',
    ]

    if description:
        mutation_parts.append(f'description: "{description}"')

    if video.get("thumbnail"):
        mutation_parts.append(f'inputThumbnailUrl: "{video["thumbnail"]}"')

    if keywords_str:
        mutation_parts.append(f"keywords: [{keywords_str}]")

    if video.get("source_item_id"):
        mutation_parts.append(f'sourceItemId: "{video["source_item_id"]}"')

    # Note: intentionally NOT setting clickUrl — when present, the Connatix
    # player redirects to YouTube on click. We want clicks to play/pause only.

    mutation_body = ",\n   ".join(mutation_parts)

    mutation = f"""
    mutation {{
        media {{
            create(media: {{
                {mutation_body}
            }}) {{
                objectId,
                success
            }}
        }}
    }}
    """

    resp = requests.post(
        CONNATIX_GRAPHQL_URL,
        data=mutation,
        headers={
            "Authorization": f"Bearer {jwt}",
            "Content-Type": "application/graphql",
            "AccountId": account_id,
        },
    )
    resp.raise_for_status()
    result = resp.json()

    if "errors" in result:
        raise ValueError(f"GraphQL error: {json.dumps(result['errors'], indent=2)}")

    return result.get("data", {}).get("media", {}).get("create", {})


# ---------------------------------------------------------------------------
# GitHub release staging
# ---------------------------------------------------------------------------


def github_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def create_github_release(token: str, repo: str, video_id: str) -> dict:
    """Create a temporary GitHub release for staging a video file."""
    tag = f"staging-{video_id}-{int(time.time())}"
    resp = requests.post(
        f"{GITHUB_API}/repos/{repo}/releases",
        json={
            "tag_name": tag,
            "name": f"Video staging: {video_id}",
            "body": "Temporary release for Connatix video ingestion. Will be deleted automatically.",
            "draft": False,
            "prerelease": True,
        },
        headers=github_headers(token),
    )
    resp.raise_for_status()
    return resp.json()


def upload_release_asset(token: str, release: dict, filepath: str) -> str:
    """Upload a file to a GitHub release and return the public download URL."""
    upload_url = release["upload_url"].replace("{?name,label}", "")
    filename = Path(filepath).name

    with open(filepath, "rb") as f:
        resp = requests.post(
            upload_url,
            params={"name": filename},
            data=f,
            headers={
                **github_headers(token),
                "Content-Type": "application/octet-stream",
            },
        )
    resp.raise_for_status()
    asset = resp.json()

    return asset["browser_download_url"]


def delete_github_release(token: str, repo: str, release_id: int, tag: str):
    """Delete a GitHub release and its tag."""
    requests.delete(
        f"{GITHUB_API}/repos/{repo}/releases/{release_id}",
        headers=github_headers(token),
    )
    requests.delete(
        f"{GITHUB_API}/repos/{repo}/git/refs/tags/{tag}",
        headers=github_headers(token),
    )


# ---------------------------------------------------------------------------
# YouTube download
# ---------------------------------------------------------------------------


def download_video(video_id: str, output_dir: str = ".", cookies_from_browser: str = None) -> str:
    """Download a YouTube video and return the local file path."""
    output_path = os.path.join(output_dir, f"{video_id}.mp4")

    cmd = [
        sys.executable, "-m", "yt_dlp",
    ]

    # Add browser cookies if specified (fixes YouTube bot detection)
    if cookies_from_browser:
        cmd.extend(["--cookies-from-browser", cookies_from_browser])

    cmd.extend([
        "--extractor-args", "youtube:jsc=deno",
        "--remote-components", "ejs:github",
        "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]",
        "--merge-output-format", "mp4",
        "-o", output_path,
        f"https://www.youtube.com/watch?v={video_id}",
    ])
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp failed: {result.stderr}")

    if not os.path.exists(output_path):
        raise FileNotFoundError(f"Expected file not found: {output_path}")

    return output_path


def get_video_metadata(video_id: str, cookies_from_browser: str = None) -> dict:
    """Fetch metadata from YouTube without downloading."""
    cmd = [
        sys.executable, "-m", "yt_dlp",
    ]

    # Add browser cookies if specified (fixes YouTube bot detection)
    if cookies_from_browser:
        cmd.extend(["--cookies-from-browser", cookies_from_browser])

    cmd.extend([
        "--extractor-args", "youtube:jsc=deno",
        "--remote-components", "ejs:github",
        "--dump-json", "--no-download",
        f"https://www.youtube.com/watch?v={video_id}",
    ])
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp metadata failed: {result.stderr}")

    meta = json.loads(result.stdout)

    best_thumb = ""
    for thumb in meta.get("thumbnails", []):
        best_thumb = thumb.get("url", best_thumb)

    iso_date = ""
    upload_date = meta.get("upload_date", "")
    if upload_date:
        try:
            dt = datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=timezone.utc)
            iso_date = dt.isoformat()
        except ValueError:
            pass

    return {
        "video_id": video_id,
        "title": meta.get("title", "")[:100],
        "description": meta.get("description", ""),
        "keywords": meta.get("tags", []),
        "thumbnail": best_thumb,
        "published": iso_date,
        "youtube_url": f"https://www.youtube.com/watch?v={video_id}",
        "source_item_id": video_id,
    }


# ---------------------------------------------------------------------------
# MRSS feed parsing
# ---------------------------------------------------------------------------


def parse_mrss_feed(feed_path: str) -> list[dict]:
    """Parse MRSS feed and return video metadata."""
    tree = etree.parse(feed_path)
    root = tree.getroot()
    videos = []

    for item in root.findall(".//item"):
        guid = item.findtext("guid", "")
        link = item.findtext("link", "")
        url = guid or link

        video_id_match = re.search(r"(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})", url)
        if not video_id_match:
            continue
        video_id = video_id_match.group(1)

        ns = {"media": MRSS_NS}
        group = item.find("media:group", ns)

        keywords = []
        if group is not None:
            kw_el = group.find("media:keywords", ns)
            if kw_el is not None and kw_el.text:
                keywords = [k.strip() for k in kw_el.text.split(",") if k.strip()]

        thumbnail = ""
        if group is not None:
            thumbs = group.findall("media:thumbnail", ns)
            if thumbs:
                thumbnail = thumbs[0].get("url", "")

        pub_date = item.findtext("pubDate", "")
        iso_date = ""
        if pub_date:
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(pub_date)
                iso_date = dt.isoformat()
            except Exception:
                pass

        videos.append({
            "video_id": video_id,
            "title": item.findtext("title", ""),
            "description": item.findtext("description", ""),
            "keywords": keywords,
            "thumbnail": thumbnail,
            "published": iso_date,
            "youtube_url": url,
            "source_item_id": video_id,
        })

    return videos


# ---------------------------------------------------------------------------
# Tracking
# ---------------------------------------------------------------------------


def load_tracking(path: str) -> dict:
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_tracking(path: str, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Main sync workflow
# ---------------------------------------------------------------------------


def sync_video(video: dict, jwt: str, account_id: str, gh_token: str, gh_repo: str,
               cookies_from_browser: str = None) -> dict:
    """
    Full sync pipeline for a single video:
    1. Download from YouTube
    2. Upload to GitHub release
    3. Create in Connatix with the GitHub URL
    4. Clean up local file
    """
    vid = video["video_id"]
    local_file = None

    try:
        # Step 1: Download
        print("  Downloading from YouTube...")
        local_file = download_video(vid, cookies_from_browser=cookies_from_browser)
        file_size_mb = os.path.getsize(local_file) / (1024 * 1024)
        print(f"  Downloaded: {file_size_mb:.1f} MB")

        # Step 2: Upload to GitHub release
        print("  Creating GitHub release...")
        release = create_github_release(gh_token, gh_repo, vid)
        print("  Uploading to GitHub...")
        public_url = upload_release_asset(gh_token, release, local_file)
        print(f"  Staged at: {public_url[:80]}...")

        # Step 3: Create in Connatix
        video["video_url"] = public_url
        print("  Creating in Connatix...")
        result = create_media(jwt, account_id, video)

        if not result.get("success"):
            raise ValueError(f"Connatix create failed: {result}")

        cnx_id = result.get("objectId", "unknown")
        print(f"  Success! Connatix ID: {cnx_id}")

        return {
            "connatix_id": cnx_id,
            "youtube_published": video.get("published", ""),
            "release_id": release["id"],
            "release_tag": release["tag_name"],
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "title": video["title"],
        }

    finally:
        # Always clean up local file
        if local_file and os.path.exists(local_file):
            os.remove(local_file)
            print("  Cleaned up local file.")


def format_eta(seconds: float) -> str:
    """Format seconds into human-readable time."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.0f}m"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h {m}m"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Sync YouTube videos to Connatix via GitHub staging."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--video-id", help="Sync a single YouTube video by ID.")
    group.add_argument("--feed", help="Path to MRSS feed XML. Syncs new videos.")
    group.add_argument("--gaps", help="Path to gaps file (one video ID per line). Requires --feed for metadata.")
    group.add_argument("--cleanup", action="store_true",
                       help="Clean up GitHub releases for previously synced videos.")
    parser.add_argument("--feed-for-gaps", dest="feed_for_gaps",
                        help="Path to MRSS feed for metadata lookup (used with --gaps).")
    parser.add_argument("--cookies-from-browser",
                        help="Browser to extract cookies from (e.g. chrome, firefox, edge). "
                             "Fixes YouTube bot detection / 'Sign in to confirm' errors.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would sync.")
    parser.add_argument("--tracking-file", default=TRACKING_FILE)
    args = parser.parse_args()

    # --- Validate env vars ---
    email = os.environ.get("CONNATIX_EMAIL", "")
    password = os.environ.get("CONNATIX_PASSWORD", "")
    gh_token = os.environ.get("GITHUB_TOKEN", "")
    gh_repo = os.environ.get("GITHUB_REPO", "")

    if not email or not password:
        print("Error: Set CONNATIX_EMAIL and CONNATIX_PASSWORD.", file=sys.stderr)
        sys.exit(1)
    if not gh_token or not gh_repo:
        print("Error: Set GITHUB_TOKEN and GITHUB_REPO.", file=sys.stderr)
        sys.exit(1)

    # --- Auth ---
    print("Authenticating with Connatix...")
    jwt = get_jwt(email, password)

    account_id = os.environ.get("CONNATIX_ACCOUNT_ID", "")
    if not account_id:
        print("Fetching account ID...")
        account_id = get_account_id(jwt)
        print(f"  Account ID: {account_id}")

    if args.cookies_from_browser:
        print(f"Using cookies from: {args.cookies_from_browser}")

    # --- Cleanup mode ---
    if args.cleanup:
        tracking = load_tracking(args.tracking_file)
        cleaned = 0
        for vid, entry in tracking.items():
            if entry.get("release_id") and not entry.get("cleaned"):
                print(f"Cleaning up {vid}: {entry.get('title', '')[:50]}...")
                try:
                    delete_github_release(
                        gh_token, gh_repo,
                        entry["release_id"], entry["release_tag"]
                    )
                    entry["cleaned"] = True
                    cleaned += 1
                    print("  Done.")
                except Exception as e:
                    print(f"  Error: {e}")
        save_tracking(args.tracking_file, tracking)
        print(f"Cleaned up {cleaned} releases.")
        return

    # --- Build video list ---
    if args.video_id:
        print(f"Fetching metadata for {args.video_id}...")
        videos = [get_video_metadata(args.video_id, cookies_from_browser=args.cookies_from_browser)]
        print(f"  Title: {videos[0]['title']}")

    elif args.gaps:
        # Gaps mode: read video IDs from file, get metadata from feed or yt-dlp
        with open(args.gaps) as f:
            gap_ids = [line.strip() for line in f if line.strip()]
        print(f"Loaded {len(gap_ids)} video IDs from {args.gaps}")

        # Filter against current Connatix library — anything already there
        # gets skipped regardless of tracking-file state.
        print("Fetching current Connatix library to filter already-synced gaps...")
        try:
            existing_ids = get_existing_connatix_youtube_ids(jwt, account_id)
        except Exception as e:
            print(f"Error: Could not fetch Connatix library: {e}", file=sys.stderr)
            sys.exit(2)
        print(f"  Connatix already has {len(existing_ids)} YouTube-sourced videos.")

        gap_ids = [vid for vid in gap_ids if vid not in existing_ids]
        print(f"  {len(gap_ids)} remaining after skipping already-in-Connatix")

        if not gap_ids:
            print("Nothing to sync — all gaps already filled.")
            return

        # Try to get metadata from MRSS feed first
        feed_path = args.feed_for_gaps
        feed_lookup = {}
        if feed_path:
            print(f"Loading metadata from feed: {feed_path}")
            feed_videos = parse_mrss_feed(feed_path)
            feed_lookup = {v["video_id"]: v for v in feed_videos}
            print(f"  {len(feed_lookup)} videos in feed")

        videos = []
        yt_dlp_needed = 0
        for vid in gap_ids:
            if vid in feed_lookup:
                videos.append(feed_lookup[vid])
            else:
                yt_dlp_needed += 1
                # Will fetch via yt-dlp during sync
                videos.append({"video_id": vid, "_needs_metadata": True})

        if yt_dlp_needed:
            print(f"  {yt_dlp_needed} videos not in feed — will fetch metadata from YouTube")
        if feed_lookup:
            print(f"  {len(videos) - yt_dlp_needed} videos matched in feed")

    else:
        # Feed mode: diff MRSS feed against Connatix library directly.
        # Replaces the old tracking-file diff which could drift out of sync
        # with reality. Connatix is the source of truth — if it's in the
        # library, we don't need to sync it; if it's not, we do.
        print(f"Parsing feed: {args.feed}")
        videos = parse_mrss_feed(args.feed)
        print(f"  Found {len(videos)} videos in feed.")

        print("Fetching current Connatix library to determine what's already synced...")
        try:
            existing_ids = get_existing_connatix_youtube_ids(jwt, account_id)
        except Exception as e:
            print(f"Error: Could not fetch Connatix library — refusing to sync without"
                  f" a reliable baseline. Details: {e}", file=sys.stderr)
            sys.exit(2)
        print(f"  Connatix already has {len(existing_ids)} YouTube-sourced videos.")

        new_videos = [v for v in videos if v["video_id"] not in existing_ids]
        print(f"  {len(new_videos)} videos in feed not yet in Connatix.")

        # Hard guard: if we're about to push more than 50 in a single run,
        # something is wrong (library fetch returned partial, MRSS regenerated
        # incorrectly, etc). Real daily deltas are 0–5 videos. Bail loudly
        # rather than mass-upload duplicates.
        if len(new_videos) > 50:
            print(
                f"\nABORT: {len(new_videos)} new videos exceeds safety ceiling of 50."
                f" This usually means the Connatix library fetch returned partial"
                f" data, or the MRSS feed has been regenerated against the wrong"
                f" channel. Investigate before re-running.\n",
                file=sys.stderr
            )
            sys.exit(3)

        videos = new_videos

    if not videos:
        print("Nothing to sync.")
        return

    if args.dry_run:
        print(f"\nDry run — would sync {len(videos)} videos:")
        for v in videos:
            title = v.get("title", "(metadata from YouTube)")
            print(f"  {v['video_id']}: {title}")
        est_time = len(videos) * 25
        print(f"\nEstimated time: {format_eta(est_time)}")
        return

    # --- Sync ---
    tracking = load_tracking(args.tracking_file)
    success_count = 0
    fail_count = 0
    times = []  # Track per-video times for ETA

    total = len(videos)
    print(f"\nSyncing {total} videos...")
    print(f"Estimated time: {format_eta(total * 25)}\n")

    for i, video in enumerate(videos, 1):
        vid = video["video_id"]
        start_time = time.time()

        # Fetch metadata from YouTube if not in feed
        if video.get("_needs_metadata"):
            print(f"\n[{i}/{total}] {vid}: fetching metadata...")
            try:
                video = get_video_metadata(vid, cookies_from_browser=args.cookies_from_browser)
            except Exception as e:
                print(f"  Error fetching metadata: {e}")
                fail_count += 1
                continue

        print(f"\n[{i}/{total}] {vid}: {video['title'][:60]}...")

        # ETA calculation
        if times:
            avg_time = sum(times) / len(times)
            remaining = total - i
            eta = format_eta(avg_time * remaining)
            print(f"  ETA for remaining {remaining}: {eta}")

        try:
            result = sync_video(video, jwt, account_id, gh_token, gh_repo,
                                cookies_from_browser=args.cookies_from_browser)
            tracking[vid] = result
            save_tracking(args.tracking_file, tracking)
            success_count += 1
        except Exception as e:
            print(f"  Error: {e}")
            tracking[vid] = {
                "error": str(e),
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "title": video.get("title", ""),
            }
            save_tracking(args.tracking_file, tracking)
            fail_count += 1

        elapsed = time.time() - start_time
        times.append(elapsed)

    print(f"\n{'='*50}")
    print(f"Done. {success_count} synced, {fail_count} failed.")
    print(f"Total time: {format_eta(sum(times))}")
    if success_count:
        print("Run with --cleanup later to remove GitHub releases after Connatix has encoded.")


if __name__ == "__main__":
    main()
