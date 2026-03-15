#!/usr/bin/env python3
"""
IPTV Playlist Updater
Automatically updates M3U playlist URLs from upstream sources while preserving metadata
"""

import re
import sys
import urllib.request
import urllib.error
import ssl
import unicodedata
import difflib
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed


# =========================
# Matching helpers (NEW)
# =========================

NOISE_PATTERNS = [
    r'^\s*\d+\s*[\.\-\)]\s*',                 # "0. Duronto TV"
    r'^\s*\[bd\]\s*',                         # "[BD] Ekhon TV"
    r'^\s*bd\s*\|\s*',                        # "BD | JAMUNA TV"
    r'^\s*news\s*\|\s*',                      # "NEWS | News24"
    r'^\s*mu\s*\|\s*',                        # "MU | SANGEET BANGLA"
    r'^\s*mov\s*\|\s*',                       # "MOV | Manoranjan Movies"
    r'^\s*in\s*\|\s*',                        # "IN | TV9 Bangla"
    r'\(ads\s*only\)',                        # "(Ads Only)"
    r'\(tplay\)',                             # "(Tplay)"
    r'\(480p\)|\(720p\)|\(1080p\)',           # "(720p)"
    r'\b(uhd|fhd|hd|sd|4k|1080p|720p|480p)\b',
    r'\bbackup\b|\balt\b|\bmirror\b',         # generic backup words
    r'\-\[\d+\]|\(\d+\)',                     # "-[2]" "(3)"
    r'[\U00010000-\U0010ffff]',               # emoji
]

# tokens that must remain because they separate channels (Channel I vs 9 vs S)
PROTECTED_TOKENS = {"i", "s", "9", "24"}

# remove these generic fillers (but keep "news24" because it's "news24" token)
STOPWORDS = {"tv", "television", "channel", "network", "bangladesh", "bd"}

# small, high-signal aliases based on your real sources
ALIASES = {
    "ekhon": {"ekhon tv"},
    "ekhon tv": {"ekhon"},

    "somoy": {"somoy tv", "somoy news", "somoy news tv"},
    "somoy tv": {"somoy", "somoy news", "somoy news tv"},

    "bangla vision": {"banglavision", "banglavision tv"},
    "banglavision": {"bangla vision", "banglavision tv"},

    "dbc": {"dbc news"},
    "dbc news": {"dbc"},

    "t sports": {"tsport", "t sports hd"},
    "tsport": {"t sports", "t sports hd"},
}


def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def canonicalize(raw: str) -> str:
    if not raw:
        return ""
    s = _strip_accents(raw).lower().strip()

    # normalize separators
    s = s.replace("&", " and ")
    s = s.replace("—", "-").replace("–", "-")
    s = s.replace("|", " | ")

    # strip patterns
    for pat in NOISE_PATTERNS:
        s = re.sub(pat, " ", s, flags=re.IGNORECASE)

    # remove bracket blocks
    s = re.sub(r"\[[^\]]*\]", " ", s)

    # punctuation → spaces
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    toks: List[str] = []
    for t in s.split():
        if t in PROTECTED_TOKENS:
            toks.append(t)
            continue
        if t in STOPWORDS:
            continue
        toks.append(t)

    return " ".join(toks).strip()


def tokenize(raw: str) -> List[str]:
    c = canonicalize(raw)
    return c.split() if c else []


def expand_names(raw: str) -> Set[str]:
    base = canonicalize(raw)
    out = {base} if base else set()
    # one hop alias expansion
    for a in list(out):
        for b in ALIASES.get(a, set()):
            out.add(canonicalize(b))
    return {x for x in out if x}


def looks_like_backup(name: str) -> bool:
    n = (name or "").lower()
    return bool(re.search(r"\bbackup\b|\balt\b|\bmirror\b|\-\[\d+\]|\(\d+\)", n))


def score_match(cur_name: str, up_name: str, cur_logo: str, up_logo: str) -> float:
    ta, tb = set(tokenize(cur_name)), set(tokenize(up_name))
    if not ta or not tb:
        return 0.0

    # hard block: protect single-token collisions like "i" vs "9" vs "s"
    if ta <= PROTECTED_TOKENS and tb <= PROTECTED_TOKENS and ta != tb:
        return -10.0

    jacc = len(ta & tb) / max(1, len(ta | tb))
    seq = difflib.SequenceMatcher(None, canonicalize(cur_name), canonicalize(up_name)).ratio()

    extra = len(tb - ta)
    penalty = 0.06 * extra

    logo_bonus = 0.03 if (cur_logo and up_logo and cur_logo == up_logo) else 0.0

    return (0.55 * seq + 0.45 * jacc + logo_bonus) - penalty


# =========================
# Data model
# =========================

@dataclass
class Channel:
    """Represents a single channel entry in an M3U playlist"""
    extinf_line: str  # Full #EXTINF line with metadata
    url: str  # Stream URL
    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    group_title: str = ""
    channel_name: str = ""  # Name after comma in EXTINF
    is_commented: bool = False
    raw_lines: List[str] = field(default_factory=list)  # Original lines including comments
    source_rank: int = 999  # Upstream priority: 0 = highest, assigned during loading

    def __post_init__(self):
        """Extract metadata from EXTINF line"""
        if not self.extinf_line:
            return

        tvg_id_match = re.search(r'tvg-id="([^"]*)"', self.extinf_line)
        if tvg_id_match:
            self.tvg_id = tvg_id_match.group(1)

        tvg_name_match = re.search(r'tvg-name="([^"]*)"', self.extinf_line)
        if tvg_name_match:
            self.tvg_name = tvg_name_match.group(1)

        tvg_logo_match = re.search(r'tvg-logo="([^"]*)"', self.extinf_line)
        if tvg_logo_match:
            self.tvg_logo = tvg_logo_match.group(1)

        group_match = re.search(r'group-title="([^"]*)"', self.extinf_line)
        if group_match:
            self.group_title = group_match.group(1)

        if ',' in self.extinf_line:
            self.channel_name = self.extinf_line.split(',', 1)[1].strip()

    def get_match_keys(self) -> List[str]:
        """Return multiple keys used for matching (id + expanded names + logo)"""
        keys: List[str] = []

        if self.tvg_id and self.tvg_id != "(no tvg-id)(m3u4u)":
            keys.append(f"id:{self.tvg_id.strip().lower()}")

        for nm in [self.tvg_name, self.channel_name]:
            if nm:
                for e in expand_names(nm):
                    keys.append(f"name:{e}")

        if self.tvg_logo:
            keys.append(f"logo:{self.tvg_logo.strip().lower()}")

        # de-dupe keep order
        seen, out = set(), []
        for k in keys:
            if k not in seen:
                seen.add(k)
                out.append(k)
        return out

    def get_primary_key(self) -> str:
        """Stable key for de-duping curated list"""
        for k in self.get_match_keys():
            if k.startswith("id:"):
                return k
        for k in self.get_match_keys():
            if k.startswith("name:"):
                return k
        return "name:" + canonicalize(self.channel_name or self.tvg_name or "")

    def display_name(self) -> str:
        return self.channel_name or self.tvg_name or ""

    def to_m3u_lines(self) -> List[str]:
        """Convert channel back to M3U format lines"""
        lines: List[str] = []

        for line in self.raw_lines:
            if line.startswith('#') and not line.startswith('#EXTINF'):
                lines.append(line)

        lines.append(self.extinf_line)

        if self.url:
            normalized_url = re.sub(
                r'(?:\s*#\s*disabled:\s*unreachable)+',
                ' # disabled: unreachable',
                self.url,
                flags=re.IGNORECASE,
            ).strip()

            if self.is_commented:
                lines.append(f"#{normalized_url.lstrip('#')}")
            else:
                lines.append(normalized_url)

        return lines


class M3UParser:
    """Parse M3U playlist files with robust error handling"""

    @staticmethod
    def parse(content: str) -> List[Channel]:
        channels: List[Channel] = []
        lines = content.split('\n')

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            if not line or line == '#EXTM3U':
                i += 1
                continue

            comment_lines: List[str] = []
            while i < len(lines) and lines[i].strip().startswith('#') and not lines[i].strip().startswith('#EXTINF'):
                comment_lines.append(lines[i].strip())
                i += 1

            if i < len(lines) and lines[i].strip().startswith('#EXTINF'):
                extinf_line = lines[i].strip()
                i += 1

                url = ""
                is_commented = False
                url_comment_lines: List[str] = []

                while i < len(lines):
                    url_line = lines[i].strip()

                    if not url_line:
                        i += 1
                        if not url:
                            continue
                        break

                    if url_line.startswith('#http'):
                        if not url:
                            url = url_line[1:]
                            is_commented = True
                        else:
                            url_comment_lines.append(url_line)
                        i += 1
                    elif url_line.startswith('http'):
                        if not url:
                            url = url_line
                            is_commented = False
                        i += 1
                        break
                    elif url_line.startswith('#EXTINF'):
                        break
                    elif url_line.startswith('#'):
                        url_comment_lines.append(url_line)
                        i += 1
                    else:
                        i += 1
                        break

                channel = Channel(
                    extinf_line=extinf_line,
                    url=url if url else "",
                    is_commented=is_commented if url else True,
                    raw_lines=comment_lines + url_comment_lines
                )
                channels.append(channel)
            else:
                i += 1

        return channels


class PlaylistUpdater:
    """Main class for updating playlists"""

    def __init__(self, curated_playlist_path: str, upstream_urls: List[str]):
        self.curated_playlist_path = curated_playlist_path
        self.upstream_urls = upstream_urls
        self.timeout = 10

    def fetch_url(self, url: str) -> Optional[str]:
        try:
            print(f"Fetching: {url}")
            req = urllib.request.Request(
                url,
                headers={'User-Agent': 'Mozilla/5.0 (IPTV-Updater/1.0)'}
            )
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

            with urllib.request.urlopen(req, timeout=self.timeout, context=context) as response:
                content = response.read().decode('utf-8', errors='ignore')
                print(f"✓ Fetched {len(content)} bytes")
                return content
        except Exception as e:
            print(f"✗ Failed to fetch {url}: {e}")
            return None

    @staticmethod
    def strip_inline_markers(url: str) -> str:
        """Return a clean URL without leading # or inline annotations."""
        if not url:
            return ""

        cleaned = url.strip()
        if cleaned.startswith('#'):
            cleaned = cleaned[1:].strip()

        # Remove any inline comments like " # disabled: unreachable" or " # LOCK"
        cleaned = re.split(r'\s+#', cleaned, maxsplit=1)[0].strip()
        return cleaned

    def validate_stream_url(self, url: str) -> bool:
        url = self.strip_inline_markers(url)

        if not url:
            return False

        try:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

            req = urllib.request.Request(url, method='HEAD')
            req.add_header('User-Agent', 'Mozilla/5.0 (IPTV-Updater/1.0)')
            with urllib.request.urlopen(req, timeout=5, context=context) as response:
                return response.status == 200
        except:
            try:
                req = urllib.request.Request(url)
                req.add_header('User-Agent', 'Mozilla/5.0 (IPTV-Updater/1.0)')
                req.add_header('Range', 'bytes=0-1024')
                with urllib.request.urlopen(req, timeout=5, context=context) as response:
                    return response.status in (200, 206)
            except:
                return False

    def validate_urls_concurrent(self, channels: List[Channel]) -> Dict[str, bool]:
        print(f"  Validating {len(channels)} URLs concurrently...")
        results: Dict[str, bool] = {}

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_channel = {
                executor.submit(self.validate_stream_url, ch.url): ch
                for ch in channels if ch.url
            }

            completed = 0
            for future in as_completed(future_to_channel):
                channel = future_to_channel[future]
                try:
                    is_working = future.result()
                    results[channel.url] = is_working
                    completed += 1
                    if completed % 10 == 0:
                        print(f"    Progress: {completed}/{len(channels)}")
                except Exception:
                    results[channel.url] = False

        working_count = sum(1 for v in results.values() if v)
        print(f"  ✓ {working_count}/{len(results)} URLs are working")
        return results

    def load_curated_playlist(self) -> List[Channel]:
        print(f"\nLoading curated playlist: {self.curated_playlist_path}")
        try:
            with open(self.curated_playlist_path, 'r', encoding='utf-8') as f:
                content = f.read()
            channels = M3UParser.parse(content)
            print(f"✓ Loaded {len(channels)} channels from curated playlist")
            return channels
        except Exception as e:
            print(f"✗ Failed to load curated playlist: {e}")
            sys.exit(1)

    def load_upstream_channels(self) -> Dict[str, List[Channel]]:
        """Load all upstream playlists and create a lookup map with ALL matches (multi-key)"""
        print("\n=== Loading Upstream Playlists ===")
        upstream_map: Dict[str, List[Channel]] = {}

        for rank, url in enumerate(self.upstream_urls):
            content = self.fetch_url(url)
            if not content:
                continue

            channels = M3UParser.parse(content)
            print(f"Parsed {len(channels)} channels from upstream (priority rank: {rank})")

            for channel in channels:
                if not channel.url:
                    continue
                channel.source_rank = rank

                for key in channel.get_match_keys():
                    upstream_map.setdefault(key, []).append(channel)

        for key in upstream_map:
            upstream_map[key].sort(key=lambda c: c.source_rank)

        total_channels = sum(len(v) for v in upstream_map.values())
        print(f"\n✓ Total upstream channel entries: {total_channels} ({len(upstream_map)} keys)")
        return upstream_map

    def is_locked_disabled(self, channel: Channel) -> bool:
        if channel.url and '# LOCK' in channel.url.upper():
            return True
        for line in channel.raw_lines:
            if '# LOCK' in line.upper():
                return True
        return False

    def _collect_candidates(self, curated: Channel, upstream_map: Dict[str, List[Channel]]) -> List[Channel]:
        cands: List[Channel] = []
        for k in curated.get_match_keys():
            cands.extend(upstream_map.get(k, []))

        # unique by URL
        seen_urls: Set[str] = set()
        uniq: List[Channel] = []
        for c in cands:
            if c.url and c.url not in seen_urls:
                seen_urls.add(c.url)
                uniq.append(c)
        return uniq

    def _pick_best_upstream(
        self,
        curated: Channel,
        candidates: List[Channel],
        url_validation_cache: Dict[str, bool],
        validate_urls: bool
    ) -> Optional[Channel]:
        if not candidates:
            return None

        cur_name = curated.display_name()
        cur_logo = curated.tvg_logo or ""

        scored = []
        for up in candidates:
            up_name = up.display_name()
            up_logo = up.tvg_logo or ""
            s = score_match(cur_name, up_name, cur_logo, up_logo)

            # working links first when validation is enabled
            if validate_urls:
                working = url_validation_cache.get(up.url, False)
                working_penalty = 0 if working else 1   # 0=good, 1=bad
            else:
                working_penalty = 0

            # Add only primitives before the Channel object so sorting never compares Channel
            # Sort priority:
            #   1) working first (0 before 1)
            #   2) upstream rank (lower first)
            #   3) match score (higher first)
            #   4) shorter URL (usually cleaner tokens)
            #   5) stable tie-breaker by name
            scored.append((
                working_penalty,
                int(up.source_rank or 999),
                float(s),
                int(len(up.url or "")),
                (up.display_name() or "").lower(),
                up
            ))

        scored.sort(key=lambda t: (t[0], t[1], -t[2], t[3], t[4]))

        best = scored[0][5]
        best_score = scored[0][2]

        # If score is too low, treat as no match (prevents random collisions)
        if best_score < 0.25:
            return None

        return best

    def update_channels(
        self,
        curated_channels: List[Channel],
        upstream_map: Dict[str, List[Channel]],
        validate_urls: bool = False
    ) -> Tuple[List[Channel], int]:
        print("\n=== Updating Channels ===")

        url_validation_cache: Dict[str, bool] = {}
        if validate_urls:
            all_upstream_channels: List[Channel] = []
            for channels_list in upstream_map.values():
                all_upstream_channels.extend(channels_list)
            url_validation_cache = self.validate_urls_concurrent(all_upstream_channels)

        updated_count = 0
        added_count = 0
        backup_added_count = 0
        updated_channels: List[Channel] = []

        seen_keys: Set[str] = set()

        for channel in curated_channels:
            primary_key = channel.get_primary_key()

            if primary_key in seen_keys:
                print(f"⊘ Skipping duplicate: {channel.display_name()}")
                continue
            seen_keys.add(primary_key)

            is_backup = looks_like_backup(channel.display_name())

            candidates = self._collect_candidates(channel, upstream_map)
            best_upstream = self._pick_best_upstream(channel, candidates, url_validation_cache, validate_urls)

            if best_upstream:
                current_url_clean = self.strip_inline_markers(channel.url)

                # If channel has no URL, add
                if not channel.url:
                    print(f"✓ Adding URL: {channel.display_name()}")
                    print(f"  New: {best_upstream.url[:80] if len(best_upstream.url) > 80 else best_upstream.url}")
                    if len(candidates) > 1:
                        print(f"  ({len(candidates)} alternatives available)")
                    channel.url = best_upstream.url
                    channel.is_commented = False
                    added_count += 1

                # Update if different and not locked
                elif current_url_clean != self.strip_inline_markers(best_upstream.url):
                    if not self.is_locked_disabled(channel):
                        print(f"↻ Updating: {channel.display_name()}")
                        print(f"  Old: {channel.url[:80]}...")
                        print(f"  New: {best_upstream.url[:80]}...")
                        if len(candidates) > 1:
                            print(f"  ({len(candidates)} alternatives available)")
                        channel.url = best_upstream.url
                        channel.is_commented = False
                        updated_count += 1
                    else:
                        print(f"⊘ Keeping (manually locked): {channel.display_name()}")
            else:
                if not channel.url:
                    print(f"⊘ Waiting for source: {channel.display_name()}")
                elif self.is_locked_disabled(channel):
                    print(f"⊘ Keeping (manually locked): {channel.display_name()}")
                else:
                    print(f"ℹ No upstream match: {channel.display_name()}")

            # Validate URL if requested (only for channels with active URLs that aren't locked)
            if validate_urls and channel.url and not self.is_locked_disabled(channel):
                if not self.validate_stream_url(channel.url):
                    print(f"⚠ Unreachable: {channel.display_name()}")

                    # try alternatives among candidates (skip current)
                    current_url_clean = self.strip_inline_markers(channel.url)
                    alts = [
                        c for c in candidates
                        if c.url and self.strip_inline_markers(c.url) != current_url_clean
                    ]
                    alts_sorted = sorted(
                        alts,
                        key=lambda u: (
                            0 if url_validation_cache.get(u.url, False) else 1,
                            u.source_rank,
                            -score_match(channel.display_name(), u.display_name(), channel.tvg_logo or "", u.tvg_logo or "")
                        )
                    )

                    switched = False
                    for alt_idx, up in enumerate(alts_sorted[:10], 1):
                        print(f"    [{alt_idx}] Testing: {up.url[:60]}...")
                        if self.validate_stream_url(up.url):
                            print(f"    ✓ Found working alternative!")
                            channel.url = up.url
                            channel.is_commented = False
                            updated_count += 1
                            switched = True
                            break

                    if not switched:
                        print(f"    ✗ No working alternatives found")
                        channel.is_commented = True
                        current_url_clean = self.strip_inline_markers(channel.url)
                        if current_url_clean:
                            channel.url = f"{current_url_clean} # disabled: unreachable"
                        else:
                            channel.url = ""

            updated_channels.append(channel)

            # Auto-create backups if multiple alternatives exist and curated entry isn't already a backup
            if (not is_backup) and candidates and len(candidates) > 1:
                base_name = channel.display_name()

                existing_backup_names = [
                    c.display_name() for c in curated_channels
                    if base_name.lower() in (c.display_name() or "").lower()
                    and looks_like_backup(c.display_name())
                ]

                available_alternatives = [c for c in candidates if c.url and c.url != (best_upstream.url if best_upstream else "")]
                max_backups = min(2, len(available_alternatives))

                if len(existing_backup_names) == 0 and max_backups > 0:
                    for backup_idx in range(max_backups):
                        backup_num = backup_idx + 1
                        backup_suffix = f" (Backup {backup_num})" if backup_num > 1 else " (Backup)"

                        backup_channel = Channel(
                            extinf_line=channel.extinf_line.replace(f',{base_name}', f',{base_name}{backup_suffix}'),
                            url=available_alternatives[backup_idx].url,
                            is_commented=False,
                            raw_lines=[],
                            source_rank=available_alternatives[backup_idx].source_rank
                        )
                        print(f"  ➕ Adding backup: {base_name}{backup_suffix} (from upstream rank {backup_channel.source_rank})")
                        print(f"     URL: {backup_channel.url[:70]}...")
                        updated_channels.append(backup_channel)
                        backup_added_count += 1

        print(f"\n✓ Kept {len(updated_channels)} channels ({added_count} URLs added, {updated_count} URLs updated, {backup_added_count} backups created)")
        return updated_channels, updated_count + added_count + backup_added_count

    def write_playlist(self, channels: List[Channel], output_path: str):
        print(f"\n=== Writing Playlist ===")
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")

                for channel in channels:
                    for line in channel.to_m3u_lines():
                        f.write(line + '\n')
                    f.write('\n')

            print(f"✓ Written {len(channels)} channels to {output_path}")
        except Exception as e:
            print(f"✗ Failed to write playlist: {e}")
            sys.exit(1)

    def run(self, validate_urls: bool = False) -> bool:
        print("=" * 60)
        print("IPTV Playlist Updater")
        print("=" * 60)

        curated_channels = self.load_curated_playlist()
        upstream_map = self.load_upstream_channels()

        updated_channels, update_count = self.update_channels(
            curated_channels,
            upstream_map,
            validate_urls
        )

        self.write_playlist(updated_channels, self.curated_playlist_path)

        print("\n" + "=" * 60)
        print(f"✓ Update Complete - {update_count} URLs updated")
        print("=" * 60)

        return update_count > 0


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Update IPTV playlist from upstream sources'
    )
    parser.add_argument(
        '--playlist',
        default='my',
        help='Path to curated playlist file (default: my)'
    )
    parser.add_argument(
        '--upstream',
        nargs='+',
        default=[
            'https://raw.githubusercontent.com/sydul104/main04/refs/heads/main/my',
            'https://raw.githubusercontent.com/musfiqeee/iptv-m3u-bot/main/output/all.m3u'
        ],
        help='Upstream playlist URLs'
    )
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate stream URLs (slower but recommended)'
    )
    parser.add_argument(
        '--no-validate',
        action='store_true',
        help='Skip URL validation (faster)'
    )

    args = parser.parse_args()

    validate = args.validate
    if args.no_validate:
        validate = False

    updater = PlaylistUpdater(args.playlist, args.upstream)
    has_changes = updater.run(validate_urls=validate)

    sys.exit(0 if has_changes else 1)


if __name__ == '__main__':
    main()