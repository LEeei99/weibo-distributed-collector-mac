"""Best-effort, anonymous public Weibo post enrichment.

The web endpoints used here are undocumented and may change. A visitor/session
gate is reported separately from deletion. This module never solves challenges,
extracts browser credentials, or infers check-ins from text or IP-region labels.
Only explicit media URLs already returned by Weibo are selected.
"""
from __future__ import annotations

import gzip
import html as html_lib
import io
import ipaddress
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from weibo_locations import has_coordinates

MAX_RESPONSE_BYTES = 8 * 1024 * 1024
POST_HOSTS = {"weibo.com", "www.weibo.com", "m.weibo.com", "m.weibo.cn", "www.weibo.cn", "weibo.cn"}
POST_TOKEN = re.compile(r"^[A-Za-z0-9]{5,30}$")


def post_token(url: str) -> str:
    """Read a status ID/bid from an exported post URL; do not fetch arbitrary URLs."""
    parsed = urllib.parse.urlsplit(url.strip())
    if parsed.scheme not in ("http", "https") or (parsed.hostname or "").lower() not in POST_HOSTS:
        raise ValueError("Expected an http(s) Weibo post URL")
    path = [urllib.parse.unquote(x) for x in parsed.path.split("/") if x]
    if path and path[0].lower() == "ttarticle":
        raise ValueError("Weibo article URL is not a status URL (unsupported_article)")
    query = urllib.parse.parse_qs(parsed.query)
    token = None
    if len(path) >= 2 and path[-2] in ("detail", "status", "statuses", "comment"):
        token = path[-1]
    elif len(path) == 2 and path[0].isdigit():
        token = path[1]
    elif path and path[-1] in ("show", "show.json"):
        token = (query.get("id") or query.get("mid") or [None])[0]
    elif not path and query.get("id"):
        token = query["id"][0]
    if not isinstance(token, str) or not POST_TOKEN.fullmatch(token):
        raise ValueError("URL does not identify a supported Weibo status")
    return token


def _url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = html_lib.unescape(value.strip())
    if value.startswith("//"):
        value = "https:" + value
    try:
        parsed = urllib.parse.urlsplit(value)
        hostname = (parsed.hostname or "").lower()
        if parsed.scheme not in ("http", "https") or not hostname or parsed.username or parsed.password:
            return None
        if hostname == "localhost" or hostname.endswith(".localhost") or hostname.endswith(".local"):
            return None
        try:
            if not ipaddress.ip_address(hostname).is_global:
                return None
        except ValueError:
            pass
        return value
    except ValueError:
        return None


def _number(value: Any) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def _identifier(status: dict) -> str:
    return str(status.get("idstr") or status.get("id") or status.get("mid") or "")


def _matches_source(status: dict, source_url: str | None) -> bool:
    if not source_url:
        return True
    try:
        token = post_token(source_url)
    except ValueError:
        return False
    known = {_identifier(status), str(status.get("mblogid") or ""), str(status.get("bid") or "")}
    if token in known:
        return True
    if not token.isdigit():
        alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        pieces, rest = [], token
        while rest:
            group, rest = rest[-4:], rest[:-4]
            number = 0
            for char in group:
                number = number * 62 + alphabet.index(char)
            pieces.append(str(number).zfill(7) if rest else str(number))
        return "".join(reversed(pieces)) in known
    return False


def _is_status(value: Any) -> bool:
    return isinstance(value, dict) and bool(_identifier(value)) and any(
        key in value for key in ("text", "text_raw", "pics", "pic_infos", "page_info", "user", "geo")
    )


def _unwrap_status(value: Any) -> dict | None:
    if _is_status(value):
        return value
    if isinstance(value, dict):
        for key in ("data", "status", "statusInfo", "mblog", "status_info"):
            child = value.get(key)
            if isinstance(child, dict):
                found = _unwrap_status(child)
                if found is not None:
                    return found
    return None


def _provenance(status: dict, role: str) -> dict:
    return {"source_post_id": _identifier(status),
            "source_bid": str(status.get("mblogid") or status.get("bid") or ""), "role": role}


def _candidate(url: Any, field: str, obj: dict | None = None) -> dict | None:
    normalized = _url(url)
    if not normalized:
        return None
    obj = obj or {}
    candidate = {"url": normalized, "field": field}
    for key in ("width", "height", "size", "bitrate", "quality_index", "quality_label"):
        if obj.get(key) is not None:
            candidate[key] = obj[key]
    return candidate


def _image_candidates(pic: Any, field: str) -> list[dict]:
    result = []
    if isinstance(pic, str):
        candidate = _candidate(pic, field)
        return [candidate] if candidate else []
    if not isinstance(pic, dict):
        return []
    # Each key here is an image rendition, rather than text/author/profile data.
    rendition_keys = ("original", "largest", "large", "mw2000", "mw1024", "bmiddle", "middle", "largecover", "thumbnail", "small")
    for key in rendition_keys:
        rendition = pic.get(key)
        if isinstance(rendition, dict):
            for url_key in ("url", "src"):
                candidate = _candidate(rendition.get(url_key), field + "." + key + "." + url_key, rendition)
                if candidate:
                    result.append(candidate)
        else:
            candidate = _candidate(rendition, field + "." + key, pic)
            if candidate:
                result.append(candidate)
    for key in ("url", "src", "original_url", "large_url"):
        candidate = _candidate(pic.get(key), field + "." + key, pic)
        if candidate:
            result.append(candidate)
    return result


def _unique_candidates(candidates: list[dict]) -> list[dict]:
    unique = {}
    for candidate in candidates:
        url = candidate["url"]
        if url not in unique:
            unique[url] = candidate
        elif _number(candidate.get("width")) * _number(candidate.get("height")) > (
            _number(unique[url].get("width")) * _number(unique[url].get("height"))
        ):
            unique[url] = candidate
    return list(unique.values())


def _image_score(candidate: dict) -> tuple:
    field = candidate["field"].lower()
    rank = 0
    for key, score in (("original", 100), ("largest", 95), ("mw2000", 90), ("mw1024", 80),
                       ("largecover", 25), ("large", 75), ("bmiddle", 50), ("middle", 40), ("thumbnail", 10), ("small", 5)):
        if key in field:
            rank = score
            break
    area = _number(candidate.get("width")) * _number(candidate.get("height"))
    # Actual pixel dimensions take precedence when supplied; named renditions
    # are used when the source omits dimensions. No URL is constructed/guessed.
    return (area if area > 0 else 0, rank, _number(candidate.get("size")))


def _image_media(status: dict, role: str) -> list[dict]:
    groups: dict[str, dict] = {}

    def add(pic: Any, field: str, identifier: Any = None):
        candidates = _image_candidates(pic, field)
        if not candidates:
            return
        if isinstance(pic, dict):
            identifier = pic.get("pid") or pic.get("pic_id") or pic.get("id") or identifier
        key = str(identifier) if identifier is not None else candidates[0]["url"]
        if key not in groups:
            groups[key] = {"kind": "image", "media_id": key, "candidates": [], **_provenance(status, role)}
        groups[key]["candidates"].extend(candidates)

    infos = status.get("pic_infos")
    if isinstance(infos, dict):
        order = status.get("pic_ids") or list(infos)
        for pid in list(order) + [pid for pid in infos if pid not in order]:
            add(infos.get(str(pid)), "pic_infos." + str(pid), pid)
    for index, pic in enumerate(status.get("pics") or []):
        fallback = (status.get("pic_ids") or [])[index:index + 1]
        add(pic, "pics." + str(index), fallback[0] if fallback else None)
    mix = status.get("mix_media_info") or {}
    if isinstance(mix, dict):
        for index, item in enumerate(mix.get("items") or []):
            if isinstance(item, dict) and str(item.get("type", "")).lower() in ("pic", "image", "photo", "1"):
                add(item.get("data") or item, "mix_media_info.items." + str(index), item.get("id"))
    result = []
    seen = set()
    for group in groups.values():
        candidates = _unique_candidates(group["candidates"])
        group["candidates"] = sorted(candidates, key=_image_score, reverse=True)
        group["url"] = group["candidates"][0]["url"]
        if group["url"] not in seen:
            seen.add(group["url"])
            result.append(group)
    return result


def _video_candidates(value: Any, field: str, depth: int = 0, inherited: dict | None = None) -> list[dict]:
    if depth > 8:
        return []
    candidates = []
    if isinstance(value, dict):
        metadata = dict(inherited or {})
        if isinstance(value.get("meta"), dict):
            metadata.update(value["meta"])
        metadata.update({k: value[k] for k in ("width", "height", "bitrate", "quality_index", "quality_label") if k in value})
        # This helper is called only within declared video/media objects.
        for key, child in value.items():
            name = field + "." + str(key)
            if isinstance(child, str) and (key in ("url", "src", "play_url", "playback_url") or
                    any(token in key.lower() for token in ("stream_url", "mp4", "m3u8"))):
                candidate = _candidate(child, name, metadata)
                if candidate:
                    target = urllib.parse.urlsplit(candidate["url"])
                    # A video page link is not a downloadable stream.
                    if ((target.hostname or "").lower() not in ("weibo.com", "www.weibo.com", "m.weibo.cn", "video.weibo.com")
                            and not target.path.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".heic"))):
                        candidates.append(candidate)
            elif isinstance(child, (dict, list)):
                # Cover media_info.urls, playback_list.play_info, and video_details.
                if key not in ("cover", "poster", "cover_info", "cover_image", "cover_images", "big_pic_info", "pic", "pic_big", "pic_middle", "pic_small", "picture", "thumbnail", "pic_info", "pic_infos", "user", "author"):
                    candidates.extend(_video_candidates(child, name, depth + 1, metadata))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            candidates.extend(_video_candidates(child, field + "." + str(index), depth + 1, inherited))
    return candidates


def _video_score(candidate: dict) -> tuple:
    field = (candidate["field"] + " " + str(candidate.get("quality_label", ""))).lower()
    resolution = _number(candidate.get("height"))
    for label, pixels in (("4k", 2160), ("2160", 2160), ("1080", 1080), ("720", 720),
                          ("480", 480), ("360", 360)):
        if label in field:
            resolution = max(resolution, pixels)
    if not resolution and any(label in field for label in ("_hd", ".hd", "high")):
        resolution = 720
    if not resolution and any(label in field for label in ("_sd", ".sd")):
        resolution = 360
    parsed = urllib.parse.urlsplit(candidate["url"])
    progressive = 0 if ".m3u8" in parsed.path.lower() or "m3u8" in field else 1
    return (resolution, _number(candidate.get("quality_index")), _number(candidate.get("bitrate")), progressive)


def _video_media(status: dict, role: str) -> list[dict]:
    groups = []

    def add(value: Any, field: str, identifier: Any = None):
        if not isinstance(value, dict):
            return
        media = value.get("media_info") if isinstance(value.get("media_info"), dict) else value
        candidates = _unique_candidates(_video_candidates(media, field + ".media_info"))
        if not candidates:
            return
        candidates.sort(key=_video_score, reverse=True)
        video = {"kind": "video", "media_id": str(value.get("object_id") or value.get("id") or identifier or ""),
                 "url": candidates[0]["url"], "candidates": candidates, **_provenance(status, role)}
        duration = media.get("duration") or value.get("duration")
        if duration is not None:
            video["duration"] = duration
        video["format"] = "hls" if ".m3u8" in urllib.parse.urlsplit(video["url"]).path.lower() else "progressive"
        groups.append(video)

    page = status.get("page_info")
    if isinstance(page, dict) and (page.get("media_info") or str(page.get("object_type", "")).lower() == "video"
                                  or str(page.get("type", "")) == "11"):
        add(page, "page_info")
    if isinstance(status.get("media_info"), dict):
        add({"media_info": status["media_info"]}, "status")
    mix = status.get("mix_media_info") or {}
    if isinstance(mix, dict):
        for index, item in enumerate(mix.get("items") or []):
            if isinstance(item, dict) and str(item.get("type", "")).lower() in ("video", "2"):
                add(item.get("data") or item, "mix_media_info.items." + str(index), item.get("id"))
    unique = {}
    for group in groups:
        unique.setdefault(group["url"], group)
    return list(unique.values())


_PLACE_TYPES = {"place", "poi", "location"}
_LOCATION_FIELDS = ("geo", "place", "poi", "location", "location_info", "poi_info")


def _canonical_poi(identifier: Any) -> str:
    raw = str(identifier)
    # Confirmed by public status fixtures: tag_struct.oid uses this namespace,
    # while geo.detail/annotations.place declare the bare POI identifier.
    match = re.fullmatch(r"(?:1022:)?100101(.+)", raw)
    return match.group(1) if match else raw


def _place_object(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if any(str(value.get(key, "")).lower() in _PLACE_TYPES for key in ("object_type", "otype", "page_type")):
        return True
    # Namespace 100101 is supported by the inspected public POI payloads.
    # Bare numeric tag_type/object_type values are deliberately insufficient.
    return any(re.fullmatch(r"(?:1022:)?100101.+", str(value.get(key, "")))
               for key in ("oid", "object_id", "page_id"))


def _declares_checkin(value: Any) -> bool:
    return isinstance(value, dict) and (str(value.get("is_checkin", "")).lower() in ("1", "true")
                                       or str(value.get("type", "")).lower() == "checkin")


def _location_entry(value: Any, field: str, kind: str, status: dict, role: str,
                    evidence: str, is_checkin: bool = False) -> dict:
    """Keep the complete source object; expose only source-declared metadata.

    The coordinates field retains the source key names, coordinate order and
    values. It is not a geocoded result or a claim about device/posting GPS.
    """
    entry = {"kind": kind, "field": field, "value": value, "is_checkin": is_checkin,
             "evidence": evidence, **_provenance(status, role)}
    if not isinstance(value, dict):
        if kind == "geo" and isinstance(value, (list, str)) and has_coordinates(value):
            entry["coordinates"] = {"coordinates": value}
            entry["coordinates_field"] = field
        return entry
    detail = value.get("detail") if isinstance(value.get("detail"), dict) else None
    # Weibo's geo.detail is a declared check-in object; it supplies the POI
    # metadata while the parent geo supplies source-ordered coordinate values.
    if detail:
        nested = _location_entry(detail, field + ".detail", "poi", status, role,
                                 "checkin_geo_detail" if _declares_checkin(detail) else "explicit_location_object",
                                 is_checkin=_declares_checkin(detail))
        for key in ("label", "label_field", "poi_id", "poi_id_field", "poi_id_raw", "url", "url_field"):
            if key in nested:
                entry[key] = nested[key]
        if nested["is_checkin"]:
            entry["is_checkin"] = True
            entry["evidence"] = "checkin_geo_detail"
    for label_key in ("tag_name", "title", "display_name", "page_title", "url_title", "name", "label"):
        if isinstance(value.get(label_key), str) and value[label_key].strip():
            entry["label"] = html_lib.unescape(re.sub(r"<[^>]+>", "", value[label_key])).strip()
            entry["label_field"] = field + "." + label_key
            break
    for id_key in ("poiid", "poi_id", "poid", "object_id", "oid", "page_id", "id"):
        identifier = value.get(id_key)
        if isinstance(identifier, (str, int)) and str(identifier):
            # Retain the exact ID supplied by the source, including namespaces.
            entry["poi_id"] = _canonical_poi(identifier)
            entry["poi_id_raw"] = str(identifier)
            entry["poi_id_field"] = field + "." + id_key
            break
    for url_key in ("url", "page_url", "target_url", "long_url", "h5_target_url", "ori_url", "short_url", "tag_scheme", "scheme"):
        target = value.get(url_key)
        if isinstance(target, str) and target:
            parsed_url = html_lib.unescape(target)
            if parsed_url.startswith("//"):
                parsed_url = "https:" + parsed_url
            try:
                allowed_scheme = urllib.parse.urlsplit(parsed_url).scheme in ("http", "https", "sinaweibo")
            except ValueError:
                allowed_scheme = False
            if allowed_scheme:
                entry["url"] = parsed_url
                entry["url_field"] = field + "." + url_key
                break
    coordinate_keys = ("latitude", "longitude", "lat", "lon", "lng", "coordinates", "coordinate")
    declared = {key: value[key] for key in coordinate_keys if value.get(key) is not None}
    # url_struct.position can be an integer text-link position. Keep that in
    # the raw value but never label it a coordinate. Some POI objects instead
    # declare a two-number position string; retain its order without axis guesses.
    position = value.get("position")
    if isinstance(position, (str, list)) and has_coordinates(position):
        declared["position"] = position
    if "coordinates" in declared and value.get("type") is not None:
        declared["type"] = value["type"]
    if declared:
        entry["coordinates"] = declared
        entry["coordinates_field"] = field
    elif isinstance(value.get("geo"), dict) and value["geo"]:
        entry["coordinates"] = value["geo"]
        entry["coordinates_field"] = field + ".geo"
    return entry


def _merge_location_evidence(entries: list[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = {}
    result = []
    for entry in entries:
        poi = entry.get("poi_id")
        key = (entry["source_post_id"], entry["role"], poi) if poi else None
        raw_evidence = {key: entry[key] for key in ("field", "value", "evidence", "is_checkin")}
        if key is None or key not in grouped:
            entry["evidence_fields"] = [entry["field"]]
            entry["evidence_objects"] = [raw_evidence]
            result.append(entry)
            if key is not None:
                grouped[key] = entry
            continue
        current = grouped[key]
        if entry["field"] not in current["evidence_fields"]:
            current["evidence_fields"].append(entry["field"])
            current["evidence_objects"].append(raw_evidence)
        current["is_checkin"] = current["is_checkin"] or entry["is_checkin"]
        # Keep geo as the primary source object when it actually supplies
        # coordinates; retain every other exact source object above.
        if has_coordinates(entry) and not has_coordinates(current):
            for field in ("field", "value", "kind", "evidence", "coordinates", "coordinates_field"):
                if field in entry:
                    current[field] = entry[field]
        for field in ("label", "label_field", "poi_id_raw", "poi_id_field", "url", "url_field"):
            if field not in current and field in entry:
                current[field] = entry[field]
    return result


def _locations(status: dict, role: str) -> list[dict]:
    result = []
    # No traversal of user.location, region_name/IP labels, topic_struct or text.
    for key in _LOCATION_FIELDS:
        value = status.get(key)
        if (isinstance(value, (dict, list)) and value) or (key == "geo" and isinstance(value, str) and has_coordinates(value)):
            result.append(_location_entry(value, key, "geo" if key == "geo" else ("poi" if "poi" in key else "place"),
                          status, role, "explicit_geo" if key == "geo" else "explicit_location_object",
                          is_checkin=key in ("place", "poi", "poi_info")))
    # Older public formats carry the user's explicit check-in in annotations.
    annotations = status.get("annotations") or []
    if isinstance(annotations, list):
        for index, annotation in enumerate(annotations):
            if not isinstance(annotation, dict):
                continue
            for key in _LOCATION_FIELDS:
                value = annotation.get(key)
                if (isinstance(value, (dict, list)) and value) or (key == "geo" and isinstance(value, str) and has_coordinates(value)):
                    result.append(_location_entry(value, "annotations." + str(index) + "." + key,
                                  "geo" if key == "geo" else ("poi" if "poi" in key else "place"), status, role,
                                  "checkin_annotation" if key in ("place", "poi", "poi_info") else "explicit_location_object",
                                  is_checkin=key in ("place", "poi", "poi_info")))
    # Dedicated optional location pills are represented in tag_struct in the
    # inspected public payloads. A bare tag_type=1, hashtag/name, or location
    # icon without an explicit place object/POI identity is insufficient.
    tags = status.get("tag_struct") or []
    if isinstance(tags, list):
        for index, tag in enumerate(tags):
            if not isinstance(tag, dict) or not _place_object(tag):
                continue
            explicit_false = str(tag.get("is_checkin", "")).lower() in ("0", "false")
            is_checkin = _declares_checkin(tag) or (not explicit_false and bool(tag.get("tag_name")))
            result.append(_location_entry(tag, "tag_struct." + str(index), "poi", status, role,
                          "checkin_tag" if is_checkin else "explicit_place_tag", is_checkin=is_checkin))
    # Place links/cards are kept separately: sharing a POI link does not by
    # itself establish the dedicated optional check-in shown below a post.
    url_struct = status.get("url_struct") or []
    if isinstance(url_struct, list):
        for index, item in enumerate(url_struct):
            if _place_object(item):
                result.append(_location_entry(item, "url_struct." + str(index), "poi", status, role, "typed_place_link"))
    page = status.get("page_info")
    if isinstance(page, dict):
        if _place_object(page):
            result.append(_location_entry(page, "page_info", "poi", status, role, "place_card"))
        for key in _LOCATION_FIELDS:
            value = page.get(key)
            if (isinstance(value, (dict, list)) and value) or (key == "geo" and isinstance(value, str) and has_coordinates(value)):
                result.append(_location_entry(value, "page_info." + key, "geo" if key == "geo" else "poi",
                              status, role, "explicit_location_object"))
    return _merge_location_evidence(result)


def parse_status(status: dict, source_url: str | None = None) -> dict:
    """Normalize one public post, including its explicitly nested repost.

    Pass either the status dict or a {data: status} envelope. success means the
    post response was accessible; it can legitimately have no media/location.
    """
    status = _unwrap_status(status)
    if status is None:
        raise ValueError("Response does not contain an identifiable status")
    root_id = _identifier(status)
    root_bid = str(status.get("mblogid") or status.get("bid") or "")
    user = status.get("user") if isinstance(status.get("user"), dict) else {}
    text = str(status.get("text_raw") or re.sub(r"<[^>]+>", " ", str(status.get("text") or "")))
    repost = status.get("retweeted_status")
    result = {
        "status": "success", "source_url": source_url, "post_id": root_id, "bid": root_bid,
        "post": {"id": root_id, "bid": root_bid, "user_id": str(user.get("idstr") or user.get("id") or ""),
                 "created_at": status.get("created_at"), "text_snippet": html_lib.unescape(text)[:500]},
        "relation": {"type": "repost" if isinstance(repost, dict) else "original", "root_id": root_id,
                     "reposted_id": _identifier(repost) if isinstance(repost, dict) else None},
        "media": [], "locations": [], "warnings": [],
    }
    seen = set()
    current, role = status, "root"
    for depth in range(4):
        identifier = _identifier(current)
        if identifier in seen:
            break
        seen.add(identifier)
        result["media"].extend(_image_media(current, role))
        result["media"].extend(_video_media(current, role))
        result["locations"].extend(_locations(current, role))
        child = current.get("retweeted_status")
        if not isinstance(child, dict):
            break
        if not _is_status(child):
            result["warnings"].append("Reposted status has no accessible content in this response")
            break
        current, role = child, "repost" if depth == 0 else "repost_" + str(depth + 1)
    return result


def parse_html(html: str, source_url: str | None = None) -> dict | None:
    """Return a status object from common public embedded-JSON page patterns.

    Returns None for visitor/login pages, client-rendered pages without post
    JSON, or an unknown layout; it never treats absence of JSON as deletion.
    """
    decoder = json.JSONDecoder()
    markers = (r"\$render_data\s*=", r"(?:window\.)?__INITIAL_STATE__\s*=",
               r"(?:window\.)?__PRELOADED_STATE__\s*=", r"(?:window\.)?__NEXT_DATA__\s*=")
    payloads = []
    for marker in markers:
        for match in re.finditer(marker, html):
            remainder = html[match.end():].lstrip()
            try:
                payloads.append(decoder.raw_decode(remainder)[0])
            except (ValueError, json.JSONDecodeError):
                pass
    for match in re.finditer(r"<script\b([^>]*)>([\s\S]*?)</script\s*>", html, re.I):
        attrs, body = match.groups()
        if re.search(r"application/(?:ld\+)?json|__NEXT_DATA__", attrs, re.I):
            try:
                payloads.append(json.loads(body.strip()))
            except (ValueError, json.JSONDecodeError):
                pass
    for payload in payloads:
        if isinstance(payload, list):
            for item in payload:
                found = _unwrap_status(item)
                if found and _matches_source(found, source_url):
                    return found
        found = _unwrap_status(payload)
        if found and _matches_source(found, source_url):
            return found
    return None


def _failure(status: str, url: str, reason: str, **extra) -> dict:
    return {"status": status, "source_url": url, "post_id": None, "bid": None, "post": None,
            "relation": None, "media": [], "locations": [], "error": reason, **extra}


def classify_access_error(payload: Any, http_status: int | None = None) -> tuple[str, str] | None:
    """Classify endpoint error fields, never words inside an accessible post."""
    if http_status == 429:
        return "rate_limited", "HTTP 429"
    if http_status in (404, 410):
        return "unavailable", "HTTP " + str(http_status)
    if http_status == 401:
        return "login_required", "HTTP 401"
    if not isinstance(payload, dict) or _unwrap_status(payload) is not None:
        return None
    message = str(payload.get("msg") or payload.get("message") or payload.get("errmsg") or "")
    if not message and isinstance(payload.get("error"), str):
        message = payload["error"]
    destination = str(payload.get("url") or payload.get("redirect_url") or "")
    if re.search(r"访问频次|频繁|请求过多|请求过于|限流|rate.?limit|too many requests", message, re.I):
        return "rate_limited", message[:300]
    if re.search(r"请完成验证|安全验证|拖动滑块|人机验证|完成验证码|验证码|captcha|verification required", message, re.I):
        return "captcha_required", message[:300]
    if str(payload.get("ok", "")) == "-100" or "visitor.passport.weibo" in destination:
        return "browser_session_required", "public_browser_session_needed"
    if re.search(r"登录|登陆|login|passport", message + " " + destination, re.I):
        return "login_required", message[:300] or "login_required"
    if re.search(r"不存在|已删除|已被删除|已屏蔽|权限|不可见|无法查看|not (?:found|exist)|deleted|unavailable", message, re.I):
        return "unavailable", message[:300]
    for key in ("data", "error"):
        if isinstance(payload.get(key), dict):
            error = classify_access_error(payload[key])
            if error:
                return error
    return None


def _classify_payload(payload: Any, text: str, url: str) -> dict:
    error = classify_access_error(payload)
    if error:
        return _failure(error[0], url, error[1])
    # A post may quote access/deletion messages. Arbitrary body text is never
    # proof of an access gate. Conventional standalone page titles are useful
    # when the HTTP response contains no matching status JSON.
    title_match = re.search(r"<title[^>]*>([^<]*)</title\s*>", text[:10000], re.I)
    title = html_lib.unescape(title_match.group(1)).strip() if title_match else ""
    if title == "Sina Visitor System":
        return _failure("browser_session_required", url, "public_browser_session_needed")
    if re.fullmatch(r"(?:安全验证|人机验证|Security Verification|Captcha)(?:\s*[-–|]\s*(?:微博|Weibo))?", title, re.I):
        return _failure("captcha_required", url, "security_verification_page")
    if re.fullmatch(r"(?:微博登录|新浪通行证|用户登录|登录)(?:\s*[-–|]\s*(?:微博|新浪微博|Weibo))?", title, re.I):
        return _failure("login_required", url, "login_page")
    message = str(payload.get("msg") or payload.get("message") or payload.get("errmsg") or "") if isinstance(payload, dict) else ""
    return _failure("unexpected_response", url, message[:300] or "No status JSON in public response")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Do not forward an optional cookie to another hostname automatically.
        return None


class WeiboBackend:
    """Public HTTP backend with bounded requests and explicit access statuses.

    Cookies, if used, must be supplied deliberately by the user. They are read
    only into memory and never included in results or logs. A normal anonymous
    browser backend may be needed for browser_session_required responses.
    """
    def __init__(self, cookie: str | None = None, cookie_file: str | Path | None = None,
                 timeout: float = 20, retries: int = 2, delay: float = 1.0):
        if cookie and cookie_file:
            raise ValueError("Supply cookie or cookie_file, not both")
        if cookie_file:
            cookie = Path(cookie_file).read_text(encoding="utf-8-sig").strip()
        if cookie and ("\r" in cookie or "\n" in cookie):
            raise ValueError("Cookie must be a single header value")
        self._cookie = cookie
        self.timeout = max(1.0, min(float(timeout), 120.0))
        self.retries = max(0, min(int(retries), 5))
        self.delay = max(0.0, float(delay))
        self._last_request = 0.0
        self._opener = urllib.request.build_opener(_NoRedirect())

    def _request(self, url: str) -> tuple[int | None, dict, str, str | None, int]:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
                   "Accept": "application/json,text/html;q=0.9,*/*;q=0.5", "Referer": "https://weibo.com/"}
        if self._cookie:
            headers["Cookie"] = self._cookie
        for attempt in range(self.retries + 1):
            remaining = self.delay - (time.monotonic() - self._last_request)
            if remaining > 0:
                time.sleep(remaining)
            self._last_request = time.monotonic()
            try:
                request = urllib.request.Request(url, headers=headers)
                try:
                    response = self._opener.open(request, timeout=self.timeout)
                except urllib.error.HTTPError as exc:
                    response = exc
                with response:
                    status = response.code
                    response_headers = dict(response.headers.items())
                    raw = response.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    return status, response_headers, "", "response_too_large", attempt + 1
                encoding = next((v for k, v in response_headers.items() if k.lower() == "content-encoding"), "")
                if "gzip" in encoding:
                    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as compressed:
                        raw = compressed.read(MAX_RESPONSE_BYTES + 1)
                    if len(raw) > MAX_RESPONSE_BYTES:
                        return status, response_headers, "", "response_too_large", attempt + 1
                text = raw.decode("utf-8-sig", errors="replace")
                if status in (500, 502, 503, 504) and attempt < self.retries:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                return status, response_headers, text, None, attempt + 1
            except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
                if attempt < self.retries:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                # Report error type only: no optional credential/header content.
                return None, {}, "", type(exc).__name__, attempt + 1
        return None, {}, "", "request_failed", self.retries + 1

    def fetch_post(self, url: str) -> dict:
        try:
            token = post_token(url)
        except ValueError as exc:
            return _failure("invalid_url", url, str(exc))
        api = "https://weibo.com/ajax/statuses/show?id=" + urllib.parse.quote(token)
        status, headers, text, error, attempts = self._request(api)
        metadata = {"backend": "public_http", "http_status": status, "attempts": attempts,
                    "endpoint": "weibo.com/ajax/statuses/show"}
        if error:
            result = _failure("network_error" if error != "response_too_large" else "unexpected_response", url, error)
        elif status == 429:
            result = _failure("rate_limited", url, "HTTP 429")
        elif status in (404, 410):
            result = _failure("unavailable", url, "HTTP " + str(status))
        elif status == 401:
            result = _failure("login_required", url, "HTTP 401")
        elif status in (301, 302, 303, 307, 308):
            destination = next((v for k, v in headers.items() if k.lower() == "location"), "")
            result = _classify_payload({"url": destination}, text, url)
        elif status in (403, 418):
            classified = _classify_payload({}, text, url)
            result = classified if classified["status"] != "unexpected_response" else _failure("access_denied", url, "HTTP " + str(status))
        elif status and status >= 500:
            result = _failure("server_error", url, "HTTP " + str(status))
        else:
            try:
                payload = json.loads(text)
            except (ValueError, json.JSONDecodeError):
                payload = None
            parsed = _unwrap_status(payload) if payload is not None else parse_html(text, url)
            if parsed and not _matches_source(parsed, url):
                parsed = None
            result = parse_status(parsed, url) if parsed else _classify_payload(payload, text, url)
            # A plain client-rendered API response can be checked against the
            # canonical public post page. Session gates/deletion are final here.
            if result["status"] == "unexpected_response":
                canonical = "https://weibo.com/detail/" + token
                page_status, page_headers, page_text, page_error, page_attempts = self._request(canonical)
                metadata["html_fallback_http_status"] = page_status
                metadata["attempts"] += page_attempts
                if not page_error:
                    page_parsed = parse_html(page_text, url) if page_status == 200 else None
                    if page_parsed:
                        result = parse_status(page_parsed, url)
                    else:
                        destination = next((v for k, v in page_headers.items() if k.lower() == "location"), "")
                        result = _classify_payload({"url": destination}, page_text, url)
                        if result["status"] == "unexpected_response":
                            if page_status in (404, 410):
                                result = _failure("unavailable", url, "HTTP " + str(page_status))
                            elif page_status == 429:
                                result = _failure("rate_limited", url, "HTTP 429")
                            elif page_status == 401:
                                result = _failure("login_required", url, "HTTP 401")
                            elif page_status in (403, 418):
                                result = _failure("access_denied", url, "HTTP " + str(page_status))
                            elif page_status and page_status >= 500:
                                result = _failure("server_error", url, "HTTP " + str(page_status))
                else:
                    result["fallback_error"] = page_error
        result["request"] = metadata
        result["requested_token"] = token
        return result


def fetch_post(url: str, **options) -> dict:
    """Convenience wrapper; reuse WeiboBackend for a multi-post pilot."""
    return WeiboBackend(**options).fetch_post(url)
