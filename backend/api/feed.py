"""
Feed API router.

Endpoints:
    GET /feed                          — latest articles (Redis → Postgres)
  GET /article/{article_id}          — single article by UUID
  GET /personalized_feed/{user_id}   — PNC-filtered, recency-weighted feed
"""
from __future__ import annotations

import datetime
import logging
import re

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import desc, or_, select, func, case

from backend.core.clients import get_encoder, get_qdrant
from backend.core.auth import get_current_user
from backend.core.db import cache
from backend.core.db.postgres import Article, AsyncSessionLocal, User, UserConstitution, ArticleVote
from backend.config import get_settings

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Feed"])

_WS_RE = re.compile(r"\s+")
_URL_RE = re.compile(r"https?://\S+", flags=re.IGNORECASE)


@router.get("/feed")
async def get_feed(category: str | None = None, limit: int | None = None, trending: bool = False):
    """
    Latest articles. If a category is provided, runs a semantic search via Qdrant to return category matches.
    """
    settings = get_settings()
    feed_limit = limit if isinstance(limit, int) and limit > 0 else settings.feed_article_limit

    if trending:
        # Trending: Join with votes and order by upvotes DESC, downvotes ASC
        async with AsyncSessionLocal() as session:
            # We use sum(case...) to count up/down votes for each article
            up_count = func.sum(case((ArticleVote.vote == 1, 1), else_=0)).label("upvotes")
            down_count = func.sum(case((ArticleVote.vote == -1, 1), else_=0)).label("downvotes")
            
            stmt = (
                select(Article, up_count, down_count)
                .outerjoin(ArticleVote, Article.id == ArticleVote.article_id)
                .group_by(Article.id)
            )
            
            if category and category != "All":
                stmt = stmt.where(Article.category == category)
            
            # Sort by upvotes DESC, then by downvotes ASC (less downvotes is better for ties)
            stmt = stmt.order_by(desc("upvotes"), "downvotes", desc(Article.published_at)).limit(feed_limit)
            
            result = await session.execute(stmt)
            rows = result.all()

            feed_data = [
                _serialize_article(art, votes=(int(ups or 0), int(downs or 0)))
                for art, ups, downs in rows
            ]

            return {"source": "postgres_trending", "data": feed_data}

    if not category or category == "All":
        cached = await cache.get_cached_feed()
        if cached:
            return {"source": "redis_cache", "data": cached[:feed_limit]}
    
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Article).order_by(desc(Article.published_at)).limit(feed_limit)
            )
            articles = result.scalars().all()
            feed_data = [_serialize_article(a) for a in articles]
    
        if feed_data:
            await cache.set_cached_feed(feed_data)
    
        return {"source": "postgres_db", "data": feed_data}
        
    # Semantic search with category (Qdrant)
    encoder = get_encoder()
    qdrant = get_qdrant()

    feed_data = []
    has_qdrant_hits = False

    if encoder and qdrant:
        try:
            CATEGORY_DEFS = {
                "Technology": "Technology Software AI Startups Gadgets Computing Engineering Internet",
                "Finance": "Finance Markets Economy Stocks Banking Crypto Business Investment",
                "Science": "Science Physics Biology Space Astronomy Research Chemistry",
                "Politics": "Politics Government Elections Policies Diplomacy Congress Law",
                "Health": "Health Medicine Wellness Healthcare Disease Fitness Diet",
                "Policy": "Policy Regulation Law Governance Legislation Public Affairs Rules",
            }
            query_str = CATEGORY_DEFS.get(category, category)
            query_vector = encoder.encode(query_str).tolist()
            
            raw_hits = qdrant.query_points(
                collection_name=settings.qdrant_collection,
                query=query_vector,
                limit=feed_limit,
            ).points

            if raw_hits:
                has_qdrant_hits = True
                feed_data = [_serialize_qdrant_hit(h, fallback_category=category) for h in raw_hits]

        except Exception as e:
            logger.warning("Qdrant category search failed for '%s': %s", category, e)
            
    if not feed_data:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Article).where(Article.category == category).order_by(desc(Article.published_at)).limit(feed_limit)
            )
            feed_data = [_serialize_article(a) for a in result.scalars().all()]
            
    source = "qdrant_semantic" if has_qdrant_hits else "postgres_fallback"
    return {"source": source, "data": feed_data}


@router.get("/article/{article_id}")
async def get_article(article_id: str):
    """
    Fetch a single article's full content by UUID.
    Cached in Redis for 1 hour.
    """
    cached = await cache.get_cached_article(article_id)
    if cached:
        return {"source": "redis_cache", "data": cached}

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Article).where(Article.id == article_id)
        )
        article = result.scalar_one_or_none()

    if not article:
        raise HTTPException(status_code=404, detail="Article not found.")

    data = _serialize_article(article, include_content=True)
    await cache.set_cached_article(article_id, data)
    return {"source": "postgres_db", "data": data}


@router.get("/personalized_feed/{user_id}")
async def get_personalized_feed(
    user_id: str,
    limit: int | None = None,
    current_user: User = Depends(get_current_user),
):
    """
    PNC-filtered, recency-weighted personalized feed.

    Uses Qdrant semantic search against the user's priority_domains,
    applies time-decay scoring, then re-hydrates from Postgres.
    Falls back to simple Postgres category filter if Qdrant is unavailable.

    Cached per-user for 2 minutes.
    """
    if user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not allowed to access another user's feed.",
        )

    settings = get_settings()
    personalized_limit = limit if isinstance(limit, int) and limit > 0 else settings.feed_article_limit

    cached = await cache.get_cached_user_feed(user_id, personalized_limit)
    if cached:
        return {"source": "redis_cache", "user_id": user_id, "data": cached}

    async with AsyncSessionLocal() as session:
        user = await session.get(UserConstitution, user_id)
        if not user:
            logger.info("User constitution not found for %s; using recency fallback.", user_id)
            fallback_data = await _fetch_recent_articles(session, personalized_limit)
            if fallback_data:
                await cache.set_cached_user_feed(user_id, fallback_data, limit=personalized_limit)
            return {
                "source": "postgres_no_constitution_fallback",
                "user_id": user_id,
                "data": fallback_data,
            }

        const = user.constitution
        raw_priority_domains = const.get("topical_constraints", {}).get("priority_domains", [])
        raw_excluded_topics = const.get("topical_constraints", {}).get("excluded_topics", [])
        priority_domains: list[str] = [
            domain.strip()
            for domain in raw_priority_domains
            if isinstance(domain, str) and domain.strip()
        ]
        excluded_topics: list[str] = [
            topic.strip()
            for topic in raw_excluded_topics
            if isinstance(topic, str) and topic.strip()
        ]

        feed_data: list[dict] = []
        encoder = get_encoder()
        qdrant = get_qdrant()
        used_qdrant = False

        if priority_domains and encoder and qdrant:
            try:
                query_vector = encoder.encode(" ".join(priority_domains)).tolist()
                raw_hits = qdrant.query_points(
                    collection_name=settings.qdrant_collection,
                    query=query_vector,
                    limit=settings.background_article_limit,
                ).points

                now = datetime.datetime.now(datetime.timezone.utc)
                scored: list[tuple[float, str]] = []

                for hit in raw_hits:
                    content = str(hit.payload.get("content", "")).lower()
                    if any(ex.lower() in content for ex in excluded_topics):
                        continue

                    days_old = _parse_days_old(hit.payload.get("timestamp", ""), now)
                    time_weighted = hit.score * (0.85 ** max(0.0, days_old))
                    scored.append((time_weighted, str(hit.id)))

                scored.sort(reverse=True)
                matched_ids = list(dict.fromkeys(doc_id for _, doc_id in scored))[:personalized_limit]

                if matched_ids:
                    result = await session.execute(
                        select(Article).where(Article.id.in_(matched_ids))
                    )
                    id_map = {a.id: a for a in result.scalars().all()}
                    sorted_articles = [id_map[did] for did in matched_ids if did in id_map]
                    feed_data = [_serialize_article(a) for a in sorted_articles]
                    used_qdrant = bool(feed_data)

            except Exception as e:
                logger.warning("Qdrant personalized feed failed for %s: %s", user_id, e)

        if len(feed_data) < personalized_limit:
            # Top-up with category-based recency from Postgres.
            existing_ids = {item["id"] for item in feed_data}
            query = _build_personalized_category_query(priority_domains, excluded_topics)
            if existing_ids:
                query = query.where(~Article.id.in_(existing_ids))
            query = query.order_by(desc(Article.published_at)).limit(personalized_limit - len(feed_data))
            result = await session.execute(query)
            feed_data.extend(_serialize_article(a) for a in result.scalars().all())

        if len(feed_data) < personalized_limit:
            # Safety top-up from latest articles while honoring excluded categories.
            existing_ids = {item["id"] for item in feed_data}
            query = select(Article)
            for ex in excluded_topics:
                query = query.where(_build_excluded_category_condition(ex))
            if existing_ids:
                query = query.where(~Article.id.in_(existing_ids))
            query = query.order_by(desc(Article.published_at)).limit(personalized_limit - len(feed_data))
            result = await session.execute(query)
            feed_data.extend(_serialize_article(a) for a in result.scalars().all())

        if not feed_data:
            # Last resort to avoid empty personalized feed when articles exist.
            feed_data = await _fetch_recent_articles(session, personalized_limit)

    if feed_data:
        await cache.set_cached_user_feed(user_id, feed_data, limit=personalized_limit)

    source = "qdrant_semantic" if used_qdrant else "postgres_fallback"
    return {"source": source, "user_id": user_id, "data": feed_data}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _serialize_article(
    article: Article,
    include_content: bool = False,
    votes: tuple[int, int] | None = None,
) -> dict:
    data = {
        "id": article.id,
        "title": article.title,
        "url": article.url,
        "source": article.source,
        "image_url": article.image_url,
        "published_at": article.published_at.isoformat() if article.published_at else None,
        "category": article.category,
        "excerpt": _excerpt_from_text(article.content),
        "ai_slop_score": article.ai_slop_score,
        "ai_slop_label": article.ai_slop_label,
    }

    if votes is not None:
        upvotes, downvotes = votes
        data["upvotes"] = upvotes
        data["downvotes"] = downvotes

    if include_content:
        data["content"] = article.content

    return data


def _serialize_qdrant_hit(hit, fallback_category: str | None = None) -> dict:
    payload = hit.payload or {}
    return {
        "id": str(hit.id),
        "title": payload.get("title", "Untitled"),
        "url": payload.get("url") or payload.get("source", ""),
        "source": payload.get("source_name") or payload.get("source", ""),
        "image_url": None,
        "published_at": _parse_qdrant_timestamp(payload.get("timestamp")),
        "category": payload.get("category") or fallback_category,
        "excerpt": _excerpt_from_text(payload.get("content")),
        "ai_slop_score": payload.get("ai_slop_score"),
        "ai_slop_label": payload.get("ai_slop_label"),
        "upvotes": 0,
        "downvotes": 0,
    }


def _parse_qdrant_timestamp(ts: str | None) -> str | None:
    if not ts:
        return None

    try:
        if len(ts) == 14 and ts.isdigit():
            return datetime.datetime.strptime(ts, "%Y%m%d%H%M%S").replace(
                tzinfo=datetime.timezone.utc
            ).isoformat()

        return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")).isoformat()
    except Exception:
        return None


def _excerpt_from_text(text: str | None, limit: int = 320) -> str:
    if not text:
        return ""

    normalized = _WS_RE.sub(" ", str(text)).strip()
    if not normalized:
        return ""

    without_urls = _URL_RE.sub("", normalized).strip()
    if not without_urls:
        return ""

    return without_urls if len(without_urls) <= limit else f"{without_urls[:limit].rstrip()}..."


def _build_personalized_category_query(
    priority_domains: list[str],
    excluded_topics: list[str],
):
    query = select(Article)
    if priority_domains:
        query = query.where(or_(*[Article.category.ilike(f"%{d}%") for d in priority_domains]))
    for ex in excluded_topics:
        query = query.where(_build_excluded_category_condition(ex))
    return query


def _build_excluded_category_condition(excluded_topic: str):
    pattern = f"%{excluded_topic}%"
    return or_(Article.category.is_(None), ~Article.category.ilike(pattern))


async def _fetch_recent_articles(session, limit: int) -> list[dict]:
    result = await session.execute(
        select(Article).order_by(desc(Article.published_at)).limit(limit)
    )
    return [_serialize_article(a) for a in result.scalars().all()]


def _parse_days_old(ts_str: str, now: datetime.datetime) -> float:
    try:
        if len(ts_str) == 14 and ts_str.isdigit():
            pub = datetime.datetime.strptime(ts_str, "%Y%m%d%H%M%S").replace(
                tzinfo=datetime.timezone.utc
            )
        else:
            pub = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return max(0.0, (now - pub).total_seconds() / 86400.0)
    except Exception:
        return 1.0
