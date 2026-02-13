"""
arXiv Collector — API Atom/XML
USTHB — PFE Master Bioinformatique 2025/2026

Collecte des preprints depuis arXiv dans les categories :
cs.LG, q-bio.GN, q-bio.BM, cs.AI, stat.ML
"""

import os
import time
import logging
import requests
import feedparser
from datetime import datetime
from typing import Optional
from base_collector import BaseCollector

logger = logging.getLogger(__name__)

ARXIV_BASE_URL  = os.getenv("ARXIV_BASE_URL", "http://export.arxiv.org/api/query")
FETCH_INTERVAL  = int(os.getenv("ARXIV_FETCH_INTERVAL_MINUTES", "120"))
MAX_RESULTS     = int(os.getenv("ARXIV_MAX_RESULTS_PER_QUERY", "50"))

# Categories arXiv pertinentes pour la bioinformatique
ARXIV_CATEGORIES = [
    "cs.LG",     # Machine Learning
    "q-bio.GN",  # Genomics
    "q-bio.BM",  # Biomolecules
    "cs.AI",     # Artificial Intelligence
    "stat.ML",   # Statistics - Machine Learning
]


class ArXivCollector(BaseCollector):
    """
    Collecteur d'articles depuis arXiv via l'API Atom.
    Pas de cle API requise.
    """

    def __init__(self):
        super().__init__("arxiv")
        self.base_url = ARXIV_BASE_URL
        self.session  = requests.Session()
        self.session.headers.update({"User-Agent": "USTHB-Recommender/1.0"})

    def collect(self, query: str, max_results: int = 50) -> list:
        """
        Collecte des articles arXiv pour une requete donnee.
        Utilise l'API Atom/RSS d'arXiv.
        """
        # Construire la requete arXiv
        # On combine le texte libre avec les categories bioinfo
        cat_filter = " OR ".join([f"cat:{c}" for c in ARXIV_CATEGORIES])
        full_query = f"({query}) AND ({cat_filter})"

        params = {
            "search_query": full_query,
            "start":        0,
            "max_results":  max_results,
            "sortBy":       "submittedDate",
            "sortOrder":    "descending"
        }

        try:
            response = self.session.get(
                self.base_url,
                params=params,
                timeout=30
            )
            response.raise_for_status()

            # Parser le feed Atom avec feedparser
            feed = feedparser.parse(response.content)

            articles = []
            for entry in feed.entries:
                article = self.parse_item(entry)
                if article:
                    articles.append(article)

            self.logger.info(f"arXiv '{query}': {len(articles)} articles collectes")

            # Rate limit arXiv : 3 secondes entre requetes
            time.sleep(3)
            return articles

        except requests.RequestException as e:
            self.logger.error(f"Erreur arXiv API: {e}")
            return []

    def parse_item(self, entry) -> Optional[dict]:
        """
        Parse une entree feedparser -> dict unifie.
        """
        try:
            # ID arXiv (format: http://arxiv.org/abs/2402.12345v1)
            raw_id  = entry.get("id", "")
            arxiv_id = raw_id.split("/abs/")[-1].replace("v", "_v") if "/abs/" in raw_id else raw_id

            if not arxiv_id:
                return None

            # Titre
            title = self.clean_text(entry.get("title", ""))
            if not title:
                return None

            # Abstract
            abstract = self.clean_text(entry.get("summary", ""))

            # Auteurs
            authors = []
            for author in entry.get("authors", []):
                name = author.get("name", "").strip()
                if name:
                    authors.append(name)

            # Categories
            categories = []
            for tag in entry.get("tags", []):
                term = tag.get("term", "")
                if term:
                    categories.append(term)

            # Date de publication
            pub_date = ""
            published = entry.get("published", "")
            if published:
                try:
                    # Format arXiv: "2024-02-20T00:00:00Z"
                    pub_date = published[:10]
                except Exception:
                    pub_date = ""

            # DOI (si disponible dans les liens)
            doi = ""
            for link in entry.get("links", []):
                if link.get("title") == "doi":
                    doi = link.get("href", "")
                    break

            # Journal (pour arXiv c'est toujours "arXiv preprint")
            journal = "arXiv preprint"

            # Extraire les mots-cles du titre et abstract
            keywords = self._extract_keywords_from_categories(categories)

            return {
                "id":               self.make_article_id("arxiv", arxiv_id),
                "source":           "arxiv",
                "title":            title,
                "abstract":         abstract,
                "authors":          authors,
                "keywords":         keywords,
                "mesh_terms":       [],
                "publication_date": pub_date,
                "journal":          journal,
                "doi":              doi,
                "citations_count":  0,
                "references":       [],
                "pmid":             None,
                "arxiv_id":         arxiv_id,
                "arxiv_categories": categories,
                "language":         "en",
                "collected_at":     datetime.utcnow().isoformat(),
                "processing_status": "raw"
            }

        except Exception as e:
            self.logger.error(f"Erreur parse_item arXiv: {e}")
            return None

    def _extract_keywords_from_categories(self, categories: list) -> list:
        """Convertit les categories arXiv en mots-cles lisibles."""
        mapping = {
            "cs.LG":    "machine learning",
            "cs.AI":    "artificial intelligence",
            "cs.CL":    "natural language processing",
            "q-bio.GN": "genomics",
            "q-bio.BM": "biomolecules",
            "q-bio.NC": "neurons and cognition",
            "stat.ML":  "statistical machine learning",
            "cs.CV":    "computer vision",
        }
        keywords = []
        for cat in categories:
            if cat in mapping:
                keywords.append(mapping[cat])
        return keywords


# ============================================================
# TEST STANDALONE : python arxiv_collector.py
# ============================================================
if __name__ == "__main__":
    import sys

    print("\n" + "="*50)
    print("  TEST ARXIV COLLECTOR — USTHB")
    print("="*50)

    TEST_QUERY = "transformer protein structure"
    TEST_MAX   = 5

    collector = ArXivCollector()

    print(f"\n[1] Collecte arXiv: '{TEST_QUERY}' (max {TEST_MAX})...")
    articles = collector.collect(TEST_QUERY, TEST_MAX)
    print(f"    Articles collectes: {len(articles)}")

    if articles:
        a = articles[0]
        print(f"\n    Premier article:")
        print(f"    ID         : {a['id']}")
        print(f"    Titre      : {a['title'][:80]}...")
        print(f"    Auteurs    : {a['authors'][:3]}")
        print(f"    Date       : {a['publication_date']}")
        print(f"    Categories : {a['arxiv_categories'][:3]}")
        print(f"    Keywords   : {a['keywords']}")

    print(f"\n[2] Envoi vers Kafka...")
    try:
        stats = collector.collect_and_send(TEST_QUERY, TEST_MAX)
        print(f"    Stats: {stats}")
        print(f"\n Verifiez http://localhost:8090 — topic: articles.raw")
    except Exception as e:
        print(f"    Erreur Kafka: {e}")

    print("\n" + "="*50)
    print("  TEST TERMINE")
    print("="*50 + "\n")