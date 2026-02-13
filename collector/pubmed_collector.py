"""
PubMed Collector — E-utilities API
USTHB — PFE Master Bioinformatique 2025/2026

Collecte des articles biomédicaux depuis PubMed via l'API E-utilities NCBI.
Flux : esearch (IDs) -> efetch (details XML) -> parse -> Kafka
"""

import os
import time
import logging
import requests
import schedule
from datetime import datetime
from typing import Optional
from xml.etree import ElementTree as ET
from base_collector import BaseCollector

logger = logging.getLogger(__name__)

# ============================================================
# CONFIG PUBMED
# ============================================================
PUBMED_BASE_URL = os.getenv("PUBMED_BASE_URL", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils")
PUBMED_API_KEY  = os.getenv("PUBMED_API_KEY", "")
FETCH_INTERVAL  = int(os.getenv("PUBMED_FETCH_INTERVAL_MINUTES", "60"))
MAX_RESULTS     = int(os.getenv("PUBMED_MAX_RESULTS_PER_QUERY", "100"))

# Requetes par seconde autorisees (3 sans cle, 10 avec cle API)
RATE_LIMIT_DELAY = 0.34 if not PUBMED_API_KEY else 0.11

# Domaines biomédicaux a surveiller
DEFAULT_QUERIES = os.getenv(
    "SEARCH_QUERIES",
    "bioinformatics,deep learning genomics,protein structure prediction,CRISPR,RNA sequencing"
).split(",")


class PubMedCollector(BaseCollector):
    """
    Collecteur d'articles depuis PubMed via E-utilities API.

    Utilise 2 appels API :
    1. esearch : cherche et retourne les IDs des articles
    2. efetch  : recupere le detail XML de chaque article
    """

    def __init__(self):
        super().__init__("pubmed")
        self.base_url = PUBMED_BASE_URL
        self.api_key  = PUBMED_API_KEY
        self.session  = requests.Session()
        self.session.headers.update({"User-Agent": "USTHB-Recommender/1.0"})

    # ──────────────────────────────────────────────────────────
    # ETAPE 1 : Recherche des IDs
    # ──────────────────────────────────────────────────────────
    def search_ids(self, query: str, max_results: int = 100) -> list:
        """
        Appelle esearch pour obtenir la liste des PMIDs.
        """
        params = {
            "db":       "pubmed",
            "term":     query,
            "retmax":   max_results,
            "retmode":  "json",
            "sort":     "relevance",
            "usehistory": "n",
        }
        if self.api_key:
            params["api_key"] = self.api_key

        try:
            url = f"{self.base_url}/esearch.fcgi"
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            ids = data.get("esearchresult", {}).get("idlist", [])
            self.logger.info(f"esearch '{query}': {len(ids)} IDs trouves")
            return ids

        except requests.RequestException as e:
            self.logger.error(f"Erreur esearch: {e}")
            return []

    # ──────────────────────────────────────────────────────────
    # ETAPE 2 : Récupération des détails
    # ──────────────────────────────────────────────────────────
    def fetch_details(self, pmids: list) -> str:
        """
        Appelle efetch pour obtenir le XML complet des articles.
        Envoie les IDs par batch de 200 max.
        """
        if not pmids:
            return ""

        # PubMed accepte max 200 IDs par requete
        batch_size = 200
        all_xml = []

        for i in range(0, len(pmids), batch_size):
            batch = pmids[i:i + batch_size]
            params = {
                "db":      "pubmed",
                "id":      ",".join(batch),
                "retmode": "xml",
                "rettype": "abstract",
            }
            if self.api_key:
                params["api_key"] = self.api_key

            try:
                url = f"{self.base_url}/efetch.fcgi"
                response = self.session.get(url, params=params, timeout=60)
                response.raise_for_status()
                all_xml.append(response.text)

                # Respecter le rate limit
                time.sleep(RATE_LIMIT_DELAY)

            except requests.RequestException as e:
                self.logger.error(f"Erreur efetch batch {i}: {e}")

        return "\n".join(all_xml)

    # ──────────────────────────────────────────────────────────
    # ETAPE 3 : Parsing XML -> dict unifié
    # ──────────────────────────────────────────────────────────
    def parse_item(self, article_xml) -> Optional[dict]:
        """
        Parse un element XML PubmedArticle -> dict unifie.
        """
        try:
            # PMID
            pmid_el = article_xml.find(".//PMID")
            if pmid_el is None:
                return None
            pmid = pmid_el.text.strip()

            # Titre
            title_el = article_xml.find(".//ArticleTitle")
            title = self.clean_text(title_el.text or "") if title_el is not None else ""
            if not title:
                return None

            # Abstract
            abstract_parts = article_xml.findall(".//AbstractText")
            if abstract_parts:
                abstract = " ".join(
                    self.clean_text(p.text or "")
                    for p in abstract_parts
                    if p.text
                )
            else:
                abstract = ""

            # Auteurs
            authors = []
            for author in article_xml.findall(".//Author"):
                last  = author.findtext("LastName", "")
                first = author.findtext("ForeName", "")
                if last:
                    authors.append(f"{last} {first}".strip())

            # Journal
            journal = article_xml.findtext(".//Journal/Title", "") or \
                      article_xml.findtext(".//ISOAbbreviation", "")

            # Date de publication
            pub_date = self._extract_date(article_xml)

            # Keywords
            keywords = [
                kw.text.strip()
                for kw in article_xml.findall(".//Keyword")
                if kw.text
            ]

            # MeSH terms
            mesh_terms = [
                m.text.strip()
                for m in article_xml.findall(".//MeshHeading/DescriptorName")
                if m.text
            ]

            # DOI
            doi = ""
            for id_el in article_xml.findall(".//ArticleId"):
                if id_el.get("IdType") == "doi":
                    doi = id_el.text or ""
                    break

            # Citations count (PubMed ne le fournit pas directement)
            citations_count = 0

            return {
                "id":               self.make_article_id("pubmed", pmid),
                "source":           "pubmed",
                "title":            title,
                "abstract":         abstract,
                "authors":          authors,
                "keywords":         keywords,
                "mesh_terms":       mesh_terms,
                "publication_date": pub_date,
                "journal":          self.clean_text(journal),
                "doi":              doi,
                "citations_count":  citations_count,
                "references":       [],
                "pmid":             pmid,
                "arxiv_id":         None,
                "arxiv_categories": [],
                "language":         "en",
                "collected_at":     datetime.utcnow().isoformat(),
                "processing_status": "raw"
            }

        except Exception as e:
            self.logger.error(f"Erreur parse_item: {e}")
            return None

    def _extract_date(self, article_xml) -> str:
        """Extrait la date de publication au format YYYY-MM-DD."""
        # Essayer PubDate d'abord
        pub_date = article_xml.find(".//PubDate")
        if pub_date is not None:
            year  = pub_date.findtext("Year", "")
            month = pub_date.findtext("Month", "01")
            day   = pub_date.findtext("Day", "01")
            if year:
                # Convertir le mois texte en chiffre si necessaire
                month_map = {
                    "Jan":"01","Feb":"02","Mar":"03","Apr":"04",
                    "May":"05","Jun":"06","Jul":"07","Aug":"08",
                    "Sep":"09","Oct":"10","Nov":"11","Dec":"12"
                }
                month = month_map.get(month, month).zfill(2)
                day   = str(day).zfill(2)
                return f"{year}-{month}-{day}"

        # Essayer ArticleDate
        art_date = article_xml.find(".//ArticleDate")
        if art_date is not None:
            year  = art_date.findtext("Year", "")
            month = art_date.findtext("Month", "01").zfill(2)
            day   = art_date.findtext("Day", "01").zfill(2)
            if year:
                return f"{year}-{month}-{day}"

        return ""

    # ──────────────────────────────────────────────────────────
    # METHODE PRINCIPALE
    # ──────────────────────────────────────────────────────────
    def collect(self, query: str, max_results: int = 100) -> list:
        """
        Collecte complete pour une requete donnee.
        1. esearch -> IDs
        2. efetch  -> XML
        3. parse   -> dicts
        """
        # 1. Obtenir les IDs
        pmids = self.search_ids(query, max_results)
        if not pmids:
            return []

        # 2. Obtenir le XML
        xml_content = self.fetch_details(pmids)
        if not xml_content:
            return []

        # 3. Parser chaque article
        articles = []
        try:
            root = ET.fromstring(xml_content)
            for article_el in root.findall(".//PubmedArticle"):
                article = self.parse_item(article_el)
                if article:
                    articles.append(article)
        except ET.ParseError as e:
            self.logger.error(f"Erreur parsing XML: {e}")

        self.logger.info(f"Collecte '{query}': {len(articles)}/{len(pmids)} articles parsed")
        return articles


# ============================================================
# MODE DAEMON — collecte en continu toutes les X minutes
# ============================================================
def run_daemon():
    """Lance le collecteur en mode daemon (toutes les FETCH_INTERVAL minutes)."""
    logger.info(f"Daemon PubMed demarre — intervalle: {FETCH_INTERVAL} min")
    logger.info(f"Requetes: {DEFAULT_QUERIES}")

    collector = PubMedCollector()

    def job():
        logger.info("=== Collecte PubMed ===")
        total = {"collected": 0, "sent": 0}
        for query in DEFAULT_QUERIES:
            try:
                stats = collector.collect_and_send(query.strip(), MAX_RESULTS)
                total["collected"] += stats["collected"]
                total["sent"]      += stats["sent"]
            except Exception as e:
                logger.error(f"Erreur query '{query}': {e}")
        logger.info(f"=== Fin collecte: {total} ===")

    # Premiere execution immediate
    job()

    # Puis toutes les FETCH_INTERVAL minutes
    schedule.every(FETCH_INTERVAL).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(30)


# ============================================================
# TEST STANDALONE : python pubmed_collector.py
# ============================================================
if __name__ == "__main__":
    import sys

    print("\n" + "="*50)
    print("  TEST PUBMED COLLECTOR — USTHB")
    print("="*50)

    # Mode test : collecte 5 articles sur une requete simple
    TEST_QUERY   = "bioinformatics deep learning 2024"
    TEST_MAX     = 5

    collector = PubMedCollector()

    print(f"\n[1] Recherche IDs: '{TEST_QUERY}'")
    ids = collector.search_ids(TEST_QUERY, TEST_MAX)
    print(f"    IDs trouves: {ids}")

    if not ids:
        print("    Aucun ID — verifiez votre connexion internet")
        sys.exit(1)

    print(f"\n[2] Recuperation XML pour {len(ids)} articles...")
    xml = collector.fetch_details(ids[:3])  # Limiter a 3 pour le test
    print(f"    XML recu: {len(xml)} caracteres")

    print(f"\n[3] Parsing XML...")
    articles = collector.collect(TEST_QUERY, TEST_MAX)
    print(f"    Articles parses: {len(articles)}")

    if articles:
        a = articles[0]
        print(f"\n    Premier article:")
        print(f"    ID      : {a['id']}")
        print(f"    Titre   : {a['title'][:80]}...")
        print(f"    Auteurs : {a['authors'][:3]}")
        print(f"    Date    : {a['publication_date']}")
        print(f"    Journal : {a['journal']}")
        print(f"    MeSH    : {a['mesh_terms'][:5]}")

    print(f"\n[4] Envoi vers Kafka...")
    try:
        stats = collector.collect_and_send(TEST_QUERY, TEST_MAX)
        print(f"    Stats: {stats}")
        print(f"\n Verifiez http://localhost:8090 — topic: articles.raw")
    except Exception as e:
        print(f"    Erreur Kafka: {e}")
        print(f"    (Normal si Kafka n'est pas lance)")

    print("\n" + "="*50)
    print("  TEST TERMINE")
    print("="*50 + "\n")