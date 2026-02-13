"""
Script principal du collecteur — tourne en continu
USTHB — PFE Master Bioinformatique 2025/2026

Lance les collecteurs PubMed et arXiv selon un planning,
envoie les articles vers Kafka automatiquement.
"""

import os
import time
import logging
import schedule
from datetime import datetime
from dotenv import load_dotenv

from pubmed_collector import PubMedCollector
from arxiv_collector import ArxivCollector

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Requetes de recherche — domaines cibles du projet
SEARCH_QUERIES = [
    "bioinformatics deep learning",
    "genomics transformer model",
    "protein structure prediction",
    "CRISPR gene editing machine learning",
    "RNA sequencing analysis",
    "variant calling deep learning",
    "single cell RNA seq",
    "drug discovery neural network",
]

PUBMED_MAX_PER_QUERY = int(os.getenv("PUBMED_MAX_RESULTS_PER_QUERY", "100"))
ARXIV_MAX_PER_QUERY  = int(os.getenv("ARXIV_MAX_RESULTS_PER_QUERY", "50"))


def run_pubmed_collection():
    """Lance une collecte PubMed complete."""
    logger.info("=== DEBUT COLLECTE PUBMED ===")
    try:
        collector = PubMedCollector()
        collector.collect_and_send(SEARCH_QUERIES, max_per_query=PUBMED_MAX_PER_QUERY)
        collector.close()
        logger.info("=== FIN COLLECTE PUBMED ===")
    except Exception as e:
        logger.error(f"Erreur collecte PubMed: {e}")


def run_arxiv_collection():
    """Lance une collecte arXiv complete."""
    logger.info("=== DEBUT COLLECTE ARXIV ===")
    try:
        collector = ArxivCollector()
        collector.collect_and_send(SEARCH_QUERIES[:4], max_per_query=ARXIV_MAX_PER_QUERY)
        collector.close()
        logger.info("=== FIN COLLECTE ARXIV ===")
    except Exception as e:
        logger.error(f"Erreur collecte arXiv: {e}")


def main():
    logger.info("Collecteur demarre")
    logger.info(f"Requetes configurees: {len(SEARCH_QUERIES)}")

    # Lancer une premiere collecte immediate au demarrage
    logger.info("Collecte initiale au demarrage...")
    run_pubmed_collection()
    run_arxiv_collection()

    # Planifier les collectes periodiques
    # PubMed : toutes les heures
    schedule.every(60).minutes.do(run_pubmed_collection)
    # arXiv : toutes les 2 heures
    schedule.every(120).minutes.do(run_arxiv_collection)

    logger.info("Scheduler configure — collecte toutes les 60/120 min")

    # Boucle principale
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()