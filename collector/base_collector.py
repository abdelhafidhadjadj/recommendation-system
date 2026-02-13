"""
Base Collector — Classe abstraite commune
USTHB — PFE Master Bioinformatique 2025/2026
"""

import os
import re
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from kafka_producer import ArticleProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)


class BaseCollector(ABC):
    """
    Classe de base pour tous les collecteurs.
    Chaque collecteur enfant implemente : collect() et parse_item()
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger = logging.getLogger(f"collector.{source_name}")
        self.producer = None
        self.session_stats = {
            "collected": 0,
            "sent_to_kafka": 0,
            "errors": 0,
            "start_time": datetime.utcnow().isoformat()
        }

    def connect_kafka(self):
        self.producer = ArticleProducer()
        self.logger.info("Connexion Kafka etablie")

    def disconnect_kafka(self):
        if self.producer:
            self.producer.close()
            self.producer = None

    @abstractmethod
    def collect(self, query: str, max_results: int = 100) -> list:
        """Collecte des articles. Retourne liste de dicts unifies."""
        pass

    @abstractmethod
    def parse_item(self, raw_item) -> Optional[dict]:
        """Transforme un item brut en dict unifie. Retourne None si erreur."""
        pass

    def send_to_kafka(self, articles: list) -> dict:
        if not self.producer:
            self.connect_kafka()
        stats = self.producer.send_batch(articles)
        self.session_stats["sent_to_kafka"] += stats.get("sent", 0)
        return stats

    def collect_and_send(self, query: str, max_results: int = 100) -> dict:
        """Pipeline complet : collecte + envoi Kafka."""
        self.logger.info(f"Collecte: query='{query}' max={max_results}")
        try:
            articles = self.collect(query, max_results)
            self.session_stats["collected"] += len(articles)
            self.logger.info(f"{len(articles)} articles collectes")

            if not articles:
                return {"collected": 0, "sent": 0, "failed": 0, "skipped": 0}

            kafka_stats = self.send_to_kafka(articles)
            return {
                "collected": len(articles),
                "sent": kafka_stats.get("sent", 0),
                "failed": kafka_stats.get("failed", 0),
                "skipped": kafka_stats.get("skipped", 0)
            }
        except Exception as e:
            self.logger.error(f"Erreur: {e}")
            self.session_stats["errors"] += 1
            raise

    def clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text.strip())
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
        return text

    def make_article_id(self, source: str, raw_id: str) -> str:
        clean_id = str(raw_id).replace("/", "_").replace(".", "_").strip()
        return f"{source}_{clean_id}"

    def get_session_stats(self) -> dict:
        stats = {**self.session_stats}
        stats["end_time"] = datetime.utcnow().isoformat()
        return stats

    def __enter__(self):
        self.connect_kafka()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect_kafka()
        self.logger.info(f"Session terminee. Stats: {self.get_session_stats()}")