"""
Kafka Producer — Systeme de Recommandation Scientifique
USTHB — PFE Master Bioinformatique 2025/2026
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ["id", "source", "title", "collected_at"]
VALID_SOURCES = ["pubmed", "arxiv", "s2orc", "manual"]


def validate_article(article: dict) -> tuple:
    for field in REQUIRED_FIELDS:
        if field not in article or not article[field]:
            return False, f"Champ manquant : '{field}'"
    if article["source"] not in VALID_SOURCES:
        return False, f"Source invalide : '{article['source']}'"
    if not article.get("title", "").strip():
        return False, "Titre vide"
    return True, ""


class ArticleProducer:
    """
    Producteur Kafka pour les articles scientifiques.

    Usage avec context manager :
        with ArticleProducer() as producer:
            producer.send_article(article_dict)
            producer.send_batch(articles_list)
    """

    def __init__(self, broker=None, topic_raw=None, max_retries=5, retry_delay=5):
        self.broker = broker or os.getenv("KAFKA_BROKER_EXTERNAL", "localhost:9092")
        self.topic_raw = topic_raw or os.getenv("KAFKA_TOPIC_ARTICLES_RAW", "articles.raw")
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.producer = None
        self.stats = {"sent": 0, "failed": 0, "skipped": 0}
        self._connect()

    def _connect(self):
        """Connexion Kafka avec retry automatique."""
        for attempt in range(1, self.max_retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.broker,
                    value_serializer=lambda v: json.dumps(
                        v, ensure_ascii=False, default=str
                    ).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                    max_request_size=10485760,
                    request_timeout_ms=30000,
                    api_version_auto_timeout_ms=30000,
                )
                logger.info(f"Kafka connecte sur {self.broker}")
                return
            except NoBrokersAvailable:
                logger.warning(f"Tentative {attempt}/{self.max_retries} — attente {self.retry_delay}s...")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    raise ConnectionError(f"Impossible de connecter a Kafka sur {self.broker}")

    def send_article(self, article: dict, topic=None) -> bool:
        """Envoie un article vers Kafka. Retourne True si succes."""
        is_valid, error_msg = validate_article(article)
        if not is_valid:
            logger.warning(f"Article ignore [{article.get('id', '?')}]: {error_msg}")
            self.stats["skipped"] += 1
            return False

        target_topic = topic or self.topic_raw
        article_id = article["id"]

        try:
            future = self.producer.send(
                topic=target_topic,
                key=article_id,
                value=article
            )
            record = future.get(timeout=10)
            logger.info(
                f"Envoye [{article_id}] -> "
                f"partition={record.partition} offset={record.offset}"
            )
            self.stats["sent"] += 1
            return True

        except KafkaError as e:
            logger.error(f"Erreur Kafka [{article_id}]: {e}")
            self.stats["failed"] += 1
            return False

        except Exception as e:
            logger.error(f"Erreur inattendue [{article_id}]: {e}")
            self.stats["failed"] += 1
            return False

    def send_batch(self, articles: list, topic=None) -> dict:
        """Envoie une liste d'articles. Retourne les stats."""
        if not articles:
            return {"sent": 0, "failed": 0, "skipped": 0}

        logger.info(f"Envoi batch de {len(articles)} articles...")
        batch_stats = {"sent": 0, "failed": 0, "skipped": 0}

        for article in articles:
            is_valid, _ = validate_article(article)
            if not is_valid:
                batch_stats["skipped"] += 1
                self.send_article(article, topic)
            elif self.send_article(article, topic):
                batch_stats["sent"] += 1
            else:
                batch_stats["failed"] += 1

        self.producer.flush()
        logger.info(f"Batch termine: {batch_stats}")
        return batch_stats

    def send_user_interaction(self, interaction: dict) -> bool:
        """Envoie une interaction utilisateur vers users.interactions."""
        topic = os.getenv("KAFKA_TOPIC_USER_INTERACTIONS", "users.interactions")
        try:
            interaction["timestamp"] = interaction.get(
                "timestamp", datetime.utcnow().isoformat()
            )
            self.producer.send(topic=topic, value=interaction)
            self.producer.flush()
            return True
        except Exception as e:
            logger.error(f"Erreur envoi interaction: {e}")
            return False

    def get_stats(self) -> dict:
        return {**self.stats, "broker": self.broker, "topic": self.topic_raw}

    def flush(self):
        if self.producer:
            self.producer.flush()

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Producer ferme. Stats: {self.stats}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def create_article_template(source: str) -> dict:
    """Retourne un template vide pour un article."""
    return {
        "id": "",
        "source": source,
        "title": "",
        "abstract": "",
        "authors": [],
        "keywords": [],
        "publication_date": "",
        "journal": "",
        "doi": "",
        "citations_count": 0,
        "references": [],
        "pmid": None,
        "mesh_terms": [],
        "arxiv_id": None,
        "arxiv_categories": [],
        "language": "en",
        "collected_at": datetime.utcnow().isoformat(),
        "processing_status": "raw"
    }


# ============================================================
# TEST STANDALONE : python kafka_producer.py
# ============================================================
if __name__ == "__main__":
    import sys

    print("\n" + "="*50)
    print("  TEST KAFKA PRODUCER — USTHB")
    print("="*50)

    try:
        with ArticleProducer() as producer:

            # Test 1 — article valide
            print("\n[Test 1] Article valide...")
            article = {
                "id": "pubmed_test_kafka_001",
                "source": "pubmed",
                "title": "BERT for genomic variant detection",
                "abstract": "We present a BERT-based approach for variant calling.",
                "authors": ["Zhang Y.", "Li M."],
                "keywords": ["BERT", "genomics"],
                "publication_date": "2025-01-01",
                "journal": "Bioinformatics",
                "citations_count": 0,
                "pmid": "test_001",
                "mesh_terms": ["Deep Learning"],
                "arxiv_id": None,
                "arxiv_categories": [],
                "language": "en",
                "collected_at": datetime.utcnow().isoformat(),
                "processing_status": "raw"
            }
            ok = producer.send_article(article)
            print(f"  -> {'OK' if ok else 'ECHEC'}")

            # Test 2 — article invalide
            print("\n[Test 2] Article invalide (titre vide)...")
            invalide = {
                "id": "test_invalide",
                "source": "pubmed",
                "title": "",
                "collected_at": datetime.utcnow().isoformat()
            }
            ok = producer.send_article(invalide)
            print(f"  -> {'REJETE comme prevu' if not ok else 'ERREUR'}")

            # Test 3 — batch
            print("\n[Test 3] Batch de 3 articles...")
            batch = [
                {**article,
                 "id": f"pubmed_batch_00{i}",
                 "title": f"Batch article {i}",
                 "collected_at": datetime.utcnow().isoformat()}
                for i in range(2, 5)
            ]
            stats = producer.send_batch(batch)
            print(f"  -> Stats: {stats}")

            print(f"\n Stats globales: {producer.get_stats()}")

        print("\n" + "="*50)
        print("  SUCCES — Verifiez http://localhost:8090")
        print("  Topic: articles.raw")
        print("="*50 + "\n")
        sys.exit(0)

    except ConnectionError as e:
        print(f"\nERREUR: {e}")
        print("Verifiez: docker compose ps kafka")
        sys.exit(1)