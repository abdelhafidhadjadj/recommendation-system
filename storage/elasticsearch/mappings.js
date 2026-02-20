"""
Elasticsearch — Mappings et initialisation des index
Système de recommandation d'articles scientifiques
USTHB — PFE Master Bioinformatique 2025/2026

Ce fichier définit la structure des index Elasticsearch,
notamment le champ dense_vector pour les embeddings BioBERT.
"""

import os
import logging
import time
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# CONNEXION ELASTICSEARCH
# ============================================================
ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX_ARTICLES = os.getenv("ES_INDEX_ARTICLES", "articles")
ES_EMBEDDING_DIMS = int(os.getenv("ES_EMBEDDING_DIMS", "768"))


def get_es_client() -> Elasticsearch:
    """Crée et retourne un client Elasticsearch."""
    client = Elasticsearch(
        ES_URL,
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=3
    )
    return client


def wait_for_elasticsearch(es: Elasticsearch, max_retries: int = 20):
    """
    Attend qu'Elasticsearch soit prêt.
    Utile au démarrage Docker quand ES met ~30s à démarrer.
    """
    for attempt in range(max_retries):
        try:
            health = es.cluster.health(wait_for_status='yellow', timeout='10s')
            logger.info(f"✅ Elasticsearch prêt — statut : {health['status']}")
            return True
        except Exception as e:
            logger.warning(f"⏳ Tentative {attempt+1}/{max_retries} — ES pas encore prêt : {e}")
            time.sleep(5)
    raise ConnectionError("❌ Elasticsearch inaccessible après plusieurs tentatives")


# ============================================================
# MAPPING : INDEX ARTICLES
# C'est le mapping le plus important du projet
# Le champ "embedding" est le cœur de la recherche vectorielle
# ============================================================
ARTICLES_MAPPING = {
    "settings": {
        "number_of_shards": 3,       # 3 shards = traitement parallèle
        "number_of_replicas": 0,     # 0 réplique en dev (1 en prod)

        # Analyseur spécialisé pour le texte scientifique
        "analysis": {
            "analyzer": {
                "scientific_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "stop",
                        "snowball",      # racinisation (stemming)
                        "asciifolding"   # normalise les accents
                    ]
                }
            }
        },

        # Configuration de la recherche kNN (vecteurs)
        # HNSW = Hierarchical Navigable Small World
        # C'est l'algorithme ANN utilisé par Elasticsearch
        "index": {
            "knn": True,
            "knn.algo_param.ef_search": 100  # précision vs vitesse
        }
    },

    "mappings": {
        "properties": {

            # ── Identifiants ──────────────────────────────────
            "id": {
                "type": "keyword",  # keyword = valeur exacte, pas analysée
                "doc_values": True
            },
            "source": {
                "type": "keyword"  # pubmed | arxiv | s2orc
            },

            # ── Contenu textuel ───────────────────────────────
            "title": {
                "type": "text",
                "analyzer": "scientific_analyzer",
                # "fields" permet d'avoir deux représentations :
                # title = full-text search
                # title.keyword = valeur exacte (pour tri/agrégations)
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 512
                    }
                }
            },
            "abstract": {
                "type": "text",
                "analyzer": "scientific_analyzer"
            },

            # ── Auteurs ───────────────────────────────────────
            "authors": {
                "type": "keyword"  # Liste de strings exacts
            },

            # ── Mots-clés ─────────────────────────────────────
            "keywords": {
                "type": "keyword"
            },
            "keywords_extracted": {
                "type": "keyword",  # Mots-clés extraits par KeyBERT
                "doc_values": True
            },

            # ── Dates et métriques ────────────────────────────
            "publication_date": {
                "type": "date",
                "format": "yyyy-MM-dd||yyyy-MM||yyyy||epoch_millis"
            },
            "journal": {
                "type": "keyword"
            },
            "citations_count": {
                "type": "integer",
                "doc_values": True
            },
            "domain": {
                "type": "keyword"
            },

            # ── DOI ───────────────────────────────────────────
            "doi": {
                "type": "keyword"
            },

            # ── Champs spécifiques PubMed ─────────────────────
            "pmid": {
                "type": "keyword"
            },
            "mesh_terms": {
                "type": "keyword"
            },

            # ── Champs spécifiques arXiv ─────────────────────
            "arxiv_id": {
                "type": "keyword"
            },
            "arxiv_categories": {
                "type": "keyword"
            },

            # ── Modèle d'embedding utilisé ────────────────────
            "embedding_model": {
                "type": "keyword"  # biobert | scibert | pubmedbert
            },

            # ════════════════════════════════════════════════════
            # CHAMP VECTORIEL — LE PLUS IMPORTANT DU MAPPING
            #
            # dense_vector stocke l'embedding BioBERT/SciBERT
            # de l'article (768 dimensions)
            #
            # index: true → active la recherche kNN HNSW
            # similarity: cosine → mesure de similarité cosinus
            #   (distance angulaire entre vecteurs)
            #   Cosinus est préféré à euclidienne pour les embeddings
            #   de texte car il mesure l'orientation, pas la magnitude
            # ════════════════════════════════════════════════════
            "embedding": {
                "type": "dense_vector",
                "dims": ES_EMBEDDING_DIMS,   # 768 pour BERT
                "index": True,
                "similarity": "cosine",
                "index_options": {
                    "type": "hnsw",
                    "m": 16,            # nb de connexions par nœud HNSW
                    "ef_construction": 100  # précision lors de l'indexation
                }
            }
        }
    }
}

# ============================================================
# MAPPING : INDEX USER PROFILES
# Profils vectoriels des utilisateurs pour recherche rapide
# ============================================================
USER_PROFILES_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "user_id": {
                "type": "keyword"
            },
            "embedding_model": {
                "type": "keyword"
            },
            "articles_count": {
                "type": "integer"
            },
            "top_domains": {
                "type": "keyword"
            },
            "top_keywords": {
                "type": "keyword"
            },
            "computed_at": {
                "type": "date"
            },
            # Vecteur profil de l'utilisateur
            "profile_vector": {
                "type": "dense_vector",
                "dims": ES_EMBEDDING_DIMS,
                "index": True,
                "similarity": "cosine"
            }
        }
    }
}


# ============================================================
# FONCTIONS D'INITIALISATION
# ============================================================

def create_index(es: Elasticsearch, index_name: str, mapping: dict) -> bool:
    """
    Crée un index si il n'existe pas déjà.
    Retourne True si créé, False si déjà existant.
    """
    try:
        if es.indices.exists(index=index_name):
            logger.info(f"📋 Index '{index_name}' existe déjà — pas de recréation")
            return False

        es.indices.create(index=index_name, body=mapping)
        logger.info(f"✅ Index '{index_name}' créé avec succès")
        return True

    except Exception as e:
        logger.error(f"❌ Erreur création index '{index_name}': {e}")
        raise


def delete_and_recreate_index(es: Elasticsearch, index_name: str, mapping: dict):
    """
    Supprime et recrée un index.
    ATTENTION : supprime toutes les données !
    Utile seulement en développement pour repartir de zéro.
    """
    try:
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            logger.warning(f"🗑️  Index '{index_name}' supprimé")

        es.indices.create(index=index_name, body=mapping)
        logger.info(f"✅ Index '{index_name}' recréé")

    except Exception as e:
        logger.error(f"❌ Erreur recréation index '{index_name}': {e}")
        raise


def setup_all_indexes(recreate: bool = False):
    """
    Initialise tous les index Elasticsearch nécessaires.

    Args:
        recreate: Si True, supprime et recrée les index existants.
                  Mettre True seulement en développement !
    """
    es = get_es_client()

    try:
        wait_for_elasticsearch(es)

        indexes = [
            (ES_INDEX_ARTICLES, ARTICLES_MAPPING),
            ("user_profiles_es", USER_PROFILES_MAPPING),
        ]

        for index_name, mapping in indexes:
            if recreate:
                delete_and_recreate_index(es, index_name, mapping)
            else:
                create_index(es, index_name, mapping)

        # Vérification finale
        logger.info("\n📊 État des index Elasticsearch :")
        for index_name, _ in indexes:
            if es.indices.exists(index=index_name):
                stats = es.indices.stats(index=index_name)
                doc_count = stats['_all']['primaries']['docs']['count']
                logger.info(f"   {index_name}: {doc_count} documents")

    finally:
        es.close()


def get_index_stats(index_name: str = None) -> dict:
    """Retourne les statistiques d'un index ou de tous les index."""
    es = get_es_client()
    try:
        target = index_name if index_name else "_all"
        return es.indices.stats(index=target)
    finally:
        es.close()


# ============================================================
# EXÉCUTION DIRECTE
# python storage/elasticsearch/mappings.py
# ============================================================
if __name__ == "__main__":
    import sys

    recreate = "--recreate" in sys.argv
    if recreate:
        logger.warning("⚠️  Mode RECREATE activé — les données existantes seront supprimées !")

    setup_all_indexes(recreate=recreate)
    logger.info("✅ Elasticsearch configuré avec succès")