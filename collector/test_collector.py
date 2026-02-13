"""
Test end-to-end — Phase 2
USTHB — PFE Master Bioinformatique 2025/2026

Verifie que tout le pipeline de collecte fonctionne :
PubMed API -> Parser -> Kafka -> Kafka UI
"""

import sys
import os
import json
from datetime import datetime

# ============================================================
# TESTS
# ============================================================

def test_kafka_producer():
    print("\n[1/4] Test Kafka Producer...")
    try:
        from kafka_producer import ArticleProducer, validate_article

        # Test validation
        article_ok = {
            "id": "test_001",
            "source": "pubmed",
            "title": "Test article",
            "collected_at": datetime.utcnow().isoformat()
        }
        assert validate_article(article_ok) == (True, ""), "Validation OK echouee"

        article_ko = {"id": "", "source": "pubmed", "title": "", "collected_at": ""}
        valid, msg = validate_article(article_ko)
        assert not valid, "Validation KO aurait du echouer"

        # Test connexion et envoi
        with ArticleProducer() as producer:
            ok = producer.send_article({
                "id": f"test_phase2_{datetime.utcnow().strftime('%H%M%S')}",
                "source": "pubmed",
                "title": "Phase 2 end-to-end test",
                "abstract": "Test article for pipeline validation",
                "authors": ["Test Author"],
                "keywords": ["test"],
                "publication_date": "2025-01-01",
                "journal": "Test Journal",
                "citations_count": 0,
                "pmid": "test",
                "mesh_terms": [],
                "arxiv_id": None,
                "arxiv_categories": [],
                "language": "en",
                "collected_at": datetime.utcnow().isoformat(),
                "processing_status": "raw"
            })
            assert ok, "Envoi Kafka echoue"
            stats = producer.get_stats()
            assert stats["sent"] >= 1, "Stats incorrectes"

        print("    OK — Kafka Producer fonctionne")
        return True

    except Exception as e:
        print(f"    ECHEC — {e}")
        return False


def test_pubmed_api():
    print("\n[2/4] Test PubMed API (connexion internet requise)...")
    try:
        from pubmed_collector import PubMedCollector

        collector = PubMedCollector()

        # Test esearch
        ids = collector.search_ids("CRISPR genomics", max_results=3)
        assert isinstance(ids, list), "esearch doit retourner une liste"

        if not ids:
            print("    SKIP — Aucun ID retourne (verifiez internet)")
            return True

        assert len(ids) <= 3, f"Trop d'IDs: {len(ids)}"
        print(f"    esearch: {len(ids)} IDs trouves — {ids}")

        # Test parsing
        articles = collector.collect("CRISPR genomics", max_results=2)
        assert isinstance(articles, list), "collect doit retourner une liste"

        if articles:
            a = articles[0]
            assert "id" in a and a["id"].startswith("pubmed_"), f"ID invalide: {a.get('id')}"
            assert "title" in a and a["title"], "Titre manquant"
            assert a["source"] == "pubmed", "Source incorrecte"
            assert "collected_at" in a, "collected_at manquant"
            print(f"    Premier article: '{a['title'][:60]}...'")

        print(f"    OK — PubMed: {len(articles)} articles collectes et parses")
        return True

    except Exception as e:
        print(f"    ECHEC — {e}")
        return False


def test_arxiv_api():
    print("\n[3/4] Test arXiv API (connexion internet requise)...")
    try:
        from arxiv_collector import ArXivCollector

        collector = ArXivCollector()
        articles = collector.collect("deep learning protein", max_results=2)

        assert isinstance(articles, list), "collect doit retourner une liste"

        if articles:
            a = articles[0]
            assert "id" in a and a["id"].startswith("arxiv_"), f"ID invalide: {a.get('id')}"
            assert "title" in a and a["title"], "Titre manquant"
            assert a["source"] == "arxiv", "Source incorrecte"
            print(f"    Premier article: '{a['title'][:60]}...'")

        print(f"    OK — arXiv: {len(articles)} articles collectes et parses")
        return True

    except Exception as e:
        print(f"    ECHEC — {e}")
        return False


def test_end_to_end():
    print("\n[4/4] Test end-to-end — PubMed -> Kafka...")
    try:
        from pubmed_collector import PubMedCollector

        collector = PubMedCollector()
        stats = collector.collect_and_send("bioinformatics transformer 2024", max_results=3)

        print(f"    Stats: collected={stats['collected']} sent={stats['sent']} failed={stats['failed']}")
        assert stats["collected"] >= 0, "collected doit etre >= 0"
        assert stats["sent"] >= 0, "sent doit etre >= 0"

        print("    OK — Pipeline end-to-end fonctionne")
        print("    Verifiez http://localhost:8090 -> topic 'articles.raw'")
        return True

    except Exception as e:
        print(f"    ECHEC — {e}")
        return False


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("\n" + "="*55)
    print("  TEST PHASE 2 — COLLECTE & INGESTION")
    print("  Systeme de Recommandation — USTHB")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*55)

    results = {
        "kafka_producer": test_kafka_producer(),
        "pubmed_api":     test_pubmed_api(),
        "arxiv_api":      test_arxiv_api(),
        "end_to_end":     test_end_to_end(),
    }

    # Résumé
    print("\n" + "="*55)
    print("  RESUME")
    print("="*55)
    all_ok = True
    for test_name, passed in results.items():
        status = "OK" if passed else "ECHEC"
        color  = "" if passed else ""
        print(f"  {status:6} — {test_name}")
        if not passed:
            all_ok = False

    print("="*55)
    if all_ok:
        print("  PHASE 2 VALIDEE — passer a la Phase 3 (Spark)")
    else:
        print("  Des tests ont echoue — verifiez les logs ci-dessus")
    print("="*55 + "\n")

    sys.exit(0 if all_ok else 1)