# Cadrage du projet — Flux de ventes

## Cas d'usage

Flux de transactions de caisse en magasin. Chaque événement = un ticket de caisse.
Le flux est produit en continu et agrégé par fenêtre temporelle et par magasin.

## Topic d'entrée

- Nom : `sales-events`
- Clé Kafka : `store_id` (garantit l'ordre par magasin)
- Partitions : 6 (à ajuster selon le parallélisme des moteurs)

## Topic de sortie

- Spark : `sales-aggregated-spark`
- Flink : `sales-aggregated-flink`
- Clé Kafka : `store_id`
- Format : JSON

## Schéma d'un événement d'entrée

| Champ | Type | Rôle |
|---|---|---|
| `event_id` | string (UUID) | Identifiant unique, détection des doublons |
| `event_time` | string (ISO 8601 UTC) | Temps événementiel (fenêtres, watermark) |
| `emit_time` | string (ISO 8601 UTC) | Temps d'émission (mesure de latence uniquement) |
| `store_id` | string (`S001`…`S050`) | Clé de partitionnement, axe d'agrégation |
| `product_id` | string (`P0001`…) | Produit vendu |
| `category` | string | Catégorie (electronics, food, clothing, home) |
| `quantity` | int (1–5) | Quantité vendue |
| `unit_price` | double (5.00–200.00) | Prix unitaire |
| `payment_method` | string | card, cash, mobile |

## Exemple d'événement

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_time": "2026-10-23T14:32:11.123Z",
  "emit_time": "2026-10-23T14:32:11.450Z",
  "store_id": "S042",
  "product_id": "P1234",
  "category": "electronics",
  "quantity": 2,
  "unit_price": 19.99,
  "payment_method": "card"
}