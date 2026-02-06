Bankier.pl – Automatyczny kanał RSS + JSON Feed (News + Giełda)

Ten projekt generuje automatyczne kanały RSS 2.0 oraz JSON Feed 1.0 z serwisu Bankier.pl:

📰 „Wiadomości”

👉 https://www.bankier.pl/wiadomosc/

📈 „Giełda – Wiadomości”

👉 https://www.bankier.pl/gielda/wiadomosci/

Skrypt w Pythonie:

✔ pobiera artykuły z pierwszych 5 stron obu działów
✔ wyciąga tytuł, link, datę i zajawkę (jeśli dostępna)
✔ filtruje newsy do ostatnich 48 godzin
✔ generuje:

RSS 2.0 (biblioteka feedgen)

JSON Feed 1.0 (kompatybilny ze stylem …/view/json np. Inoreader)

✔ działa automatycznie przez GitHub Actions
✔ wynik publikowany jest jako statyczne pliki przez GitHub Pages

🔗 Adresy feedów
📰 Wiadomości – RSS

https://bbttoonnee-commits.github.io/rrs_json/bankier-rss.xml

📰 Wiadomości – JSON Feed

https://bbttoonnee-commits.github.io/rrs_json/bankier-feed.json

📈 Giełda – RSS

https://bbttoonnee-commits.github.io/rrs_json/bankier-gielda-rss.xml

📈 Giełda – JSON Feed

https://bbttoonnee-commits.github.io/rrs_json/bankier-gielda-feed.json

📁 Struktura projektu
.
├── bankier_rss.py              # Skrypt generujący RSS i JSON
├── requirements.txt
├── docs/
│   ├── bankier-rss.xml         # RSS – Wiadomości
│   ├── bankier-feed.json       # JSON – Wiadomości
│   ├── bankier-gielda-rss.xml  # RSS – Giełda
│   └── bankier-gielda-feed.json# JSON – Giełda
└── .github/
    └── workflows/
        └── bankier-rss.yml     # Workflow GitHub Actions

⚙️ Jak działa skrypt
Pobiera HTML z:
📰 Wiadomości
/wiadomosc/
/wiadomosc/2
/wiadomosc/3
/wiadomosc/4
/wiadomosc/5

📈 Wiadomości giełdowe
/gielda/wiadomosci/
/gielda/wiadomosci/2
/gielda/wiadomosci/3
/gielda/wiadomosci/4
/gielda/wiadomosci/5

Dla każdego artykułu zapisywane są:

✔ tytuł
✔ pełny link
✔ data publikacji / aktualizacji (używana nowsza)
✔ zajawka (jeśli jest — bez „Czytaj dalej”)

Czas jest konwertowany do Europe/Warsaw (CET/CEST).

➡️ Do feedów trafiają tylko artykuły z ostatnich 48h.

📡 Tworzone są dwa formaty feedów
RSS 2.0

GUID = pełny URL artykułu

description = zajawka

pubDate = data z timezone

JSON Feed 1.0

Każdy wpis ma m.in.:

{
  "id": "<URL>",
  "url": "<URL>",
  "title": "Tytuł",
  "content_html": "Zajawka",
  "date_published": "2025-12-30T21:09:00+01:00"
}

Pliki zapisywane są do:
docs/bankier-rss.xml
docs/bankier-feed.json
docs/bankier-gielda-rss.xml
docs/bankier-gielda-feed.json

🔧 Konfiguracja

W bankier_rss.py możesz zmienić:

NUM_PAGES_NEWS = 5
NUM_PAGES_GIELDA = 5
HOURS_BACK = 48
SLEEP_BETWEEN_REQUESTS = 2.5


np.:

HOURS_BACK = 72
NUM_PAGES_NEWS = 3


Nagłówki HTTP ustawisz w HEADERS.

▶️ Ręczne uruchomienie

Instalacja zależności:

pip install requests beautifulsoup4 feedgen pytz


lub:

pip install -r requirements.txt

Generowanie RSS — Wiadomości
python bankier_rss.py rss > docs/bankier-rss.xml

Generowanie JSON — Wiadomości
python bankier_rss.py json > docs/bankier-feed.json

Generowanie RSS — Giełda
python bankier_rss.py rss gielda > docs/bankier-gielda-rss.xml

Generowanie JSON — Giełda
python bankier_rss.py json gielda > docs/bankier-gielda-feed.json

🤖 GitHub Actions

Workflow:

.github/workflows/bankier-rss.yml


Uruchamia się:

✔ co 30 minut
✔ lub ręcznie

Commit tworzony jest tylko jeśli pliki się zmieniły.

🌐 GitHub Pages

Włącz w repozytorium:

Settings → Pages

Source: Deploy from a branch

Branch: main

Folder: /docs

📡 Finalne adresy feedów
📰 RSS

https://bbttoonnee-commits.github.io/rrs_json/bankier-rss.xml

📰 JSON

https://bbttoonnee-commits.github.io/rrs_json/bankier-feed.json

📈 RSS

https://bbttoonnee-commits.github.io/rrs_json/bankier-gielda-rss.xml

📈 JSON

https://bbttoonnee-commits.github.io/rrs_json/bankier-gielda-feed.json

🛠 Troubleshooting
❌ Brak pliku RSS/JSON

✔ sprawdź logi Actions
✔ upewnij się, że commit się wykonał

🕑 Brak nowych artykułów

✔ działa filtr ostatnich 48h
➡️ zmień HOURS_BACK

🚦 „Too many requests”

✔ zwiększ:

SLEEP_BETWEEN_REQUESTS

🚀 Pomysły na rozwój

parametryzacja przez zmienne środowiskowe

pobieranie pełnej treści artykułu

cache HTTP

walidacja XML/JSON w workflow

paginacja JSON Feed

integracja z Telegram bot / webhook

automatyczne testy

🧩 Technologie

Python

GitHub Actions

GitHub Pages

➡️ bez własnego serwera — w pełni serverless
