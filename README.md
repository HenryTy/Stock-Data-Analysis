# Stock-Data-Analysis

Projekt realizowany w ramach przedmiotu Przetwarzanie Big Data. Celem projektu była implementacja rozwiązania dokonującego przetwarzania danych strumieniowych
dotyczących notowań spółek giełdowych.

## Architektura rozwiązania
![alt text](https://github.com/HenryTy/Stock-Data-Analysis/blob/master/images/diagram.png)

Dane źródłowe mają postać plików csv. Producent Kafki odczytuje zawartość kolejnych
plików i wysyła je, linia po linii, do brokera Kafki symulując w ten sposób zachodzenie zdarzeń w
świecie rzeczywistym. Następnie program Kafka Streams odczytuje te dane z serwera Kafki i utrzymuje na ich podstawie obraz czasu rzeczywistego
oraz rejestruje wystąpienia "anomalii".

Obraz czasu rzeczywistego to zagregowane wartości na poziomie miesiąca oraz symbolu i nazwy spółki giełdowej:
- średnia wartość kursu zamknięcia
- najmniejsza wartość akcji
- największa wartość akcji
- sumaryczny obrót

Wykrywanie "anomalii" polega na wykrywaniu znaczących wahań kursu akcji danej spółki, które zostaną
zarejestrowane w ciągu podanego czasu.

Wyniki te wysyłane są do tematów Kafki, a następnie zapisywane do indeksów Elasticsearcha za pomocą ElasticsearchSinkConnectora.
