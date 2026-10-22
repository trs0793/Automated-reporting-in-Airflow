# Allegro Daily Analytics

**Dokumentacja aplikacji · wersja 1.0.0**

Allegro Daily Analytics to projekt wewnętrznego raportowania dla własnego konta sprzedawcy Allegro. Jego celem jest przygotowywanie dziennych zestawień zamówień, sprzedaży, opłat oraz rozliczeń na podstawie danych udostępnionych przez Allegro REST API.

## Zakres

Pierwszy etap integracji przewiduje odczyt danych zamówień, ofert, płatności i rozliczeń. Dalszy plan obejmuje dane One Fulfillment, przesyłki oraz odczyt wiadomości i spraw kupujących, w zakresie dostępnych zasobów API i udzielonych uprawnień.

Automatyczne wysyłanie wiadomości, zmiany ofert i zwroty środków nie należą do pierwszego etapu.

## Autoryzacja

Dostęp wymaga autoryzacji właściciela konta przez OAuth 2.0 Allegro. Żądania API mają używać indywidualnego nagłówka User-Agent wygenerowanego dla tej aplikacji. Ta strona jest przeznaczona jako publiczny opis jej celu i zakresu.

## Dane

Strona dokumentacji zawiera wyłącznie opis aplikacji. Nie zawiera danych kupujących, zamówień, wyników sprzedaży, kluczy aplikacji ani tokenów. Kod oraz konfiguracja projektu są utrzymywane oddzielnie.

## Status

**Konfiguracja początkowa.** Trwa przygotowanie integracji. Autoryzacja konta, pobieranie danych i regularne raportowanie wymagają dalszego wdrożenia oraz weryfikacji.

## Opiekun projektu

[trs0793 na GitHub](https://github.com/trs0793)

## Oficjalne materiały Allegro

- [Podstawy API i User-Agent](https://developer.allegro.pl/tutorials/informacje-podstawowe-b21569boAI1)
- [OAuth](https://developer.allegro.pl/tutorials/uwierzytelnianie-i-autoryzacja-zlq9e75GdIR)
- [Dokumentacja API](https://developer.allegro.pl/documentation)
