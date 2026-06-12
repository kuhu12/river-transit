# Dam Year Enrichment Report

## Summary

| Metric | Count | % of Total |
|--------|-------|------------|
| Total dams in JSON | 781 | 100% |
| Matched via NRLD 2019 | 685 | 87.7% |
| Matched via Wikipedia | 29 | 3.7% |
| **Total with year** | **714** | **91.4%** |
| NRLD matched but year blank in PDF | 0 | 0.0% |
| **Missing (no year found)** | **67** | **8.6%** |

## Data Sources

- **NRLD 2019**: National Register of Large Dams, Central Water Commission, June 2019.
  Contains 5,745 dams across India. Extracted programmatically using pdfplumber.
  Ganga basin records identified by "River Basin = Ganga" in the PDF tables.

- **Wikipedia**: Manual lookup for major dams not matched in NRLD, or where NRLD
  year was blank. Covers ~68 well-known dams.

- **JSON source**: `ganga_dams_detail.json` — 781 curated Ganga basin dams with
  coordinates, capacity, but no construction year.

## Coverage by State

| State | Total | NRLD Matched | Wikipedia | Missing | % Coverage |
|-------|-------|-------------|-----------|---------|------------|
| Bihar | 26 | 18 | 7 | 1 | 96.2% |
| Chhattisgarh | 10 | 8 | 1 | 1 | 90.0% |
| Jharkhand | 26 | 15 | 3 | 8 | 69.2% |
| Madhya Pradesh | 440 | 408 | 3 | 29 | 93.4% |
| Rajasthan | 128 | 105 | 4 | 19 | 85.2% |
| Uttar Pradesh | 123 | 113 | 2 | 8 | 93.5% |
| Uttarakhand | 24 | 14 | 9 | 1 | 95.8% |
| West Bengal | 4 | 4 | 0 | 0 | 100.0% |

## Year Distribution (matched dams)

| Decade | Count |
|--------|-------|
| 1550s | 1 |
| 1580s | 1 |
| 1610s | 1 |
| 1620s | 1 |
| 1670s | 1 |
| 1690s | 3 |
| 1840s | 1 |
| 1860s | 2 |
| 1870s | 2 |
| 1880s | 6 |
| 1890s | 5 |
| 1900s | 14 |
| 1910s | 49 |
| 1920s | 11 |
| 1930s | 13 |
| 1940s | 1 |
| 1950s | 62 |
| 1960s | 98 |
| 1970s | 126 |
| 1980s | 174 |
| 1990s | 73 |
| 2000s | 58 |
| 2010s | 9 |
| 2020s | 2 |

## Low-Confidence Matches (score < 0.7)

These matches may be incorrect. Manual verification recommended.

| JSON Name | NRLD Raw Text | Score | Year | Method | State |
|-----------|---------------|-------|------|--------|-------|
| Maneri Stage I Dam | MANERI Stage I Uttarakhand Jal Vidyut Ni | 0.50 | 1984 | first_word_match | Uttarakhand |
| Jamrani Phase I Dam | JAMRANI Phase IUttarakhand Irrigation De | 0.54 | 1990 | first_word_match | Uttarakhand |
| Tehri Dam | TEHRI HPP Tehri Hydro Development Corpor | 0.56 | 2006 | first_word_match | Uttarakhand |
| Koteshwar Hep Dam | K H O EP TESHWAR Tehri Hydro Development | 0.56 | 2011 | name_similarity | Uttarakhand |
| Nagi Dam | NAGI B D i e h p a a r r t W m a e t n e | 0.60 | 1968 | first_word_match | Bihar |
| Kohira Dam | KOHIRA B D i e h p a a r r t W m a e t n | 0.60 | 1962 | first_word_match | Bihar |
| Amrity Dam | AMRITY B D i e h p a a r r t W m a e t n | 0.60 | 1965 | first_word_match | Bihar |
| Badua Dam | BADUA B D i e h p a a r r t W m a e t n  | 0.60 | 1965 | first_word_match | Bihar |
| Srikhandi Dam | SRIKHANDI B D i e h p a a r r t W m a e  | 0.60 | 1965 | first_word_match | Bihar |
| Chandan Dam | CHANDAN B D i e h p a a r r t W m a e t  | 0.60 | 1968 | first_word_match | Bihar |
| Jalkund Dam | JALKUND B D i e h p a a r r t W m a e t  | 0.60 | 1968 | first_word_match | Bihar |
| Morway Dam | MORWAY B D i e h p a a r r t W m a e t n | 0.60 | 1969 | first_word_match | Bihar |
| Job Dam | JOB B D i e h p a a r r t W m a e t n e  | 0.60 | 1978 | first_word_match | Bihar |
| Nakti Dam | NAKTI B D i e h p a a r r t W m a e t n  | 0.60 | 1980 | first_word_match | Bihar |
| Baskund Dam | BASKUND B D i e h p a a r r t W m a e t  | 0.60 | 1984 | first_word_match | Bihar |
| Belharna Dam | BELHARNA B D i e h p a a r r t W m a e t | 0.60 | 1987 | first_word_match | Bihar |
| Phulwaria Dam | PHULWARIA B D i e h p a a r r t W m a e  | 0.60 | 1988 | first_word_match | Bihar |
| Anjan Dam | ANJAN B D i e h p a a r r t W m a e t n  | 0.60 | 1989 | first_word_match | Bihar |
| Orhni Dam | ORHNI B D i e h p a a r r t W m a e t n  | 0.60 | 2000 | first_word_match | Bihar |
| Bilashi Dam | BILASHI B D i e h p a a r r t W m a e t  | 0.60 | 2001 | first_word_match | Bihar |
| Upper Kiul Dam | UPPER KIUL B D i e h p a a r r t W m a e | 0.60 | 2004 | first_word_match | Bihar |
| Durgawati Dam | DURGAWATI B D i e h p a a r r t W m a e  | 0.60 | 2014 | first_word_match | Bihar |
| Sindhwarni Dam | SINDHWARNI B D i e h p a a r r t W m a e | 0.60 | None | first_word_match | Bihar |
| Jagannathpur T. Dam | JAGANNATHPUR T. C R h es h o a u tt r i  | 0.60 | 1976 | first_word_match | Chhattisgarh |
| Dhab Dam | DHAB C R h es h o a u tt r i c s e g s a | 0.60 | 1978 | first_word_match | Chhattisgarh |
| Khunal(Khunsi) T Dam | K T HUNAL(KHUNSI) C R h es h o a u tt r  | 0.60 | 1980 | first_word_match | Chhattisgarh |
| Chhota Palgi T. Dam | CHHOTA PALGI T. C R h es h o a u tt r i  | 0.60 | 2000 | first_word_match | Chhattisgarh |
| Batare Dam | BATARE J R h e a s r o k u h r a c n e d | 0.60 | 1954 | first_word_match | Jharkhand |
| Baranadi Dam | BARANADI J R h e a s r o k u h r a c n e | 0.60 | 1967 | first_word_match | Jharkhand |
| Kairabani Dam | KAIRABANI J R h e a s r o k u h r a c n  | 0.60 | 1967 | first_word_match | Jharkhand |

## Unmatched Dams (22 total)

These dams have no construction year from either NRLD or Wikipedia.
They are likely small dams not registered in NRLD, or have significantly
different naming between the two sources.

- Chandra Nagar T. Dam (Chhattisgarh)
- S. Ashok Sagar Dam (Madhya Pradesh)
- Ashok Nagar (Tulsi Sarovar) Dam (Madhya Pradesh)
- Chikhali Dam (Madhya Pradesh)
- Dhangaon Dam (Madhya Pradesh)
- Kandra Dam (Madhya Pradesh)
- Ruthai (Gopi Krishna Sagar) Dam (Madhya Pradesh)
- Mohini Pick-Up Weir Dam (Madhya Pradesh)
- Tons Barrage(Mpseb) Dam (Madhya Pradesh)
- Rajiv Sagar (Maksudangarh) Dam (Madhya Pradesh)
- Swaroop Sagar Dam (Rajasthan)
- Sainthal Sagar Dam (Rajasthan)
- Patan (Deosagar) Dam (Rajasthan)
- Bundika Gothra Dam (Rajasthan)
- Chandrabhaga Dam (Rajasthan)
- Modia Mahadev Dam (Rajasthan)
- Mandol Dam (Rajasthan)
- Kothari Stage 1 Dam (Rajasthan)
- Sanwariya Sarowar Dam (Rajasthan)
- Bari Mansarowar Dam (Rajasthan)
- Navratan Sagar Dam (Rajasthan)
- Mundliya Kheri Dam (Rajasthan)

## Limitations

- **PDF extraction quality**: NRLD PDF uses image-embedded tables. Text extraction
  via pdfplumber produces garbled dam names due to column interleaving. Matching
  relies on normalized first-word comparison and PIC-code serial number alignment.

- **Year accuracy**: Some NRLD entries have blank year fields (under construction
  or data not reported). These are marked as YearOfCompletion=null.

- **Name mismatches**: The JSON uses curated names (e.g., "Kharagpur Lake Dam")
  while NRLD uses official names (e.g., "KHARAGPUR LAKE"). Suffixes like "Dam",
  "Tank", "Reservoir" differ between sources.

- **Ganga basin filtering**: NRLD records filtered by "River Basin = Ganga" in
  the PDF text. Some border cases (dams in inter-basin areas) may be misclassified.

- **Wikipedia coverage**: Only covers ~68 major dams. Hundreds of small irrigation
  dams in MP and Rajasthan have no Wikipedia presence.
