# Ganga Basin — River Tier Reclassification: Processing Summary

**Stage:** River tier classification
**Input:** `ganga_rivers_summary.csv` (362 unique rivers)
**Output:** `ganga_rivers_tiered.csv` (`river_name`, `segments`, `Updated_Tier`)
**Tier scheme:** 5-level hydrological-importance hierarchy (1 = basin-defining → 5 = minor local drainage)

---

## 1. Input condition

The supplied `tier` column was effectively unpopulated: **353 of 362 rivers** carried the placeholder value `99` (unclassified), and only 9 carried real tier values (three at Tier 1, four at Tier 2, two at Tier 3). Several of those nine were also questionable — three single-/double-segment streams (Gahra, Garkhe, Hiyuni) were pre-tagged Tier 2 despite negligible catchment, almost certainly seed errors.

**Consequence:** this stage was a classification from first principles, not a revision of an existing scheme.

---

## 2. Method

Classification followed a fixed decision order. Segment count was used **only as a tiebreaker**, never as the primary driver.

1. **Position in the drainage hierarchy** — main stem / main distributary / major tributary trunk / sub-basin river / local channel.
2. **Discharge and catchment scale** — including perennial vs. seasonal behavior and snow/glacier contribution.
3. **Sub-basin role** — significance within the Yamuna, Ghaghara, Gandak, Kosi, Son, Chambal, Betwa, Ken, and other recognized sub-basins.
4. **Segment count** — supporting evidence only, to break ties between otherwise comparable rivers.

A curated knowledge base placed every river with a recognized basin-scale role (and known low-segment exceptions such as Hooghly and Goriganga). Unrecognized single-segment named channels defaulted to Tier 5 by inference, with the basis stated in the working notes.

### Why segment count was not the driver

Segment counts reflect how the network was split during extraction, not hydrological mass. The final per-tier segment ranges confirm the two are decoupled:

| Tier | Rivers | Segment range |
|------|--------|---------------|
| 1 | 5 | 1 – 108 |
| 2 | 15 | 5 – 33 |
| 3 | 31 | 1 – 29 |
| 4 | 95 | 1 – 15 |
| 5 | 216 | 1 – 3 |

A 1-segment river sits in Tier 1 (Hooghly) while 29-segment seasonal rivers sit in Tier 3 (Banas, Khari) — the deliberate result of ranking by hydrology over count.

---

## 3. Output distribution

| Tier | Count | Definition |
|------|-------|------------|
| **1** | 5 | Main stem + main-stem-class channels / basin-defining giants |
| **2** | 15 | Major tributary trunks; great sub-basin rivers |
| **3** | 31 | Medium tributaries with clear catchment-level importance |
| **4** | 95 | Smaller / local tributaries; glacier-fed Himalayan headwater streams |
| **5** | 216 | Minor streams, delta distributaries, limited basin-scale influence |
| | **362** | |

The distribution forms the expected drainage pyramid: a narrow apex, a defined middle, and a long minor-stream tail.

---

## 4. Tier 1 and Tier 2 membership

**Tier 1 (5):** Ganga (main stem), Padma (downstream continuation), Hooghly (Indian deltaic main stem), Yamuna (master river of the south-central limb), Ghaghara (largest tributary by mean discharge).

**Tier 2 (15):** Gandak, Kosi, Son, Sharda, Mahakali, Ramganga, Gomti, Chambal, Betwa, Ken, Bagmati, Burhi Gandak, Rapti, Mahananda, Tons.

Gandak and Kosi sit immediately below the Tier-1 line and are noted as borderline in the working reasons.

---

## 5. Tier 3 membership (31)

Alaknanda, Bhagirathi, Mandakini, Pindar, Nandakini, Goriganga (Ganga source / Panch-Prayag glacier-fed streams); Banas, Parbati, Kali Sindh, Khari, Mendha (Chambal system); Dhasan, Sonar, Bearma (Betwa/Ken system); Rihand, Koel, Kanhar, Belan (Son/Tons system); Sai, Kali, Sindh, Hindon (plains tributaries); Punpun, Phalgu, Harohar, Karmanasa, Kiul (Bihar Ganga tributaries); Kamala, Rohini, Sarju, Tamasa.

---

## 6. Cases flagged for expert verification (59 rows)

Flags are concentrated on genuine ambiguities, not scattered uncertainty. The decision-affecting ones:

- **Tons vs. Tamasa** — two distinct rivers in the basin share these names: the Himalayan Tons (largest Yamuna tributary by volume) and the Vindhyan Tamsa that joins the Ganga directly. Tiers assigned on the most likely reading; confirm which physical river each row represents.
- **Kali** — read as the doab Kali Nadi (Tier 3), but the name collides with the Kali / Sharda / Mahakali Himalayan trunk.
- **Sarju** — could be the lower Ghaghara/Ayodhya Saryu or the Kumaon Saryu (Sharda tributary).
- **Dhauli Ganga** — two same-named glacier-fed rivers exist (Alaknanda system and Kali system).
- **Mendha / Khari** — Chambal-system tributaries whose exact confluence/identity should be confirmed.

The remaining flags are unrecognized small named channels where a hydrologist's local knowledge would refine the Tier-4/5 boundary.

---

## 7. Data-quality notes

- **Seed-tier corrections:** Gahra, Garkhe, Hiyuni downgraded from a pre-existing Tier 2 to Tier 4/5 (minor streams; seed values appear erroneous).
- **River names:** no spelling corrections were required; all names preserved verbatim.
- **Kosi:** confirmed as the single canonical Sapt Kosi (consistent with the earlier merge of Kosi variants); the Kumaon Kosi is not separately represented.
- **Default rule:** unrecognized single-segment rivers → Tier 5, on the inference that an unrecognized single-segment named channel in a 362-river inventory is minor local drainage.
