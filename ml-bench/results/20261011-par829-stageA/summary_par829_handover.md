
### Hand-over winners — `tables_2026-08-15_2026-10-07` (autumn-shoulder origins 2026-08-08..2026-10-07)

Source: `tables_2026-08-15_2026-10-07/handover.csv`, **274 rows**. A PAR-829 arm wins **34 of 274**. ORACLE (`_owx`) rows winning: **0** (the harness excludes `_owx` from this table by a suffix test, so 0 is the correct and expected value).

| winner | hand-over rows |
|---|---|
| `h5` | 52 |
| `clim` | 52 |
| `wt_med` | 39 |
| `lvlh5_tft` | 32 |
| `snaive7` | 26 |
| `nf_tide` **(PAR-829)** | 24 |
| `lvl_snaive7` | 13 |
| `persistence` | 10 |
| `lvl_tft` | 7 |
| `nf_nhits_x_h5` **(PAR-829)** | 6 |
| `nf_tide_x_h5` **(PAR-829)** | 4 |
| `lvl_naive4` | 4 |
| `lvl_clim` | 4 |
| `prod_served_lin` | 1 |

**The 34 cells a PAR-829 arm wins** — margin is against the hardest naive in the envelope, park-cluster CI; `pd` = park-day CI. `share` is the winner's paired share; `nod` the origin days behind the cell; `unit` the unit the LOW-N gate was applied on.

| uc | metric | seg | lead | winner | margin vs hardest | park-cluster CI | park-day CI | hardest | share | nod | unit | units agree |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| D1 | next-best precision | all | 0-120 | `nf_tide` | **+0.0382** | [+0.0035, +0.0743] | [+0.0286, +0.0485] | `snaive7` | 0.9603 | 54 | park-day | True |
| D1 | next-best precision | all | 0-60 | `nf_tide` | **+0.0394** | [+0.0057, +0.0745] | [+0.0299, +0.0495] | `snaive7` | 0.9520 | 54 | park-day | True |
| D1 | next-best precision (top-3) | all | 0-120 | `nf_tide` | **+0.0405** | [+0.0241, +0.0570] | [+0.0336, +0.0470] | `snaive7` | 0.9415 | 54 | park-day | True |
| D1 | next-best precision (top-3) | all | 0-60 | `nf_tide` | **+0.0475** | [+0.0303, +0.0642] | [+0.0405, +0.0542] | `snaive7` | 0.9305 | 54 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 0 | `nf_tide` | **+0.0790** | [+0.0681, +0.0916] | [+0.0751, +0.0829] | `wt_med` | 0.9918 | 54 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 1 | `nf_tide` | **+0.0734** | [+0.0628, +0.0859] | [+0.0694, +0.0776] | `wt_med` | 0.9915 | 54 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 2 | `nf_tide` | **+0.0678** | [+0.0576, +0.0804] | [+0.0639, +0.0720] | `wt_med` | 0.9907 | 54 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 3 | `nf_tide` | **+0.0661** | [+0.0559, +0.0783] | [+0.0619, +0.0702] | `wt_med` | 0.9898 | 54 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 4 | `nf_tide` | **+0.0676** | [+0.0572, +0.0796] | [+0.0635, +0.0717] | `wt_med` | 0.9901 | 54 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 5 | `nf_tide` | **+0.0648** | [+0.0546, +0.0771] | [+0.0609, +0.0690] | `wt_med` | 0.9904 | 54 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 6 | `nf_tide` | **+0.0674** | [+0.0570, +0.0795] | [+0.0632, +0.0714] | `wt_med` | 0.9913 | 54 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 7 | `nf_tide` | **+0.0645** | [+0.0548, +0.0762] | [+0.0605, +0.0690] | `wt_med` | 0.9920 | 54 | park-day | True |
| D4 | optimiser regret (min per park-day) | all | 1 | `nf_tide_x_h5` | **-2.5932** | [-4.0497, -1.2813] | [-3.3999, -1.7249] | `wt_med` | 0.9698 | 54 | park-day | True |
| D4 | optimiser regret (min per park-day) | all | 3 | `nf_tide_x_h5` | **-2.7045** | [-4.0593, -1.3651] | [-3.4762, -1.8371] | `wt_med` | 0.9641 | 54 | park-day | True |
| D4 | optimiser regret (min per park-day) | all | 7 | `nf_nhits_x_h5` | **-2.4523** | [-3.7093, -1.2255] | [-3.3028, -1.6176] | `wt_med` | 0.9712 | 54 | park-day | True |
| D5 | first-hour MAE (opening-aligned) | all | 3 | `nf_tide_x_h5` | **-0.4367** | [-0.7537, -0.1558] | [-0.6799, -0.4293] | `clim` | 0.9953 | 54 | park-day | True |
| D5 | first-hour MAE (opening-aligned) | all | 4 | `nf_tide_x_h5` | **-0.4209** | [-0.7335, -0.1441] | [-0.6554, -0.3997] | `clim` | 0.9951 | 54 | park-day | True |
| D5 | first-hour MAE (opening-aligned) | all | 5 | `nf_nhits_x_h5` | **-0.4073** | [-0.7236, -0.1300] | [-0.6244, -0.3682] | `clim` | 0.9950 | 54 | park-day | True |
| D5 | first-hour MAE (opening-aligned) | all | 6 | `nf_nhits_x_h5` | **-0.3761** | [-0.6833, -0.1011] | [-0.5743, -0.3166] | `clim` | 0.9945 | 54 | park-day | True |
| D5 | first-hour MAE (opening-aligned) | all | 7 | `nf_nhits_x_h5` | **-0.3353** | [-0.6415, -0.0607] | [-0.5563, -0.4091] | `clim` | 0.9945 | 54 | park-day | True |
| D5 | first-hour MAE, schedule known at origin | all | 6 | `nf_nhits_x_h5` | **-0.3900** | [-0.7447, -0.0615] | [-0.6955, -0.3990] | `clim` | 0.9938 | 54 | park-day | True |
| D5 | first-hour MAE, schedule known at origin | all | 7 | `nf_nhits_x_h5` | **-0.3428** | [-0.6970, -0.0186] | [-0.5934, -0.4272] | `clim` | 0.9943 | 54 | park-day | True |
| UC1 | MAE | all | m090 | `nf_tide` | **-0.4208** | [-0.7033, -0.1355] | [-0.4932, -0.3396] | `persistence` | 0.9688 | 54 | park-day | True |
| UC1 | MAE | all | m105 | `nf_tide` | **-0.8059** | [-1.1170, -0.5052] | [-0.8831, -0.7206] | `persistence` | 0.9684 | 54 | park-day | True |
| UC1 | MAE | all | m120 | `nf_tide` | **-1.0912** | [-1.4158, -0.7595] | [-1.1746, -1.0044] | `persistence` | 0.9680 | 54 | park-day | True |
| UC1 | MAE | busy | m060 | `nf_tide` | **-0.5519** | [-0.9552, -0.1079] | [-0.6653, -0.4327] | `persistence` | 0.9734 | 54 | park-day | True |
| UC1 | MAE | busy | m090 | `nf_tide` | **-1.0280** | [-1.4915, -0.5338] | [-1.1731, -0.8657] | `persistence` | 0.9730 | 54 | park-day | True |
| UC1 | MAE | busy | m105 | `nf_tide` | **-1.7310** | [-2.2251, -1.2418] | [-1.8842, -1.5596] | `persistence` | 0.9725 | 54 | park-day | True |
| UC1 | MAE | busy | m120 | `nf_tide` | **-2.3316** | [-2.8817, -1.7715] | [-2.4980, -2.1523] | `persistence` | 0.9723 | 54 | park-day | True |
| UC2-intraday | MAE | all | h2-4 | `nf_tide` | **-1.0136** | [-1.2553, -0.7748] | [-1.7947, -1.5913] | `wt_med` | 0.9645 | 54 | park-day | True |
| UC2-intraday | MAE | all | h4-8 | `nf_tide` | **-0.5049** | [-0.7165, -0.2856] | [-0.5878, -0.4244] | `wt_med` | 0.9795 | 54 | park-day | True |
| UC2-intraday | MAE | busy | h2-4 | `nf_tide` | **-2.6394** | [-3.0890, -2.1557] | [-3.4766, -3.0826] | `wt_med` | 0.9695 | 54 | park-day | True |
| UC2-intraday | MAE | busy | h4-8 | `nf_tide` | **-1.4408** | [-1.8887, -0.9480] | [-1.6368, -1.2527] | `wt_med` | 0.9625 | 54 | park-day | True |
| UC3 | dayPeak Spearman across rides | all | 0 | `nf_tide` | **+0.0132** | [+0.0029, +0.0243] | [+0.0069, +0.0200] | `snaive7` | 0.9763 | 54 | park-day | True |

**The planner slot surface (UC2 / UC3 MAE, d0–d7) — who wins it** (autumn-shoulder origins 2026-08-08..2026-10-07):

| uc | seg | lead | winner | margin vs hardest | hardest | n_tested | nod |
|---|---|---|---|---|---|---|---|
| UC2 | all | 0 | `lvlh5_tft` | -0.3078 | `wt_med` | 9 | 54 |
| UC2 | all | 1 | `lvlh5_tft` | -0.2847 | `wt_med` | 11 | 54 |
| UC3 | all | 1 | `lvlh5_tft` | -0.2847 | `wt_med` | 11 | 54 |
| UC3 | all | 2 | `h5` | -0.1370 | `wt_med` | 11 | 54 |
| UC3 | all | 3 | `h5` | -0.1389 | `wt_med` | 13 | 54 |
| UC3 | all | 4 | `h5` | -0.1401 | `wt_med` | 13 | 54 |
| UC3 | all | 5 | `h5` | -0.1395 | `wt_med` | 13 | 54 |
| UC3 | all | 6 | `snaive7` | — | `—` | 13 | 54 |
| UC3 | all | 7 | `wt_med` | — | `—` | 12 | 54 |
| UC2 | busy | 0 | `lvlh5_tft` | -1.3606 | `wt_med` | 9 | 54 |
| UC2 | busy | 1 | `lvlh5_tft` | -1.3358 | `wt_med` | 11 | 54 |
| UC3 | busy | 1 | `lvlh5_tft` | -1.3358 | `wt_med` | 11 | 54 |
| UC3 | busy | 2 | `lvlh5_tft` | -1.2229 | `wt_med` | 11 | 54 |
| UC3 | busy | 3 | `lvlh5_tft` | -1.0910 | `wt_med` | 13 | 54 |
| UC3 | busy | 4 | `lvlh5_tft` | -0.9898 | `wt_med` | 13 | 54 |
| UC3 | busy | 5 | `lvlh5_tft` | -0.9528 | `wt_med` | 13 | 54 |
| UC3 | busy | 6 | `lvlh5_tft` | -0.8586 | `wt_med` | 13 | 54 |
| UC3 | busy | 7 | `lvlh5_tft` | -0.6520 | `wt_med` | 12 | 54 |

**The daily-level surfaces (UC4 / D6 / D7) — who wins them** (autumn-shoulder origins 2026-08-08..2026-10-07). No PAR-829 level source wins any of these cells; the paired regressions are in the diagnostics table below.

| uc | metric | lead | winner | unit | n_tested | nod |
|---|---|---|---|---|---|---|
| D6 | crowd bucket exact | 1 | `lvl_snaive7` | park-day | 8 | 54 |
| D6 | crowd bucket exact | 2 | `lvl_snaive7` | park-day | 8 | 54 |
| D6 | crowd bucket exact | 3 | `lvl_snaive7` | park-day | 8 | 54 |
| D6 | crowd bucket exact | 4 | `lvl_snaive7` | park-day | 8 | 54 |
| D6 | crowd bucket exact | 5 | `lvl_snaive7` | park-day | 8 | 54 |
| D6 | crowd bucket exact | 6 | `lvl_snaive7` | park-day | 8 | 54 |
| D6 | crowd bucket exact | 7 | `lvl_tft` | park-day | 7 | 54 |
| D6 | crowd bucket exact | 10 | `lvl_tft` | park-day | 5 | 51 |
| D6 | crowd bucket exact | 14 | `lvl_tft` | park-day | 5 | 47 |
| D6 | crowd bucket exact | 21 | `lvl_tft` | park-day | 5 | 40 |
| D6 | crowd bucket exact | 30 | `lvl_tft` | park-day | 5 | 31 |
| D6 | crowd bucket exact | 45 | `lvl_naive4` | park-day | 0 | 16 |
| D6 | crowd bucket exact | 60 | `lvl_naive4` | park-day | 0 | 1 |
| D7 | day comparison winner accuracy | d1-7 | `lvl_snaive7` | park-origin | 8 | 58 |
| D7 | day comparison winner accuracy | d31-90 | `lvl_tft` | park-origin | 5 | 30 |
| D7 | day comparison winner accuracy | d8-30 | `lvl_tft` | park-origin | 5 | 53 |
| UC4 | UC4 Spearman within park-month | 1 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 2 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 3 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 4 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 5 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 6 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 7 | `lvl_naive4` | park-month | 7 | 3 |
| UC4 | UC4 Spearman within park-month | 10 | `lvl_naive4` | park-month | 5 | 3 |
| UC4 | UC4 Spearman within park-month | 14 | `lvl_clim` | park-month | 5 | 3 |
| UC4 | UC4 Spearman within park-month | 21 | `lvl_clim` | park-month | 5 | 3 |
| UC4 | UC4 Spearman within park-month | 30 | `lvl_clim` | park-month | 5 | 2 |
| UC4 | UC4 Spearman within park-month | 45 | `lvl_clim` | park-month | 4 | 2 |

**Usable horizon / `leads_tested`, PAR-829 arms** (`tables_2026-08-15_2026-10-07/usable_horizon.csv`). `leads_tested > 0` everywhere below, so none of these is the 'nothing was tested' tautology; an empty `usable_horizon` with a non-empty `max_significant_lead` means the wins form a **band**, not a prefix from the first lead.

| metric | uc | seg | model | unit | leads_scored | leads_tested | usable_horizon | max_significant_lead | not_tested_because |
|---|---|---|---|---|---|---|---|---|---|
| next-best precision | D1 | all | `nf_nhits` | park-day | 2 | **2** | — | — | — |
| next-best precision | D1 | all | `nf_tide` | park-day | 2 | **2** | 0-120 | 0-120 | — |
| next-best precision (top-3) | D1 | all | `nf_nhits` | park-day | 2 | **2** | — | — | — |
| next-best precision (top-3) | D1 | all | `nf_tide` | park-day | 2 | **2** | 0-120 | 0-120 | — |
| next-best recall | D1 | all | `nf_nhits` | park-day | 2 | **2** | — | — | — |
| next-best recall | D1 | all | `nf_tide` | park-day | 2 | **2** | — | — | — |
| live-window MAE (headliners) | D2 | headliners | `nf_nhits` | park-day | 1 | **1** | — | — | — |
| live-window MAE (headliners) | D2 | headliners | `nf_tide` | park-day | 1 | **1** | — | — | — |
| best-time hit rate (top-2) | D3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| best-time hit rate (top-2) | D3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| best-time hit rate (top-2) | D3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| best-time hit rate (top-2) | D3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| best-time hit rate (±30 min) | D3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| best-time hit rate (±30 min) | D3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| best-time hit rate (±30 min) | D3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| best-time hit rate (±30 min) | D3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| best-time regret (min) | D3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| best-time regret (min) | D3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| best-time regret (min) | D3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| best-time regret (min) | D3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| dayPeak pairwise ordering (headliners) | D4 | all | `nf_nhits` | park-day | 8 | **8** | 7 | 7 | — |
| dayPeak pairwise ordering (headliners) | D4 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| dayPeak pairwise ordering (headliners) | D4 | all | `nf_tide` | park-day | 8 | **8** | 7 | 7 | — |
| dayPeak pairwise ordering (headliners) | D4 | all | `nf_tide_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| optimiser regret (min per park-day) | D4 | all | `nf_nhits` | park-day | 3 | **3** | — | — | — |
| optimiser regret (min per park-day) | D4 | all | `nf_nhits_x_h5` | park-day | 3 | **3** | 7 | 7 | — |
| optimiser regret (min per park-day) | D4 | all | `nf_tide` | park-day | 3 | **3** | — | — | — |
| optimiser regret (min per park-day) | D4 | all | `nf_tide_x_h5` | park-day | 3 | **3** | 7 | 7 | — |
| first-hour MAE (opening-aligned) | D5 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| first-hour MAE (opening-aligned) | D5 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| first-hour MAE (opening-aligned) | D5 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| first-hour MAE (opening-aligned) | D5 | all | `nf_tide_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| first-hour MAE, schedule known at origin | D5 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| first-hour MAE, schedule known at origin | D5 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| first-hour MAE, schedule known at origin | D5 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| first-hour MAE, schedule known at origin | D5 | all | `nf_tide_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| first-hour MAE, schedule projected at origin | D5 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| first-hour MAE, schedule projected at origin | D5 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| first-hour MAE, schedule projected at origin | D5 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| first-hour MAE, schedule projected at origin | D5 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| rope-drop worth agreement | D5 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| rope-drop worth agreement | D5 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| rope-drop worth agreement | D5 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| rope-drop worth agreement | D5 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| crowd bucket exact | D6 | all | `lvl_nf_nhits` | park-day | 7 | **7** | — | — | — |
| crowd bucket exact | D6 | all | `lvl_nf_tide` | park-day | 7 | **7** | — | — | — |
| day comparison winner accuracy | D7 | all | `lvl_nf_nhits` | park-origin | 1 | **1** | — | — | — |
| day comparison winner accuracy | D7 | all | `lvl_nf_tide` | park-origin | 1 | **1** | — | — | — |
| MAE | UC1 | all | `nf_nhits` | park-day | 8 | **8** | — | m120 | — |
| MAE | UC1 | all | `nf_tide` | park-day | 8 | **8** | — | m120 | — |
| MAE | UC1 | busy | `nf_nhits` | park-day | 8 | **8** | — | m120 | — |
| MAE | UC1 | busy | `nf_tide` | park-day | 8 | **8** | — | m120 | — |
| MAE | UC2 | all | `nf_nhits` | park-day | 2 | **2** | — | — | — |
| MAE | UC2 | all | `nf_nhits_x_h5` | park-day | 1 | **1** | — | — | — |
| MAE | UC2 | all | `nf_tide` | park-day | 2 | **2** | 0 | 0 | — |
| MAE | UC2 | all | `nf_tide_x_h5` | park-day | 1 | **1** | — | — | — |
| MAE | UC2 | busy | `nf_nhits` | park-day | 2 | **2** | — | — | — |
| MAE | UC2 | busy | `nf_nhits_x_h5` | park-day | 1 | **1** | — | — | — |
| MAE | UC2 | busy | `nf_tide` | park-day | 2 | **2** | 0 | 0 | — |
| MAE | UC2 | busy | `nf_tide_x_h5` | park-day | 1 | **1** | 1 | 1 | — |
| MAE | UC2-intraday | all | `nf_nhits` | park-day | 3 | **3** | — | — | — |
| MAE | UC2-intraday | all | `nf_tide` | park-day | 3 | **3** | h4-8 | h4-8 | — |
| MAE | UC2-intraday | busy | `nf_nhits` | park-day | 3 | **3** | h2-4 | h2-4 | — |
| MAE | UC2-intraday | busy | `nf_tide` | park-day | 3 | **3** | h4-8 | h4-8 | — |
| Spearman of slots within ride-day | UC2/UC3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| Spearman of slots within ride-day | UC2/UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| Spearman of slots within ride-day | UC2/UC3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| Spearman of slots within ride-day | UC2/UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | all | `nf_nhits` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | all | `nf_tide` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | busy | `nf_nhits` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | busy | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | busy | `nf_tide` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | busy | `nf_tide_x_h5` | park-day | 7 | **7** | 1 | 1 | — |
| MAE, schedule known at origin | UC3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| MAE, schedule known at origin | UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE, schedule known at origin | UC3 | all | `nf_tide` | park-day | 8 | **8** | 0 | 0 | — |
| MAE, schedule known at origin | UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE, schedule projected at origin | UC3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| MAE, schedule projected at origin | UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE, schedule projected at origin | UC3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| MAE, schedule projected at origin | UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| dayPeak Spearman across rides | UC3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| dayPeak Spearman across rides | UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| dayPeak Spearman across rides | UC3 | all | `nf_tide` | park-day | 8 | **8** | 0 | 0 | — |
| dayPeak Spearman across rides | UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| dayPeak abs error (min) | UC3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| dayPeak abs error (min) | UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| dayPeak abs error (min) | UC3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| dayPeak abs error (min) | UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| UC4 Spearman within park-month | UC4 | all | `lvl_nf_nhits` | park-month | 7 | **7** | — | — | — |
| UC4 Spearman within park-month | UC4 | all | `lvl_nf_tide` | park-month | 7 | **7** | — | — | — |


### Hand-over winners — `tables` (autumn-shoulder origins 2026-08-08..2026-10-07)

Source: `tables/handover.csv`, **274 rows**. A PAR-829 arm wins **34 of 274**. ORACLE (`_owx`) rows winning: **0** (the harness excludes `_owx` from this table by a suffix test, so 0 is the correct and expected value).

| winner | hand-over rows |
|---|---|
| `h5` | 55 |
| `clim` | 53 |
| `wt_med` | 39 |
| `lvlh5_tft` | 29 |
| `snaive7` | 24 |
| `nf_tide` **(PAR-829)** | 24 |
| `lvl_snaive7` | 13 |
| `persistence` | 10 |
| `nf_nhits_x_h5` **(PAR-829)** | 6 |
| `lvl_tft` | 6 |
| `lvl_naive4` | 5 |
| `nf_tide_x_h5` **(PAR-829)** | 4 |
| `lvl_clim` | 4 |
| `prod_served_lin` | 2 |

**The 34 cells a PAR-829 arm wins** — margin is against the hardest naive in the envelope, park-cluster CI; `pd` = park-day CI. `share` is the winner's paired share; `nod` the origin days behind the cell; `unit` the unit the LOW-N gate was applied on.

| uc | metric | seg | lead | winner | margin vs hardest | park-cluster CI | park-day CI | hardest | share | nod | unit | units agree |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| D1 | next-best precision | all | 0-120 | `nf_tide` | **+0.0491** | [+0.0181, +0.0863] | [+0.0388, +0.0590] | `snaive7` | 0.9630 | 61 | park-day | True |
| D1 | next-best precision | all | 0-60 | `nf_tide` | **+0.0486** | [+0.0218, +0.0826] | [+0.0392, +0.0588] | `clim` | 0.9545 | 61 | park-day | True |
| D1 | next-best precision (top-3) | all | 0-120 | `nf_tide` | **+0.0438** | [+0.0284, +0.0600] | [+0.0375, +0.0503] | `snaive7` | 0.9457 | 61 | park-day | True |
| D1 | next-best precision (top-3) | all | 0-60 | `nf_tide` | **+0.0490** | [+0.0327, +0.0672] | [+0.0430, +0.0557] | `snaive7` | 0.9344 | 61 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 0 | `nf_tide` | **+0.0760** | [+0.0661, +0.0869] | [+0.0722, +0.0797] | `wt_med` | 0.9927 | 61 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 1 | `nf_tide` | **+0.0711** | [+0.0605, +0.0824] | [+0.0674, +0.0749] | `wt_med` | 0.9924 | 60 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 2 | `nf_tide` | **+0.0673** | [+0.0567, +0.0790] | [+0.0637, +0.0711] | `wt_med` | 0.9915 | 59 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 3 | `nf_tide` | **+0.0648** | [+0.0540, +0.0763] | [+0.0612, +0.0686] | `wt_med` | 0.9906 | 58 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 4 | `nf_tide` | **+0.0664** | [+0.0557, +0.0780] | [+0.0626, +0.0702] | `wt_med` | 0.9907 | 57 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 5 | `nf_tide` | **+0.0637** | [+0.0531, +0.0752] | [+0.0598, +0.0677] | `wt_med` | 0.9908 | 56 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 6 | `nf_tide` | **+0.0672** | [+0.0558, +0.0795] | [+0.0634, +0.0711] | `wt_med` | 0.9915 | 55 | park-day | True |
| D4 | dayPeak pairwise ordering (headliners) | all | 7 | `nf_tide` | **+0.0645** | [+0.0533, +0.0766] | [+0.0606, +0.0686] | `wt_med` | 0.9920 | 54 | park-day | True |
| D4 | optimiser regret (min per park-day) | all | 1 | `nf_tide_x_h5` | **-2.2477** | [-3.7250, -0.9484] | [-3.0016, -1.5480] | `wt_med` | 0.9734 | 60 | park-day | True |
| D4 | optimiser regret (min per park-day) | all | 3 | `nf_tide_x_h5` | **-2.7007** | [-4.1067, -1.4624] | [-3.4296, -1.9780] | `wt_med` | 0.9673 | 58 | park-day | True |
| D4 | optimiser regret (min per park-day) | all | 7 | `nf_nhits_x_h5` | **-2.4523** | [-3.7998, -1.2193] | [-3.3116, -1.6617] | `wt_med` | 0.9712 | 54 | park-day | True |
| D5 | first-hour MAE (opening-aligned) | all | 3 | `nf_tide_x_h5` | **-0.4385** | [-0.8073, -0.1769] | [-0.6848, -0.4536] | `wt_med` | 0.9954 | 58 | park-day | True |
| D5 | first-hour MAE (opening-aligned) | all | 4 | `nf_tide_x_h5` | **-0.4239** | [-0.7487, -0.1622] | [-0.6667, -0.4318] | `clim` | 0.9952 | 57 | park-day | True |
| D5 | first-hour MAE (opening-aligned) | all | 5 | `nf_nhits_x_h5` | **-0.4080** | [-0.7436, -0.1418] | [-0.6375, -0.4031] | `clim` | 0.9951 | 56 | park-day | True |
| D5 | first-hour MAE (opening-aligned) | all | 6 | `nf_nhits_x_h5` | **-0.3750** | [-0.7043, -0.1099] | [-0.5840, -0.3396] | `clim` | 0.9946 | 55 | park-day | True |
| D5 | first-hour MAE (opening-aligned) | all | 7 | `nf_nhits_x_h5` | **-0.3353** | [-0.6740, -0.0633] | [-0.5575, -0.4192] | `clim` | 0.9945 | 54 | park-day | True |
| D5 | first-hour MAE, schedule known at origin | all | 6 | `nf_nhits_x_h5` | **-0.3888** | [-0.7553, -0.0733] | [-0.7028, -0.4220] | `clim` | 0.9939 | 55 | park-day | True |
| D5 | first-hour MAE, schedule known at origin | all | 7 | `nf_nhits_x_h5` | **-0.3428** | [-0.7199, -0.0259] | [-0.5932, -0.4321] | `clim` | 0.9943 | 54 | park-day | True |
| UC1 | MAE | all | m090 | `nf_tide` | **-0.5154** | [-0.7766, -0.2496] | [-0.5911, -0.4490] | `persistence` | 0.9690 | 61 | park-day | True |
| UC1 | MAE | all | m105 | `nf_tide` | **-0.9023** | [-1.1903, -0.6112] | [-0.9830, -0.8323] | `persistence` | 0.9685 | 61 | park-day | True |
| UC1 | MAE | all | m120 | `nf_tide` | **-1.1936** | [-1.5116, -0.8784] | [-1.2776, -1.1223] | `persistence` | 0.9681 | 61 | park-day | True |
| UC1 | MAE | busy | m060 | `nf_tide` | **-0.6186** | [-1.0063, -0.1837] | [-0.7239, -0.5098] | `persistence` | 0.9746 | 61 | park-day | True |
| UC1 | MAE | busy | m090 | `nf_tide` | **-1.1889** | [-1.6009, -0.7095] | [-1.3448, -1.0480] | `persistence` | 0.9742 | 61 | park-day | True |
| UC1 | MAE | busy | m105 | `nf_tide` | **-1.8987** | [-2.3508, -1.3980] | [-2.0587, -1.7587] | `persistence` | 0.9735 | 61 | park-day | True |
| UC1 | MAE | busy | m120 | `nf_tide` | **-2.5158** | [-3.0205, -1.9550] | [-2.6781, -2.3715] | `persistence` | 0.9734 | 61 | park-day | True |
| UC2-intraday | MAE | all | h2-4 | `nf_tide` | **-0.9536** | [-1.2065, -0.7256] | [-1.9301, -1.7440] | `wt_med` | 0.9646 | 61 | park-day | True |
| UC2-intraday | MAE | all | h4-8 | `nf_tide` | **-0.4697** | [-0.6777, -0.2744] | [-0.5425, -0.3971] | `wt_med` | 0.9813 | 61 | park-day | True |
| UC2-intraday | MAE | busy | h2-4 | `nf_tide` | **-2.4966** | [-3.0091, -2.0235] | [-3.7145, -3.3483] | `wt_med` | 0.9709 | 61 | park-day | True |
| UC2-intraday | MAE | busy | h4-8 | `nf_tide` | **-1.3387** | [-1.8248, -0.9009] | [-1.5136, -1.1717] | `wt_med` | 0.9643 | 61 | park-day | True |
| UC3 | dayPeak Spearman across rides | all | 0 | `nf_tide` | **+0.0138** | [+0.0053, +0.0221] | [+0.0098, +0.0179] | `wt_med` | 0.9790 | 61 | park-day | True |

**The planner slot surface (UC2 / UC3 MAE, d0–d7) — who wins it** (autumn-shoulder origins 2026-08-08..2026-10-07):

| uc | seg | lead | winner | margin vs hardest | hardest | n_tested | nod |
|---|---|---|---|---|---|---|---|
| UC2 | all | 0 | `lvlh5_tft` | -0.2302 | `wt_med` | 9 | 61 |
| UC2 | all | 1 | `h5` | -0.1184 | `wt_med` | 11 | 60 |
| UC3 | all | 1 | `h5` | -0.1184 | `wt_med` | 11 | 60 |
| UC3 | all | 2 | `h5` | -0.1215 | `wt_med` | 11 | 59 |
| UC3 | all | 3 | `h5` | -0.1272 | `wt_med` | 13 | 58 |
| UC3 | all | 4 | `h5` | -0.1317 | `wt_med` | 13 | 57 |
| UC3 | all | 5 | `h5` | -0.1339 | `wt_med` | 13 | 56 |
| UC3 | all | 6 | `h5` | -0.1356 | `wt_med` | 13 | 55 |
| UC3 | all | 7 | `wt_med` | — | `—` | 12 | 54 |
| UC2 | busy | 0 | `lvlh5_tft` | -1.1829 | `wt_med` | 9 | 61 |
| UC2 | busy | 1 | `lvlh5_tft` | -1.1416 | `wt_med` | 11 | 60 |
| UC3 | busy | 1 | `lvlh5_tft` | -1.1416 | `wt_med` | 11 | 60 |
| UC3 | busy | 2 | `lvlh5_tft` | -1.0683 | `wt_med` | 11 | 59 |
| UC3 | busy | 3 | `lvlh5_tft` | -0.9343 | `wt_med` | 13 | 58 |
| UC3 | busy | 4 | `lvlh5_tft` | -0.8743 | `wt_med` | 13 | 57 |
| UC3 | busy | 5 | `lvlh5_tft` | -0.8548 | `wt_med` | 13 | 56 |
| UC3 | busy | 6 | `lvlh5_tft` | -0.8103 | `wt_med` | 13 | 55 |
| UC3 | busy | 7 | `lvlh5_tft` | -0.6520 | `wt_med` | 12 | 54 |

**The daily-level surfaces (UC4 / D6 / D7) — who wins them** (autumn-shoulder origins 2026-08-08..2026-10-07). No PAR-829 level source wins any of these cells; the paired regressions are in the diagnostics table below.

| uc | metric | lead | winner | unit | n_tested | nod |
|---|---|---|---|---|---|---|
| D6 | crowd bucket exact | 1 | `lvl_snaive7` | park-day | 8 | 61 |
| D6 | crowd bucket exact | 2 | `lvl_snaive7` | park-day | 8 | 59 |
| D6 | crowd bucket exact | 3 | `lvl_snaive7` | park-day | 8 | 58 |
| D6 | crowd bucket exact | 4 | `lvl_snaive7` | park-day | 8 | 57 |
| D6 | crowd bucket exact | 5 | `lvl_snaive7` | park-day | 8 | 56 |
| D6 | crowd bucket exact | 6 | `lvl_snaive7` | park-day | 8 | 55 |
| D6 | crowd bucket exact | 7 | `lvl_tft` | park-day | 7 | 54 |
| D6 | crowd bucket exact | 10 | `lvl_tft` | park-day | 5 | 51 |
| D6 | crowd bucket exact | 14 | `lvl_tft` | park-day | 5 | 47 |
| D6 | crowd bucket exact | 21 | `lvl_tft` | park-day | 5 | 40 |
| D6 | crowd bucket exact | 30 | `lvl_naive4` | park-day | 5 | 31 |
| D6 | crowd bucket exact | 45 | `lvl_naive4` | park-day | 0 | 16 |
| D6 | crowd bucket exact | 60 | `lvl_naive4` | park-day | 0 | 1 |
| D7 | day comparison winner accuracy | d1-7 | `lvl_snaive7` | park-origin | 8 | 59 |
| D7 | day comparison winner accuracy | d31-90 | `lvl_tft` | park-origin | 5 | 30 |
| D7 | day comparison winner accuracy | d8-30 | `lvl_tft` | park-origin | 5 | 53 |
| UC4 | UC4 Spearman within park-month | 1 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 2 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 3 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 4 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 5 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 6 | `lvl_snaive7` | park-month | 8 | 3 |
| UC4 | UC4 Spearman within park-month | 7 | `lvl_naive4` | park-month | 7 | 3 |
| UC4 | UC4 Spearman within park-month | 10 | `lvl_naive4` | park-month | 5 | 3 |
| UC4 | UC4 Spearman within park-month | 14 | `lvl_clim` | park-month | 5 | 3 |
| UC4 | UC4 Spearman within park-month | 21 | `lvl_clim` | park-month | 5 | 3 |
| UC4 | UC4 Spearman within park-month | 30 | `lvl_clim` | park-month | 5 | 2 |
| UC4 | UC4 Spearman within park-month | 45 | `lvl_clim` | park-month | 4 | 2 |

**Usable horizon / `leads_tested`, PAR-829 arms** (`tables/usable_horizon.csv`). `leads_tested > 0` everywhere below, so none of these is the 'nothing was tested' tautology; an empty `usable_horizon` with a non-empty `max_significant_lead` means the wins form a **band**, not a prefix from the first lead.

| metric | uc | seg | model | unit | leads_scored | leads_tested | usable_horizon | max_significant_lead | not_tested_because |
|---|---|---|---|---|---|---|---|---|---|
| next-best precision | D1 | all | `nf_nhits` | park-day | 2 | **2** | — | — | — |
| next-best precision | D1 | all | `nf_tide` | park-day | 2 | **2** | 0-120 | 0-120 | — |
| next-best precision (top-3) | D1 | all | `nf_nhits` | park-day | 2 | **2** | — | — | — |
| next-best precision (top-3) | D1 | all | `nf_tide` | park-day | 2 | **2** | 0-120 | 0-120 | — |
| next-best recall | D1 | all | `nf_nhits` | park-day | 2 | **2** | — | — | — |
| next-best recall | D1 | all | `nf_tide` | park-day | 2 | **2** | — | — | — |
| live-window MAE (headliners) | D2 | headliners | `nf_nhits` | park-day | 1 | **1** | — | — | — |
| live-window MAE (headliners) | D2 | headliners | `nf_tide` | park-day | 1 | **1** | — | — | — |
| best-time hit rate (top-2) | D3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| best-time hit rate (top-2) | D3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| best-time hit rate (top-2) | D3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| best-time hit rate (top-2) | D3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| best-time hit rate (±30 min) | D3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| best-time hit rate (±30 min) | D3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| best-time hit rate (±30 min) | D3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| best-time hit rate (±30 min) | D3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| best-time regret (min) | D3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| best-time regret (min) | D3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| best-time regret (min) | D3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| best-time regret (min) | D3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| dayPeak pairwise ordering (headliners) | D4 | all | `nf_nhits` | park-day | 8 | **8** | 7 | 7 | — |
| dayPeak pairwise ordering (headliners) | D4 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| dayPeak pairwise ordering (headliners) | D4 | all | `nf_tide` | park-day | 8 | **8** | 7 | 7 | — |
| dayPeak pairwise ordering (headliners) | D4 | all | `nf_tide_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| optimiser regret (min per park-day) | D4 | all | `nf_nhits` | park-day | 3 | **3** | — | — | — |
| optimiser regret (min per park-day) | D4 | all | `nf_nhits_x_h5` | park-day | 3 | **3** | 7 | 7 | — |
| optimiser regret (min per park-day) | D4 | all | `nf_tide` | park-day | 3 | **3** | — | — | — |
| optimiser regret (min per park-day) | D4 | all | `nf_tide_x_h5` | park-day | 3 | **3** | 7 | 7 | — |
| first-hour MAE (opening-aligned) | D5 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| first-hour MAE (opening-aligned) | D5 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| first-hour MAE (opening-aligned) | D5 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| first-hour MAE (opening-aligned) | D5 | all | `nf_tide_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| first-hour MAE, schedule known at origin | D5 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| first-hour MAE, schedule known at origin | D5 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| first-hour MAE, schedule known at origin | D5 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| first-hour MAE, schedule known at origin | D5 | all | `nf_tide_x_h5` | park-day | 7 | **7** | 7 | 7 | — |
| first-hour MAE, schedule projected at origin | D5 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| first-hour MAE, schedule projected at origin | D5 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| first-hour MAE, schedule projected at origin | D5 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| first-hour MAE, schedule projected at origin | D5 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| rope-drop worth agreement | D5 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| rope-drop worth agreement | D5 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| rope-drop worth agreement | D5 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| rope-drop worth agreement | D5 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| crowd bucket exact | D6 | all | `lvl_nf_nhits` | park-day | 7 | **7** | — | — | — |
| crowd bucket exact | D6 | all | `lvl_nf_tide` | park-day | 7 | **7** | — | — | — |
| day comparison winner accuracy | D7 | all | `lvl_nf_nhits` | park-origin | 1 | **1** | — | — | — |
| day comparison winner accuracy | D7 | all | `lvl_nf_tide` | park-origin | 1 | **1** | — | — | — |
| MAE | UC1 | all | `nf_nhits` | park-day | 8 | **8** | — | m120 | — |
| MAE | UC1 | all | `nf_tide` | park-day | 8 | **8** | — | m120 | — |
| MAE | UC1 | busy | `nf_nhits` | park-day | 8 | **8** | — | m120 | — |
| MAE | UC1 | busy | `nf_tide` | park-day | 8 | **8** | — | m120 | — |
| MAE | UC2 | all | `nf_nhits` | park-day | 2 | **2** | — | — | — |
| MAE | UC2 | all | `nf_nhits_x_h5` | park-day | 1 | **1** | — | — | — |
| MAE | UC2 | all | `nf_tide` | park-day | 2 | **2** | — | — | — |
| MAE | UC2 | all | `nf_tide_x_h5` | park-day | 1 | **1** | — | — | — |
| MAE | UC2 | busy | `nf_nhits` | park-day | 2 | **2** | — | — | — |
| MAE | UC2 | busy | `nf_nhits_x_h5` | park-day | 1 | **1** | — | — | — |
| MAE | UC2 | busy | `nf_tide` | park-day | 2 | **2** | 0 | 0 | — |
| MAE | UC2 | busy | `nf_tide_x_h5` | park-day | 1 | **1** | — | — | — |
| MAE | UC2-intraday | all | `nf_nhits` | park-day | 3 | **3** | — | — | — |
| MAE | UC2-intraday | all | `nf_tide` | park-day | 3 | **3** | h4-8 | h4-8 | — |
| MAE | UC2-intraday | busy | `nf_nhits` | park-day | 3 | **3** | — | — | — |
| MAE | UC2-intraday | busy | `nf_tide` | park-day | 3 | **3** | h4-8 | h4-8 | — |
| Spearman of slots within ride-day | UC2/UC3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| Spearman of slots within ride-day | UC2/UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| Spearman of slots within ride-day | UC2/UC3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| Spearman of slots within ride-day | UC2/UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | all | `nf_nhits` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | all | `nf_tide` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | busy | `nf_nhits` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | busy | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | busy | `nf_tide` | park-day | 7 | **7** | — | — | — |
| MAE | UC3 | busy | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE, schedule known at origin | UC3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| MAE, schedule known at origin | UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE, schedule known at origin | UC3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| MAE, schedule known at origin | UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE, schedule projected at origin | UC3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| MAE, schedule projected at origin | UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| MAE, schedule projected at origin | UC3 | all | `nf_tide` | park-day | 8 | **8** | — | — | — |
| MAE, schedule projected at origin | UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| dayPeak Spearman across rides | UC3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| dayPeak Spearman across rides | UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| dayPeak Spearman across rides | UC3 | all | `nf_tide` | park-day | 8 | **8** | 0 | 0 | — |
| dayPeak Spearman across rides | UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| dayPeak abs error (min) | UC3 | all | `nf_nhits` | park-day | 8 | **8** | — | — | — |
| dayPeak abs error (min) | UC3 | all | `nf_nhits_x_h5` | park-day | 7 | **7** | — | — | — |
| dayPeak abs error (min) | UC3 | all | `nf_tide` | park-day | 8 | **8** | 0 | 0 | — |
| dayPeak abs error (min) | UC3 | all | `nf_tide_x_h5` | park-day | 7 | **7** | — | — | — |
| UC4 Spearman within park-month | UC4 | all | `lvl_nf_nhits` | park-month | 7 | **7** | — | — | — |
| UC4 Spearman within park-month | UC4 | all | `lvl_nf_tide` | park-month | 7 | **7** | — | — | — |
