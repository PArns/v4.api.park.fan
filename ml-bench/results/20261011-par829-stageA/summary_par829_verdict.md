PAR-829 pre-registered decision rule, applied to 1668 margin cells from 4 file(s).
AUTUMN-SHOULDER ORIGINS 2026-08-08..2026-10-07 -- not comparable with any full-period figure.
serve clause testable on this run: False (no non-autumn origin -> SHADOW is the ceiling)

source                                seg   model                    lead  verdict / why
----------------------------------------------------------------------------------------------------------------------------
paired_margin_alltargets_intraday.csv all   nf_nhits                 h2-4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.305 REGRESSION pk[+0.113,+0.514]
                                                                         h5: +0.176 worse, not significant
                                                                         persistence: -0.939 beats pk[-1.153,-0.738] (cov ref 0.965 / model 0.631)
paired_margin_alltargets_intraday.csv all   nf_nhits                 h4-8  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.781 REGRESSION pk[+0.604,+0.987]
                                                                         h5: +0.665 REGRESSION pk[+0.389,+0.941]
                                                                         persistence: -2.428 beats pk[-2.880,-2.010] (cov ref 0.953 / model 0.436)
paired_margin_alltargets_intraday.csv all   nf_nhits                  h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.528 REGRESSION pk[+1.229,+1.867]
                                                                         h5: +1.732 REGRESSION pk[+1.374,+2.121]
                                                                         persistence: -4.996 GATED [NOT TESTED: model-side coverage 0.197<0.3] (cov ref 0.932 / model 0.197)
paired_margin_alltargets_intraday.csv all   nf_nhits                 m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.690 beats pk[-3.049,-2.349] (cov ref 0.985 / model 0.971)
                                                                         h5: -2.846 beats pk[-3.241,-2.452] (cov ref 0.984 / model 0.992)
                                                                         persistence: +2.338 REGRESSION pk[+2.077,+2.613]
paired_margin_alltargets_intraday.csv all   nf_nhits                 m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.222 beats pk[-2.542,-1.915] (cov ref 0.985 / model 0.971)
                                                                         h5: -2.335 beats pk[-2.691,-1.984] (cov ref 0.984 / model 0.992)
                                                                         persistence: +1.177 REGRESSION pk[+1.020,+1.345]
paired_margin_alltargets_intraday.csv all   nf_nhits                 m045  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -1.854 beats pk[-2.131,-1.588] (cov ref 0.985 / model 0.971)
                                                                         h5: -1.962 beats pk[-2.282,-1.648] (cov ref 0.984 / model 0.992)
                                                                         persistence: +0.545 REGRESSION pk[+0.400,+0.696]
paired_margin_alltargets_intraday.csv all   nf_nhits                 m060  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -1.457 beats pk[-1.718,-1.201] (cov ref 0.985 / model 0.971)
                                                                         h5: -1.591 beats pk[-1.889,-1.304] (cov ref 0.984 / model 0.992)
                                                                         persistence: +0.166 REGRESSION pk[+0.009,+0.335]
paired_margin_alltargets_intraday.csv all   nf_nhits                 m075  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -0.777 beats pk[-1.012,-0.533] (cov ref 0.985 / model 0.972)
                                                                         h5: -0.922 beats pk[-1.221,-0.624] (cov ref 0.985 / model 0.992)
                                                                         persistence: +0.547 REGRESSION pk[+0.403,+0.696]
paired_margin_alltargets_intraday.csv all   nf_nhits                 m090  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -0.761 beats pk[-0.987,-0.535] (cov ref 0.985 / model 0.972)
                                                                         h5: -0.865 beats pk[-1.141,-0.581] (cov ref 0.984 / model 0.992)
                                                                         persistence: -0.028 better, not significant
paired_margin_alltargets_intraday.csv all   nf_nhits                 m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.632 beats pk[-0.868,-0.416] (cov ref 0.984 / model 0.971)
                                                                         h5: -0.740 beats pk[-1.022,-0.441] (cov ref 0.984 / model 0.992)
                                                                         persistence: -0.412 beats pk[-0.569,-0.258] (cov ref 0.969 / model 0.779)
paired_margin_alltargets_intraday.csv all   nf_nhits                 m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.351 beats pk[-0.568,-0.145] (cov ref 0.984 / model 0.970)
                                                                         h5: -0.507 beats pk[-0.779,-0.239] (cov ref 0.984 / model 0.992)
                                                                         persistence: -0.644 beats pk[-0.843,-0.453] (cov ref 0.968 / model 0.771)
paired_margin_alltargets_intraday.csv all   nf_tide                  h2-4  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.705 beats pk[-0.892,-0.522] (cov ref 0.984 / model 0.970)
                                                                         h5: -0.840 beats pk[-1.060,-0.623] (cov ref 0.983 / model 0.992)
                                                                         persistence: -1.831 beats pk[-2.222,-1.436] (cov ref 0.965 / model 0.631)
paired_margin_alltargets_intraday.csv all   nf_tide                  h4-8  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.224 beats pk[-0.384,-0.076] (cov ref 0.981 / model 0.969)
                                                                         h5: -0.343 beats pk[-0.522,-0.163] (cov ref 0.980 / model 0.991)
                                                                         persistence: -3.396 beats pk[-4.050,-2.803] (cov ref 0.953 / model 0.436)
paired_margin_alltargets_intraday.csv all   nf_tide                   h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.199 worse, not significant
                                                                         h5: +0.410 REGRESSION pk[+0.185,+0.649]
                                                                         persistence: -6.488 GATED [NOT TESTED: model-side coverage 0.197<0.3] (cov ref 0.932 / model 0.197)
paired_margin_alltargets_intraday.csv all   nf_tide                  m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.960 beats pk[-3.320,-2.605] (cov ref 0.985 / model 0.971)
                                                                         h5: -3.118 beats pk[-3.511,-2.715] (cov ref 0.984 / model 0.992)
                                                                         persistence: +2.202 REGRESSION pk[+1.979,+2.429]
paired_margin_alltargets_intraday.csv all   nf_tide                  m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.578 beats pk[-2.897,-2.261] (cov ref 0.985 / model 0.971)
                                                                         h5: -2.694 beats pk[-3.058,-2.319] (cov ref 0.984 / model 0.992)
                                                                         persistence: +0.982 REGRESSION pk[+0.794,+1.172]
paired_margin_alltargets_intraday.csv all   nf_tide                  m045  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.302 beats pk[-2.584,-2.012] (cov ref 0.985 / model 0.971)
                                                                         h5: -2.413 beats pk[-2.739,-2.081] (cov ref 0.984 / model 0.992)
                                                                         persistence: +0.306 REGRESSION pk[+0.112,+0.508]
paired_margin_alltargets_intraday.csv all   nf_tide                  m060  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -2.034 beats pk[-2.302,-1.763] (cov ref 0.985 / model 0.971)
                                                                         h5: -2.171 beats pk[-2.482,-1.844] (cov ref 0.984 / model 0.992)
                                                                         persistence: -0.135 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_intraday.csv all   nf_tide                  m075  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -1.414 beats pk[-1.635,-1.202] (cov ref 0.985 / model 0.972)
                                                                         h5: -1.564 beats pk[-1.826,-1.312] (cov ref 0.985 / model 0.992)
                                                                         persistence: +0.040 worse, not significant
paired_margin_alltargets_intraday.csv all   nf_tide                  m090  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.425 beats pk[-1.649,-1.208] (cov ref 0.985 / model 0.972)
                                                                         h5: -1.535 beats pk[-1.803,-1.279] (cov ref 0.984 / model 0.992)
                                                                         persistence: -0.515 beats pk[-0.784,-0.240] (cov ref 0.969 / model 0.793)
paired_margin_alltargets_intraday.csv all   nf_tide                  m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.331 beats pk[-1.536,-1.113] (cov ref 0.984 / model 0.971)
                                                                         h5: -1.448 beats pk[-1.706,-1.186] (cov ref 0.984 / model 0.992)
                                                                         persistence: -0.902 beats pk[-1.207,-0.596] (cov ref 0.969 / model 0.779)
paired_margin_alltargets_intraday.csv all   nf_tide                  m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.135 beats pk[-1.337,-0.939] (cov ref 0.984 / model 0.970)
                                                                         h5: -1.295 beats pk[-1.541,-1.047] (cov ref 0.984 / model 0.992)
                                                                         persistence: -1.194 beats pk[-1.533,-0.866] (cov ref 0.968 / model 0.771)
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx           h2-4  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.781 beats pk[-0.959,-0.604] (cov ref 0.984 / model 0.970)
                                                                         h5: -0.916 beats pk[-1.136,-0.700] (cov ref 0.983 / model 0.992)
                                                                         persistence: -1.840 beats pk[-2.242,-1.454] (cov ref 0.965 / model 0.631)
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx           h4-8  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.364 beats pk[-0.516,-0.219] (cov ref 0.981 / model 0.969)
                                                                         h5: -0.483 beats pk[-0.666,-0.292] (cov ref 0.980 / model 0.991)
                                                                         persistence: -3.547 beats pk[-4.228,-2.940] (cov ref 0.953 / model 0.436)
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx            h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.085 worse, not significant
                                                                         h5: +0.292 REGRESSION pk[+0.074,+0.542]
                                                                         persistence: -6.676 GATED [NOT TESTED: model-side coverage 0.197<0.3] (cov ref 0.932 / model 0.197)
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx           m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -3.003 beats pk[-3.365,-2.649] (cov ref 0.985 / model 0.971)
                                                                         h5: -3.161 beats pk[-3.556,-2.753] (cov ref 0.984 / model 0.992)
                                                                         persistence: +2.149 REGRESSION pk[+1.929,+2.375]
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx           m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.617 beats pk[-2.942,-2.292] (cov ref 0.985 / model 0.971)
                                                                         h5: -2.732 beats pk[-3.097,-2.355] (cov ref 0.984 / model 0.992)
                                                                         persistence: +0.932 REGRESSION pk[+0.742,+1.124]
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx           m045  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.338 beats pk[-2.621,-2.044] (cov ref 0.985 / model 0.971)
                                                                         h5: -2.449 beats pk[-2.781,-2.114] (cov ref 0.984 / model 0.992)
                                                                         persistence: +0.254 REGRESSION pk[+0.063,+0.455]
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx           m060  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -2.066 beats pk[-2.341,-1.793] (cov ref 0.985 / model 0.971)
                                                                         h5: -2.202 beats pk[-2.519,-1.878] (cov ref 0.984 / model 0.992)
                                                                         persistence: -0.188 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx           m075  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -1.475 beats pk[-1.704,-1.257] (cov ref 0.985 / model 0.972)
                                                                         h5: -1.624 beats pk[-1.891,-1.363] (cov ref 0.985 / model 0.992)
                                                                         persistence: +0.019 worse, not significant
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx           m090  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.497 beats pk[-1.728,-1.272] (cov ref 0.985 / model 0.972)
                                                                         h5: -1.607 beats pk[-1.874,-1.349] (cov ref 0.984 / model 0.992)
                                                                         persistence: -0.546 beats pk[-0.814,-0.271] (cov ref 0.969 / model 0.793)
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx           m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.425 beats pk[-1.648,-1.201] (cov ref 0.984 / model 0.971)
                                                                         h5: -1.541 beats pk[-1.809,-1.264] (cov ref 0.984 / model 0.992)
                                                                         persistence: -0.949 beats pk[-1.256,-0.641] (cov ref 0.969 / model 0.779)
paired_margin_alltargets_intraday.csv all   nf_tide_wx_owx           m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.245 beats pk[-1.453,-1.033] (cov ref 0.984 / model 0.970)
                                                                         h5: -1.405 beats pk[-1.662,-1.148] (cov ref 0.984 / model 0.992)
                                                                         persistence: -1.245 beats pk[-1.584,-0.915] (cov ref 0.968 / model 0.771)
paired_margin_alltargets_intraday.csv busy  nf_nhits                 h2-4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.660 REGRESSION pk[+0.227,+1.079]
                                                                         h5: -0.299 park-day only (park-cluster CI spans 0)
                                                                         persistence: -1.940 beats pk[-2.255,-1.630] (cov ref 0.971 / model 0.669)
paired_margin_alltargets_intraday.csv busy  nf_nhits                 h4-8  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.607 REGRESSION pk[+1.244,+1.973]
                                                                         h5: +0.685 REGRESSION pk[+0.105,+1.296]
                                                                         persistence: -4.564 beats pk[-5.184,-4.021] (cov ref 0.964 / model 0.498)
paired_margin_alltargets_intraday.csv busy  nf_nhits                  h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +3.295 REGRESSION pk[+2.739,+3.861]
                                                                         h5: +3.017 REGRESSION pk[+2.339,+3.672]
                                                                         persistence: -8.875 GATED [NOT TESTED: model-side coverage 0.247<0.3] (cov ref 0.954 / model 0.247)
paired_margin_alltargets_intraday.csv busy  nf_nhits                 m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -4.986 beats pk[-5.654,-4.381] (cov ref 0.984 / model 0.974)
                                                                         h5: -5.948 beats pk[-6.664,-5.210] (cov ref 0.983 / model 0.994)
                                                                         persistence: +4.112 REGRESSION pk[+3.669,+4.599]
paired_margin_alltargets_intraday.csv busy  nf_nhits                 m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -4.191 beats pk[-4.804,-3.601] (cov ref 0.984 / model 0.974)
                                                                         h5: -5.089 beats pk[-5.759,-4.419] (cov ref 0.983 / model 0.994)
                                                                         persistence: +1.932 REGRESSION pk[+1.603,+2.270]
paired_margin_alltargets_intraday.csv busy  nf_nhits                 m045  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -3.497 beats pk[-4.058,-2.950] (cov ref 0.984 / model 0.974)
                                                                         h5: -4.367 beats pk[-4.994,-3.727] (cov ref 0.983 / model 0.994)
                                                                         persistence: +0.757 REGRESSION pk[+0.471,+1.056]
paired_margin_alltargets_intraday.csv busy  nf_nhits                 m060  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -2.743 beats pk[-3.310,-2.175] (cov ref 0.983 / model 0.973)
                                                                         h5: -3.634 beats pk[-4.247,-2.991] (cov ref 0.983 / model 0.993)
                                                                         persistence: -0.031 better, not significant
paired_margin_alltargets_intraday.csv busy  nf_nhits                 m075  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -1.347 beats pk[-1.939,-0.784] (cov ref 0.984 / model 0.975)
                                                                         h5: -2.324 beats pk[-3.039,-1.627] (cov ref 0.984 / model 0.994)
                                                                         persistence: +0.858 REGRESSION pk[+0.540,+1.157]
paired_margin_alltargets_intraday.csv busy  nf_nhits                 m090  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.355 beats pk[-1.859,-0.881] (cov ref 0.984 / model 0.974)
                                                                         h5: -2.275 beats pk[-2.927,-1.677] (cov ref 0.983 / model 0.994)
                                                                         persistence: -0.243 beats pk[-0.479,-0.007] (cov ref 0.974 / model 0.810)
paired_margin_alltargets_intraday.csv busy  nf_nhits                 m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.129 beats pk[-1.635,-0.659] (cov ref 0.984 / model 0.973)
                                                                         h5: -2.072 beats pk[-2.693,-1.435] (cov ref 0.983 / model 0.993)
                                                                         persistence: -0.996 beats pk[-1.236,-0.735] (cov ref 0.974 / model 0.795)
paired_margin_alltargets_intraday.csv busy  nf_nhits                 m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.630 beats pk[-1.116,-0.194] (cov ref 0.984 / model 0.972)
                                                                         h5: -1.630 beats pk[-2.245,-0.967] (cov ref 0.983 / model 0.992)
                                                                         persistence: -1.533 beats pk[-1.846,-1.204] (cov ref 0.973 / model 0.786)
paired_margin_alltargets_intraday.csv busy  nf_tide                  h2-4  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.261 beats pk[-1.642,-0.877] (cov ref 0.983 / model 0.973)
                                                                         h5: -2.230 beats pk[-2.660,-1.785] (cov ref 0.982 / model 0.993)
                                                                         persistence: -3.520 beats pk[-4.114,-2.913] (cov ref 0.971 / model 0.669)
paired_margin_alltargets_intraday.csv busy  nf_tide                  h4-8  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -0.127 better, not significant
                                                                         h5: -1.059 beats pk[-1.453,-0.650] (cov ref 0.979 / model 0.993)
                                                                         persistence: -5.919 beats pk[-6.872,-5.027] (cov ref 0.964 / model 0.498)
paired_margin_alltargets_intraday.csv busy  nf_tide                   h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.780 REGRESSION pk[+0.280,+1.288]
                                                                         h5: +0.498 worse, not significant
                                                                         persistence: -11.061 GATED [NOT TESTED: model-side coverage 0.247<0.3] (cov ref 0.954 / model 0.247)
paired_margin_alltargets_intraday.csv busy  nf_tide                  m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -5.533 beats pk[-6.113,-4.961] (cov ref 0.984 / model 0.974)
                                                                         h5: -6.499 beats pk[-7.156,-5.815] (cov ref 0.983 / model 0.994)
                                                                         persistence: +3.769 REGRESSION pk[+3.413,+4.147]
paired_margin_alltargets_intraday.csv busy  nf_tide                  m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -4.885 beats pk[-5.452,-4.338] (cov ref 0.984 / model 0.974)
                                                                         h5: -5.791 beats pk[-6.407,-5.151] (cov ref 0.983 / model 0.994)
                                                                         persistence: +1.499 REGRESSION pk[+1.130,+1.879]
paired_margin_alltargets_intraday.csv busy  nf_tide                  m045  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -4.374 beats pk[-4.885,-3.872] (cov ref 0.984 / model 0.974)
                                                                         h5: -5.251 beats pk[-5.858,-4.631] (cov ref 0.983 / model 0.994)
                                                                         persistence: +0.253 worse, not significant
paired_margin_alltargets_intraday.csv busy  nf_tide                  m060  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -3.897 beats pk[-4.404,-3.395] (cov ref 0.983 / model 0.973)
                                                                         h5: -4.792 beats pk[-5.378,-4.202] (cov ref 0.983 / model 0.993)
                                                                         persistence: -0.619 beats pk[-1.014,-0.188] (cov ref 0.975 / model 0.870)
paired_margin_alltargets_intraday.csv busy  nf_tide                  m075  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -2.552 beats pk[-3.018,-2.119] (cov ref 0.984 / model 0.975)
                                                                         h5: -3.542 beats pk[-4.077,-3.014] (cov ref 0.984 / model 0.994)
                                                                         persistence: -0.156 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_intraday.csv busy  nf_tide                  m090  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.592 beats pk[-3.039,-2.169] (cov ref 0.984 / model 0.974)
                                                                         h5: -3.524 beats pk[-4.048,-3.024] (cov ref 0.983 / model 0.994)
                                                                         persistence: -1.189 beats pk[-1.638,-0.729] (cov ref 0.974 / model 0.810)
paired_margin_alltargets_intraday.csv busy  nf_tide                  m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.402 beats pk[-2.857,-1.977] (cov ref 0.984 / model 0.973)
                                                                         h5: -3.360 beats pk[-3.865,-2.853] (cov ref 0.983 / model 0.993)
                                                                         persistence: -1.899 beats pk[-2.408,-1.417] (cov ref 0.974 / model 0.795)
paired_margin_alltargets_intraday.csv busy  nf_tide                  m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.039 beats pk[-2.448,-1.657] (cov ref 0.984 / model 0.972)
                                                                         h5: -3.048 beats pk[-3.546,-2.545] (cov ref 0.983 / model 0.992)
                                                                         persistence: -2.516 beats pk[-3.082,-1.991] (cov ref 0.973 / model 0.786)
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx           h2-4  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.395 beats pk[-1.768,-1.034] (cov ref 0.983 / model 0.973)
                                                                         h5: -2.364 beats pk[-2.786,-1.905] (cov ref 0.982 / model 0.993)
                                                                         persistence: -3.544 beats pk[-4.128,-2.936] (cov ref 0.971 / model 0.669)
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx           h4-8  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.408 beats pk[-0.735,-0.106] (cov ref 0.980 / model 0.973)
                                                                         h5: -1.338 beats pk[-1.735,-0.912] (cov ref 0.979 / model 0.993)
                                                                         persistence: -6.208 beats pk[-7.178,-5.302] (cov ref 0.964 / model 0.498)
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx            h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.560 REGRESSION pk[+0.075,+1.074]
                                                                         h5: +0.274 worse, not significant
                                                                         persistence: -11.399 GATED [NOT TESTED: model-side coverage 0.247<0.3] (cov ref 0.954 / model 0.247)
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx           m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -5.609 beats pk[-6.194,-5.035] (cov ref 0.984 / model 0.974)
                                                                         h5: -6.574 beats pk[-7.228,-5.873] (cov ref 0.983 / model 0.994)
                                                                         persistence: +3.677 REGRESSION pk[+3.322,+4.069]
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx           m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -4.952 beats pk[-5.525,-4.398] (cov ref 0.984 / model 0.974)
                                                                         h5: -5.856 beats pk[-6.474,-5.212] (cov ref 0.983 / model 0.994)
                                                                         persistence: +1.410 REGRESSION pk[+1.042,+1.790]
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx           m045  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -4.436 beats pk[-4.948,-3.913] (cov ref 0.984 / model 0.974)
                                                                         h5: -5.313 beats pk[-5.924,-4.683] (cov ref 0.983 / model 0.994)
                                                                         persistence: +0.157 worse, not significant
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx           m060  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -3.947 beats pk[-4.454,-3.433] (cov ref 0.983 / model 0.973)
                                                                         h5: -4.842 beats pk[-5.442,-4.245] (cov ref 0.983 / model 0.993)
                                                                         persistence: -0.714 beats pk[-1.104,-0.295] (cov ref 0.975 / model 0.870)
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx           m075  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -2.639 beats pk[-3.122,-2.192] (cov ref 0.984 / model 0.975)
                                                                         h5: -3.626 beats pk[-4.168,-3.094] (cov ref 0.984 / model 0.994)
                                                                         persistence: -0.176 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx           m090  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.717 beats pk[-3.184,-2.287] (cov ref 0.984 / model 0.974)
                                                                         h5: -3.647 beats pk[-4.187,-3.133] (cov ref 0.983 / model 0.994)
                                                                         persistence: -1.239 beats pk[-1.688,-0.783] (cov ref 0.974 / model 0.810)
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx           m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.584 beats pk[-3.043,-2.141] (cov ref 0.984 / model 0.973)
                                                                         h5: -3.540 beats pk[-4.073,-3.007] (cov ref 0.983 / model 0.993)
                                                                         persistence: -1.989 beats pk[-2.489,-1.504] (cov ref 0.974 / model 0.795)
paired_margin_alltargets_intraday.csv busy  nf_tide_wx_owx           m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.268 beats pk[-2.701,-1.866] (cov ref 0.984 / model 0.972)
                                                                         h5: -3.275 beats pk[-3.781,-2.760] (cov ref 0.983 / model 0.992)
                                                                         persistence: -2.621 beats pk[-3.185,-2.089] (cov ref 0.973 / model 0.786)
paired_margin_alltargets_slot.csv     all   nf_nhits                    0  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.502 REGRESSION pk[+1.229,+1.800]
                                                                         h5: +1.360 REGRESSION pk[+1.054,+1.710]
paired_margin_alltargets_slot.csv     all   nf_nhits                    1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.429 REGRESSION pk[+1.187,+1.702]
                                                                         h5: +1.321 REGRESSION pk[+1.030,+1.626]
paired_margin_alltargets_slot.csv     all   nf_nhits                    2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.353 REGRESSION pk[+1.127,+1.605]
                                                                         h5: +1.271 REGRESSION pk[+0.991,+1.575]
paired_margin_alltargets_slot.csv     all   nf_nhits                    3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.275 REGRESSION pk[+1.056,+1.525]
                                                                         h5: +1.258 REGRESSION pk[+0.985,+1.582]
paired_margin_alltargets_slot.csv     all   nf_nhits                    4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.267 REGRESSION pk[+1.056,+1.505]
                                                                         h5: +1.299 REGRESSION pk[+1.022,+1.606]
paired_margin_alltargets_slot.csv     all   nf_nhits                    5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.249 REGRESSION pk[+1.041,+1.490]
                                                                         h5: +1.280 REGRESSION pk[+1.017,+1.576]
paired_margin_alltargets_slot.csv     all   nf_nhits                    6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.188 REGRESSION pk[+0.990,+1.419]
                                                                         h5: +1.262 REGRESSION pk[+0.997,+1.555]
paired_margin_alltargets_slot.csv     all   nf_nhits                    7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.924 REGRESSION pk[+0.722,+1.160]
                                                                         h5: +1.107 REGRESSION pk[+0.857,+1.389]
paired_margin_alltargets_slot.csv     all   nf_nhits_x_h5               1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.297 REGRESSION pk[+1.059,+1.546]
                                                                         h5: +1.187 REGRESSION pk[+0.888,+1.514]
paired_margin_alltargets_slot.csv     all   nf_nhits_x_h5               2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.319 REGRESSION pk[+1.062,+1.599]
                                                                         h5: +1.238 REGRESSION pk[+0.929,+1.586]
paired_margin_alltargets_slot.csv     all   nf_nhits_x_h5               3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.314 REGRESSION pk[+1.064,+1.565]
                                                                         h5: +1.297 REGRESSION pk[+0.980,+1.656]
paired_margin_alltargets_slot.csv     all   nf_nhits_x_h5               4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.146 REGRESSION pk[+0.916,+1.381]
                                                                         h5: +1.170 REGRESSION pk[+0.873,+1.505]
paired_margin_alltargets_slot.csv     all   nf_nhits_x_h5               5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.170 REGRESSION pk[+0.936,+1.416]
                                                                         h5: +1.190 REGRESSION pk[+0.893,+1.525]
paired_margin_alltargets_slot.csv     all   nf_nhits_x_h5               6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.084 REGRESSION pk[+0.869,+1.314]
                                                                         h5: +1.145 REGRESSION pk[+0.850,+1.470]
paired_margin_alltargets_slot.csv     all   nf_nhits_x_h5               7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.859 REGRESSION pk[+0.622,+1.103]
                                                                         h5: +1.031 REGRESSION pk[+0.733,+1.358]
paired_margin_alltargets_slot.csv     all   nf_tide                     0  no win
                                                                         lvlh5_tft: +0.104 worse, not significant
                                                                         h5: -0.028 better, not significant
paired_margin_alltargets_slot.csv     all   nf_tide                     1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.362 REGRESSION pk[+0.231,+0.508]
                                                                         h5: +0.260 REGRESSION pk[+0.106,+0.418]
paired_margin_alltargets_slot.csv     all   nf_tide                     2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.531 REGRESSION pk[+0.350,+0.732]
                                                                         h5: +0.452 REGRESSION pk[+0.298,+0.617]
paired_margin_alltargets_slot.csv     all   nf_tide                     3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.552 REGRESSION pk[+0.391,+0.724]
                                                                         h5: +0.534 REGRESSION pk[+0.392,+0.688]
paired_margin_alltargets_slot.csv     all   nf_tide                     4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.463 REGRESSION pk[+0.295,+0.652]
                                                                         h5: +0.498 REGRESSION pk[+0.358,+0.647]
paired_margin_alltargets_slot.csv     all   nf_tide                     5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.529 REGRESSION pk[+0.330,+0.733]
                                                                         h5: +0.560 REGRESSION pk[+0.412,+0.717]
paired_margin_alltargets_slot.csv     all   nf_tide                     6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.342 REGRESSION pk[+0.160,+0.527]
                                                                         h5: +0.411 REGRESSION pk[+0.262,+0.569]
paired_margin_alltargets_slot.csv     all   nf_tide                     7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.475 REGRESSION pk[+0.267,+0.695]
                                                                         h5: +0.653 REGRESSION pk[+0.505,+0.801]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx              0  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -0.044 better, not significant
                                                                         h5: -0.179 beats pk[-0.344,-0.002] (cov ref 0.983 / model 0.992)
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx              1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.232 REGRESSION pk[+0.110,+0.365]
                                                                         h5: +0.129 worse, not significant
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx              2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.409 REGRESSION pk[+0.241,+0.586]
                                                                         h5: +0.328 REGRESSION pk[+0.182,+0.486]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx              3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.316 REGRESSION pk[+0.164,+0.475]
                                                                         h5: +0.297 REGRESSION pk[+0.146,+0.456]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx              4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.390 REGRESSION pk[+0.209,+0.587]
                                                                         h5: +0.426 REGRESSION pk[+0.291,+0.568]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx              5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.349 REGRESSION pk[+0.179,+0.524]
                                                                         h5: +0.379 REGRESSION pk[+0.240,+0.525]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx              6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.257 REGRESSION pk[+0.066,+0.449]
                                                                         h5: +0.324 REGRESSION pk[+0.177,+0.483]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx              7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.277 REGRESSION pk[+0.073,+0.478]
                                                                         h5: +0.455 REGRESSION pk[+0.319,+0.592]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx_x_h5         1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.269 REGRESSION pk[+0.149,+0.389]
                                                                         h5: +0.165 worse, not significant
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx_x_h5         2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.310 REGRESSION pk[+0.160,+0.459]
                                                                         h5: +0.233 REGRESSION pk[+0.064,+0.415]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx_x_h5         3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.270 REGRESSION pk[+0.121,+0.416]
                                                                         h5: +0.255 REGRESSION pk[+0.073,+0.446]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx_x_h5         4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.242 REGRESSION pk[+0.073,+0.402]
                                                                         h5: +0.272 REGRESSION pk[+0.106,+0.450]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx_x_h5         5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.174 REGRESSION pk[+0.023,+0.311]
                                                                         h5: +0.200 REGRESSION pk[+0.023,+0.377]
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx_x_h5         6  no win
                                                                         lvlh5_tft: +0.111 worse, not significant
                                                                         h5: +0.173 worse, not significant
paired_margin_alltargets_slot.csv     all   nf_tide_wx_owx_x_h5         7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.048 worse, not significant
                                                                         h5: +0.223 REGRESSION pk[+0.048,+0.394]
paired_margin_alltargets_slot.csv     all   nf_tide_x_h5                1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.496 REGRESSION pk[+0.366,+0.624]
                                                                         h5: +0.393 REGRESSION pk[+0.206,+0.579]
paired_margin_alltargets_slot.csv     all   nf_tide_x_h5                2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.563 REGRESSION pk[+0.393,+0.726]
                                                                         h5: +0.488 REGRESSION pk[+0.310,+0.681]
paired_margin_alltargets_slot.csv     all   nf_tide_x_h5                3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.500 REGRESSION pk[+0.343,+0.649]
                                                                         h5: +0.486 REGRESSION pk[+0.304,+0.672]
paired_margin_alltargets_slot.csv     all   nf_tide_x_h5                4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.411 REGRESSION pk[+0.244,+0.562]
                                                                         h5: +0.441 REGRESSION pk[+0.256,+0.636]
paired_margin_alltargets_slot.csv     all   nf_tide_x_h5                5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.482 REGRESSION pk[+0.309,+0.649]
                                                                         h5: +0.507 REGRESSION pk[+0.345,+0.689]
paired_margin_alltargets_slot.csv     all   nf_tide_x_h5                6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.336 REGRESSION pk[+0.164,+0.498]
                                                                         h5: +0.401 REGRESSION pk[+0.212,+0.591]
paired_margin_alltargets_slot.csv     all   nf_tide_x_h5                7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.272 REGRESSION pk[+0.100,+0.441]
                                                                         h5: +0.448 REGRESSION pk[+0.274,+0.619]
paired_margin_alltargets_slot.csv     busy  nf_nhits                    0  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +3.371 REGRESSION pk[+2.851,+3.939]
                                                                         h5: +2.413 REGRESSION pk[+1.707,+3.154]
paired_margin_alltargets_slot.csv     busy  nf_nhits                    1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +3.184 REGRESSION pk[+2.735,+3.685]
                                                                         h5: +2.286 REGRESSION pk[+1.641,+2.969]
paired_margin_alltargets_slot.csv     busy  nf_nhits                    2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +3.029 REGRESSION pk[+2.594,+3.529]
                                                                         h5: +2.192 REGRESSION pk[+1.570,+2.876]
paired_margin_alltargets_slot.csv     busy  nf_nhits                    3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.844 REGRESSION pk[+2.405,+3.349]
                                                                         h5: +2.165 REGRESSION pk[+1.514,+2.895]
paired_margin_alltargets_slot.csv     busy  nf_nhits                    4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.839 REGRESSION pk[+2.422,+3.322]
                                                                         h5: +2.235 REGRESSION pk[+1.651,+2.908]
paired_margin_alltargets_slot.csv     busy  nf_nhits                    5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.741 REGRESSION pk[+2.319,+3.215]
                                                                         h5: +2.175 REGRESSION pk[+1.560,+2.836]
paired_margin_alltargets_slot.csv     busy  nf_nhits                    6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.643 REGRESSION pk[+2.256,+3.085]
                                                                         h5: +2.130 REGRESSION pk[+1.555,+2.771]
paired_margin_alltargets_slot.csv     busy  nf_nhits                    7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.175 REGRESSION pk[+1.756,+2.611]
                                                                         h5: +1.856 REGRESSION pk[+1.275,+2.459]
paired_margin_alltargets_slot.csv     busy  nf_nhits_x_h5               1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.636 REGRESSION pk[+2.133,+3.181]
                                                                         h5: +1.738 REGRESSION pk[+1.017,+2.527]
paired_margin_alltargets_slot.csv     busy  nf_nhits_x_h5               2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.673 REGRESSION pk[+2.101,+3.290]
                                                                         h5: +1.842 REGRESSION pk[+1.091,+2.681]
paired_margin_alltargets_slot.csv     busy  nf_nhits_x_h5               3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.613 REGRESSION pk[+2.075,+3.214]
                                                                         h5: +1.936 REGRESSION pk[+1.182,+2.757]
paired_margin_alltargets_slot.csv     busy  nf_nhits_x_h5               4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.311 REGRESSION pk[+1.812,+2.853]
                                                                         h5: +1.688 REGRESSION pk[+0.952,+2.500]
paired_margin_alltargets_slot.csv     busy  nf_nhits_x_h5               5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.335 REGRESSION pk[+1.815,+2.879]
                                                                         h5: +1.748 REGRESSION pk[+1.011,+2.548]
paired_margin_alltargets_slot.csv     busy  nf_nhits_x_h5               6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.215 REGRESSION pk[+1.758,+2.712]
                                                                         h5: +1.679 REGRESSION pk[+0.931,+2.473]
paired_margin_alltargets_slot.csv     busy  nf_nhits_x_h5               7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.760 REGRESSION pk[+1.232,+2.284]
                                                                         h5: +1.421 REGRESSION pk[+0.668,+2.198]
paired_margin_alltargets_slot.csv     busy  nf_tide                     0  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.434 REGRESSION pk[+0.139,+0.727]
                                                                         h5: -0.516 beats pk[-0.890,-0.130] (cov ref 0.981 / model 0.993)
paired_margin_alltargets_slot.csv     busy  nf_tide                     1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.042 REGRESSION pk[+0.775,+1.331]
                                                                         h5: +0.146 worse, not significant
paired_margin_alltargets_slot.csv     busy  nf_tide                     2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.451 REGRESSION pk[+1.063,+1.841]
                                                                         h5: +0.616 REGRESSION pk[+0.252,+0.991]
paired_margin_alltargets_slot.csv     busy  nf_tide                     3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.452 REGRESSION pk[+1.082,+1.799]
                                                                         h5: +0.767 REGRESSION pk[+0.432,+1.082]
paired_margin_alltargets_slot.csv     busy  nf_tide                     4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.301 REGRESSION pk[+0.932,+1.687]
                                                                         h5: +0.700 REGRESSION pk[+0.360,+1.017]
paired_margin_alltargets_slot.csv     busy  nf_tide                     5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.404 REGRESSION pk[+0.947,+1.863]
                                                                         h5: +0.835 REGRESSION pk[+0.480,+1.180]
paired_margin_alltargets_slot.csv     busy  nf_tide                     6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.080 REGRESSION pk[+0.671,+1.511]
                                                                         h5: +0.555 REGRESSION pk[+0.205,+0.904]
paired_margin_alltargets_slot.csv     busy  nf_tide                     7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.382 REGRESSION pk[+0.918,+1.859]
                                                                         h5: +1.051 REGRESSION pk[+0.714,+1.337]
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx              0  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: +0.167 worse, not significant
                                                                         h5: -0.785 beats pk[-1.166,-0.382] (cov ref 0.981 / model 0.993)
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx              1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.798 REGRESSION pk[+0.535,+1.083]
                                                                         h5: -0.101 better, not significant
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx              2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.189 REGRESSION pk[+0.841,+1.547]
                                                                         h5: +0.350 REGRESSION pk[+0.010,+0.701]
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx              3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.981 REGRESSION pk[+0.648,+1.329]
                                                                         h5: +0.293 worse, not significant
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx              4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.159 REGRESSION pk[+0.754,+1.574]
                                                                         h5: +0.556 REGRESSION pk[+0.229,+0.875]
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx              5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.032 REGRESSION pk[+0.630,+1.438]
                                                                         h5: +0.459 REGRESSION pk[+0.119,+0.788]
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx              6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.921 REGRESSION pk[+0.486,+1.356]
                                                                         h5: +0.386 REGRESSION pk[+0.036,+0.750]
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx              7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.998 REGRESSION pk[+0.557,+1.445]
                                                                         h5: +0.661 REGRESSION pk[+0.339,+0.939]
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx_x_h5         1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.454 REGRESSION pk[+0.199,+0.739]
                                                                         h5: -0.443 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx_x_h5         2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.548 REGRESSION pk[+0.237,+0.880]
                                                                         h5: -0.281 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx_x_h5         3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.445 REGRESSION pk[+0.138,+0.780]
                                                                         h5: -0.238 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx_x_h5         4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.408 REGRESSION pk[+0.072,+0.795]
                                                                         h5: -0.209 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx_x_h5         5  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: +0.200 worse, not significant
                                                                         h5: -0.382 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx_x_h5         6  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: +0.152 worse, not significant
                                                                         h5: -0.390 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_slot.csv     busy  nf_tide_wx_owx_x_h5         7  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -0.002 better, not significant
                                                                         h5: -0.342 park-day only (park-cluster CI spans 0)
paired_margin_alltargets_slot.csv     busy  nf_tide_x_h5                1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.816 REGRESSION pk[+0.535,+1.110]
                                                                         h5: -0.079 better, not significant
paired_margin_alltargets_slot.csv     busy  nf_tide_x_h5                2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.995 REGRESSION pk[+0.644,+1.361]
                                                                         h5: +0.169 worse, not significant
paired_margin_alltargets_slot.csv     busy  nf_tide_x_h5                3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.785 REGRESSION pk[+0.467,+1.132]
                                                                         h5: +0.106 worse, not significant
paired_margin_alltargets_slot.csv     busy  nf_tide_x_h5                4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.694 REGRESSION pk[+0.363,+1.061]
                                                                         h5: +0.079 worse, not significant
paired_margin_alltargets_slot.csv     busy  nf_tide_x_h5                5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.750 REGRESSION pk[+0.387,+1.130]
                                                                         h5: +0.166 worse, not significant
paired_margin_alltargets_slot.csv     busy  nf_tide_x_h5                6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.538 REGRESSION pk[+0.168,+0.897]
                                                                         h5: +0.004 worse, not significant
paired_margin_alltargets_slot.csv     busy  nf_tide_x_h5                7  no win
                                                                         lvlh5_tft: +0.348 worse, not significant
                                                                         h5: +0.011 worse, not significant
paired_margin_window_intraday.csv     all   nf_nhits                 h2-4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.284 REGRESSION pk[+0.101,+0.468]
                                                                         h5: +0.097 worse, not significant
                                                                         persistence: -0.859 beats pk[-1.077,-0.659] (cov ref 0.964 / model 0.627)
paired_margin_window_intraday.csv     all   nf_nhits                 h4-8  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.804 REGRESSION pk[+0.620,+0.988]
                                                                         h5: +0.623 REGRESSION pk[+0.340,+0.922]
                                                                         persistence: -2.322 beats pk[-2.789,-1.905] (cov ref 0.953 / model 0.431)
paired_margin_window_intraday.csv     all   nf_nhits                  h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.553 REGRESSION pk[+1.239,+1.878]
                                                                         h5: +1.711 REGRESSION pk[+1.372,+2.089]
                                                                         persistence: -4.917 GATED [NOT TESTED: model-side coverage 0.197<0.3] (cov ref 0.933 / model 0.197)
paired_margin_window_intraday.csv     all   nf_nhits                 m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.689 beats pk[-3.088,-2.367] (cov ref 0.983 / model 0.971)
                                                                         h5: -2.895 beats pk[-3.335,-2.483] (cov ref 0.983 / model 0.993)
                                                                         persistence: +2.326 REGRESSION pk[+2.084,+2.606]
paired_margin_window_intraday.csv     all   nf_nhits                 m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.239 beats pk[-2.596,-1.945] (cov ref 0.984 / model 0.971)
                                                                         h5: -2.401 beats pk[-2.809,-2.041] (cov ref 0.983 / model 0.993)
                                                                         persistence: +1.182 REGRESSION pk[+1.020,+1.366]
paired_margin_window_intraday.csv     all   nf_nhits                 m045  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -1.889 beats pk[-2.195,-1.633] (cov ref 0.984 / model 0.971)
                                                                         h5: -2.047 beats pk[-2.420,-1.730] (cov ref 0.983 / model 0.993)
                                                                         persistence: +0.564 REGRESSION pk[+0.412,+0.721]
paired_margin_window_intraday.csv     all   nf_nhits                 m060  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -1.502 beats pk[-1.791,-1.252] (cov ref 0.984 / model 0.971)
                                                                         h5: -1.684 beats pk[-2.008,-1.397] (cov ref 0.983 / model 0.992)
                                                                         persistence: +0.188 REGRESSION pk[+0.015,+0.357]
paired_margin_window_intraday.csv     all   nf_nhits                 m075  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -0.818 beats pk[-1.077,-0.585] (cov ref 0.984 / model 0.972)
                                                                         h5: -1.013 beats pk[-1.369,-0.704] (cov ref 0.983 / model 0.993)
                                                                         persistence: +0.590 REGRESSION pk[+0.438,+0.746]
paired_margin_window_intraday.csv     all   nf_nhits                 m090  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -0.799 beats pk[-1.040,-0.586] (cov ref 0.984 / model 0.971)
                                                                         h5: -0.960 beats pk[-1.282,-0.643] (cov ref 0.983 / model 0.993)
                                                                         persistence: +0.014 worse, not significant
paired_margin_window_intraday.csv     all   nf_nhits                 m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.665 beats pk[-0.892,-0.462] (cov ref 0.983 / model 0.971)
                                                                         h5: -0.840 beats pk[-1.166,-0.509] (cov ref 0.982 / model 0.993)
                                                                         persistence: -0.372 beats pk[-0.528,-0.212] (cov ref 0.968 / model 0.777)
paired_margin_window_intraday.csv     all   nf_nhits                 m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.394 beats pk[-0.609,-0.192] (cov ref 0.983 / model 0.970)
                                                                         h5: -0.611 beats pk[-0.923,-0.287] (cov ref 0.982 / model 0.992)
                                                                         persistence: -0.600 beats pk[-0.802,-0.400] (cov ref 0.968 / model 0.769)
paired_margin_window_intraday.csv     all   nf_tide                  h2-4  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.689 beats pk[-0.869,-0.510] (cov ref 0.983 / model 0.970)
                                                                         h5: -0.881 beats pk[-1.124,-0.667] (cov ref 0.982 / model 0.993)
                                                                         persistence: -1.696 beats pk[-2.085,-1.315] (cov ref 0.964 / model 0.627)
paired_margin_window_intraday.csv     all   nf_tide                  h4-8  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.178 beats pk[-0.336,-0.032] (cov ref 0.979 / model 0.969)
                                                                         h5: -0.360 beats pk[-0.561,-0.161] (cov ref 0.979 / model 0.992)
                                                                         persistence: -3.225 beats pk[-3.861,-2.651] (cov ref 0.953 / model 0.431)
paired_margin_window_intraday.csv     all   nf_tide                   h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.245 REGRESSION pk[+0.010,+0.476]
                                                                         h5: +0.414 REGRESSION pk[+0.166,+0.677]
                                                                         persistence: -6.283 GATED [NOT TESTED: model-side coverage 0.197<0.3] (cov ref 0.933 / model 0.197)
paired_margin_window_intraday.csv     all   nf_tide                  m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.946 beats pk[-3.309,-2.611] (cov ref 0.983 / model 0.971)
                                                                         h5: -3.154 beats pk[-3.575,-2.745] (cov ref 0.983 / model 0.993)
                                                                         persistence: +2.196 REGRESSION pk[+1.974,+2.418]
paired_margin_window_intraday.csv     all   nf_tide                  m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.577 beats pk[-2.912,-2.280] (cov ref 0.984 / model 0.971)
                                                                         h5: -2.742 beats pk[-3.140,-2.355] (cov ref 0.983 / model 0.993)
                                                                         persistence: +1.001 REGRESSION pk[+0.807,+1.205]
paired_margin_window_intraday.csv     all   nf_tide                  m045  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.314 beats pk[-2.611,-2.044] (cov ref 0.984 / model 0.971)
                                                                         h5: -2.476 beats pk[-2.833,-2.125] (cov ref 0.983 / model 0.993)
                                                                         persistence: +0.342 REGRESSION pk[+0.143,+0.542]
paired_margin_window_intraday.csv     all   nf_tide                  m060  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -2.051 beats pk[-2.338,-1.801] (cov ref 0.984 / model 0.971)
                                                                         h5: -2.234 beats pk[-2.584,-1.908] (cov ref 0.983 / model 0.992)
                                                                         persistence: -0.090 park-day only (park-cluster CI spans 0)
paired_margin_window_intraday.csv     all   nf_tide                  m075  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -1.398 beats pk[-1.609,-1.190] (cov ref 0.984 / model 0.972)
                                                                         h5: -1.600 beats pk[-1.880,-1.316] (cov ref 0.983 / model 0.993)
                                                                         persistence: +0.137 worse, not significant
paired_margin_window_intraday.csv     all   nf_tide                  m090  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.404 beats pk[-1.618,-1.192] (cov ref 0.984 / model 0.971)
                                                                         h5: -1.573 beats pk[-1.861,-1.290] (cov ref 0.983 / model 0.993)
                                                                         persistence: -0.421 beats pk[-0.722,-0.147] (cov ref 0.969 / model 0.791)
paired_margin_window_intraday.csv     all   nf_tide                  m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.304 beats pk[-1.526,-1.098] (cov ref 0.983 / model 0.971)
                                                                         h5: -1.489 beats pk[-1.777,-1.208] (cov ref 0.982 / model 0.993)
                                                                         persistence: -0.806 beats pk[-1.124,-0.504] (cov ref 0.968 / model 0.777)
paired_margin_window_intraday.csv     all   nf_tide                  m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.104 beats pk[-1.309,-0.918] (cov ref 0.983 / model 0.970)
                                                                         h5: -1.327 beats pk[-1.602,-1.056] (cov ref 0.982 / model 0.992)
                                                                         persistence: -1.091 beats pk[-1.422,-0.763] (cov ref 0.968 / model 0.769)
paired_margin_window_intraday.csv     all   nf_tide_wx_owx           h2-4  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.761 beats pk[-0.951,-0.590] (cov ref 0.983 / model 0.970)
                                                                         h5: -0.952 beats pk[-1.200,-0.728] (cov ref 0.982 / model 0.993)
                                                                         persistence: -1.711 beats pk[-2.103,-1.323] (cov ref 0.964 / model 0.627)
paired_margin_window_intraday.csv     all   nf_tide_wx_owx           h4-8  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.313 beats pk[-0.457,-0.173] (cov ref 0.979 / model 0.969)
                                                                         h5: -0.494 beats pk[-0.709,-0.294] (cov ref 0.979 / model 0.992)
                                                                         persistence: -3.372 beats pk[-4.016,-2.774] (cov ref 0.953 / model 0.431)
paired_margin_window_intraday.csv     all   nf_tide_wx_owx            h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.141 worse, not significant
                                                                         h5: +0.307 REGRESSION pk[+0.061,+0.576]
                                                                         persistence: -6.430 GATED [NOT TESTED: model-side coverage 0.197<0.3] (cov ref 0.933 / model 0.197)
paired_margin_window_intraday.csv     all   nf_tide_wx_owx           m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -3.000 beats pk[-3.365,-2.662] (cov ref 0.983 / model 0.971)
                                                                         h5: -3.208 beats pk[-3.637,-2.790] (cov ref 0.983 / model 0.993)
                                                                         persistence: +2.131 REGRESSION pk[+1.910,+2.357]
paired_margin_window_intraday.csv     all   nf_tide_wx_owx           m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.627 beats pk[-2.967,-2.325] (cov ref 0.984 / model 0.971)
                                                                         h5: -2.791 beats pk[-3.192,-2.392] (cov ref 0.983 / model 0.993)
                                                                         persistence: +0.939 REGRESSION pk[+0.746,+1.142]
paired_margin_window_intraday.csv     all   nf_tide_wx_owx           m045  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -2.361 beats pk[-2.663,-2.088] (cov ref 0.984 / model 0.971)
                                                                         h5: -2.523 beats pk[-2.886,-2.163] (cov ref 0.983 / model 0.993)
                                                                         persistence: +0.278 REGRESSION pk[+0.079,+0.482]
paired_margin_window_intraday.csv     all   nf_tide_wx_owx           m060  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -2.095 beats pk[-2.386,-1.837] (cov ref 0.984 / model 0.971)
                                                                         h5: -2.278 beats pk[-2.635,-1.947] (cov ref 0.983 / model 0.992)
                                                                         persistence: -0.153 park-day only (park-cluster CI spans 0)
paired_margin_window_intraday.csv     all   nf_tide_wx_owx           m075  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -1.474 beats pk[-1.697,-1.253] (cov ref 0.984 / model 0.972)
                                                                         h5: -1.675 beats pk[-1.965,-1.391] (cov ref 0.983 / model 0.993)
                                                                         persistence: +0.102 worse, not significant
paired_margin_window_intraday.csv     all   nf_tide_wx_owx           m090  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.493 beats pk[-1.722,-1.273] (cov ref 0.984 / model 0.971)
                                                                         h5: -1.660 beats pk[-1.953,-1.369] (cov ref 0.983 / model 0.993)
                                                                         persistence: -0.465 beats pk[-0.755,-0.194] (cov ref 0.969 / model 0.791)
paired_margin_window_intraday.csv     all   nf_tide_wx_owx           m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.413 beats pk[-1.643,-1.200] (cov ref 0.983 / model 0.971)
                                                                         h5: -1.597 beats pk[-1.899,-1.297] (cov ref 0.982 / model 0.993)
                                                                         persistence: -0.864 beats pk[-1.176,-0.557] (cov ref 0.968 / model 0.777)
paired_margin_window_intraday.csv     all   nf_tide_wx_owx           m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.234 beats pk[-1.449,-1.032] (cov ref 0.983 / model 0.970)
                                                                         h5: -1.456 beats pk[-1.747,-1.168] (cov ref 0.982 / model 0.992)
                                                                         persistence: -1.156 beats pk[-1.485,-0.819] (cov ref 0.968 / model 0.769)
paired_margin_window_intraday.csv     busy  nf_nhits                 h2-4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.617 REGRESSION pk[+0.208,+0.998]
                                                                         h5: -0.485 park-day only (park-cluster CI spans 0)
                                                                         persistence: -1.808 beats pk[-2.123,-1.460] (cov ref 0.970 / model 0.666)
paired_margin_window_intraday.csv     busy  nf_nhits                 h4-8  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.629 REGRESSION pk[+1.293,+1.977]
                                                                         h5: +0.557 worse, not significant
                                                                         persistence: -4.402 beats pk[-5.054,-3.829] (cov ref 0.963 / model 0.494)
paired_margin_window_intraday.csv     busy  nf_nhits                  h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +3.286 REGRESSION pk[+2.700,+3.911]
                                                                         h5: +2.920 REGRESSION pk[+2.240,+3.616]
                                                                         persistence: -8.805 GATED [NOT TESTED: model-side coverage 0.246<0.3] (cov ref 0.952 / model 0.246)
paired_margin_window_intraday.csv     busy  nf_nhits                 m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -4.973 beats pk[-5.665,-4.374] (cov ref 0.982 / model 0.975)
                                                                         h5: -6.065 beats pk[-6.879,-5.339] (cov ref 0.982 / model 0.995)
                                                                         persistence: +4.079 REGRESSION pk[+3.628,+4.575]
paired_margin_window_intraday.csv     busy  nf_nhits                 m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -4.217 beats pk[-4.857,-3.659] (cov ref 0.982 / model 0.975)
                                                                         h5: -5.249 beats pk[-5.972,-4.571] (cov ref 0.982 / model 0.995)
                                                                         persistence: +1.928 REGRESSION pk[+1.573,+2.293]
paired_margin_window_intraday.csv     busy  nf_nhits                 m045  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -3.565 beats pk[-4.140,-3.045] (cov ref 0.982 / model 0.974)
                                                                         h5: -4.568 beats pk[-5.238,-3.956] (cov ref 0.982 / model 0.994)
                                                                         persistence: +0.778 REGRESSION pk[+0.478,+1.090]
paired_margin_window_intraday.csv     busy  nf_nhits                 m060  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -2.826 beats pk[-3.416,-2.265] (cov ref 0.982 / model 0.974)
                                                                         h5: -3.844 beats pk[-4.496,-3.239] (cov ref 0.981 / model 0.993)
                                                                         persistence: +0.002 worse, not significant
paired_margin_window_intraday.csv     busy  nf_nhits                 m075  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -1.420 beats pk[-2.005,-0.833] (cov ref 0.983 / model 0.975)
                                                                         h5: -2.530 beats pk[-3.345,-1.747] (cov ref 0.982 / model 0.994)
                                                                         persistence: +0.921 REGRESSION pk[+0.588,+1.268]
paired_margin_window_intraday.csv     busy  nf_nhits                 m090  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -1.431 beats pk[-1.930,-0.979] (cov ref 0.983 / model 0.974)
                                                                         h5: -2.495 beats pk[-3.227,-1.800] (cov ref 0.982 / model 0.994)
                                                                         persistence: -0.180 park-day only (park-cluster CI spans 0)
paired_margin_window_intraday.csv     busy  nf_nhits                 m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.198 beats pk[-1.657,-0.778] (cov ref 0.983 / model 0.974)
                                                                         h5: -2.304 beats pk[-3.009,-1.562] (cov ref 0.982 / model 0.994)
                                                                         persistence: -0.933 beats pk[-1.198,-0.649] (cov ref 0.972 / model 0.794)
paired_margin_window_intraday.csv     busy  nf_nhits                 m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -0.726 beats pk[-1.176,-0.304] (cov ref 0.982 / model 0.973)
                                                                         h5: -1.873 beats pk[-2.574,-1.127] (cov ref 0.981 / model 0.993)
                                                                         persistence: -1.461 beats pk[-1.798,-1.106] (cov ref 0.972 / model 0.785)
paired_margin_window_intraday.csv     busy  nf_tide                  h2-4  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.224 beats pk[-1.588,-0.826] (cov ref 0.981 / model 0.973)
                                                                         h5: -2.337 beats pk[-2.825,-1.874] (cov ref 0.981 / model 0.994)
                                                                         persistence: -3.286 beats pk[-3.843,-2.676] (cov ref 0.970 / model 0.666)
paired_margin_window_intraday.csv     busy  nf_tide                  h4-8  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -0.048 better, not significant
                                                                         h5: -1.130 beats pk[-1.607,-0.649] (cov ref 0.977 / model 0.993)
                                                                         persistence: -5.626 beats pk[-6.518,-4.766] (cov ref 0.963 / model 0.494)
paired_margin_window_intraday.csv     busy  nf_tide                   h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.845 REGRESSION pk[+0.299,+1.374]
                                                                         h5: +0.482 worse, not significant
                                                                         persistence: -10.748 GATED [NOT TESTED: model-side coverage 0.246<0.3] (cov ref 0.952 / model 0.246)
paired_margin_window_intraday.csv     busy  nf_tide                  m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -5.511 beats pk[-6.071,-4.936] (cov ref 0.982 / model 0.975)
                                                                         h5: -6.605 beats pk[-7.301,-5.918] (cov ref 0.982 / model 0.995)
                                                                         persistence: +3.738 REGRESSION pk[+3.369,+4.097]
paired_margin_window_intraday.csv     busy  nf_tide                  m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -4.883 beats pk[-5.440,-4.340] (cov ref 0.982 / model 0.975)
                                                                         h5: -5.921 beats pk[-6.572,-5.229] (cov ref 0.982 / model 0.995)
                                                                         persistence: +1.516 REGRESSION pk[+1.135,+1.899]
paired_margin_window_intraday.csv     busy  nf_tide                  m045  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -4.406 beats pk[-4.898,-3.907] (cov ref 0.982 / model 0.974)
                                                                         h5: -5.415 beats pk[-6.047,-4.791] (cov ref 0.982 / model 0.994)
                                                                         persistence: +0.302 worse, not significant
paired_margin_window_intraday.csv     busy  nf_tide                  m060  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -3.933 beats pk[-4.409,-3.439] (cov ref 0.982 / model 0.974)
                                                                         h5: -4.953 beats pk[-5.570,-4.326] (cov ref 0.981 / model 0.993)
                                                                         persistence: -0.552 beats pk[-0.963,-0.129] (cov ref 0.973 / model 0.870)
paired_margin_window_intraday.csv     busy  nf_tide                  m075  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -2.517 beats pk[-2.974,-2.064] (cov ref 0.983 / model 0.975)
                                                                         h5: -3.643 beats pk[-4.212,-3.083] (cov ref 0.982 / model 0.994)
                                                                         persistence: +0.013 worse, not significant
paired_margin_window_intraday.csv     busy  nf_tide                  m090  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.557 beats pk[-2.989,-2.108] (cov ref 0.983 / model 0.974)
                                                                         h5: -3.635 beats pk[-4.194,-3.094] (cov ref 0.982 / model 0.994)
                                                                         persistence: -1.028 beats pk[-1.510,-0.563] (cov ref 0.973 / model 0.809)
paired_margin_window_intraday.csv     busy  nf_tide                  m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.356 beats pk[-2.764,-1.913] (cov ref 0.983 / model 0.974)
                                                                         h5: -3.479 beats pk[-4.042,-2.905] (cov ref 0.982 / model 0.994)
                                                                         persistence: -1.731 beats pk[-2.221,-1.230] (cov ref 0.972 / model 0.794)
paired_margin_window_intraday.csv     busy  nf_tide                  m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.990 beats pk[-2.382,-1.576] (cov ref 0.982 / model 0.973)
                                                                         h5: -3.148 beats pk[-3.732,-2.571] (cov ref 0.981 / model 0.993)
                                                                         persistence: -2.332 beats pk[-2.867,-1.769] (cov ref 0.972 / model 0.785)
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx           h2-4  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -1.356 beats pk[-1.716,-0.969] (cov ref 0.981 / model 0.973)
                                                                         h5: -2.468 beats pk[-2.966,-1.998] (cov ref 0.981 / model 0.994)
                                                                         persistence: -3.324 beats pk[-3.877,-2.704] (cov ref 0.970 / model 0.666)
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx           h4-8  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -0.311 park-day only (park-cluster CI spans 0)
                                                                         h5: -1.389 beats pk[-1.860,-0.915] (cov ref 0.977 / model 0.993)
                                                                         persistence: -5.899 beats pk[-6.785,-5.057] (cov ref 0.963 / model 0.494)
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx            h8+  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.650 REGRESSION pk[+0.123,+1.184]
                                                                         h5: +0.284 worse, not significant
                                                                         persistence: -11.017 GATED [NOT TESTED: model-side coverage 0.246<0.3] (cov ref 0.952 / model 0.246)
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx           m015  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -5.607 beats pk[-6.178,-5.014] (cov ref 0.982 / model 0.975)
                                                                         h5: -6.701 beats pk[-7.399,-6.019] (cov ref 0.982 / model 0.995)
                                                                         persistence: +3.625 REGRESSION pk[+3.258,+3.986]
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx           m030  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: -4.966 beats pk[-5.519,-4.416] (cov ref 0.982 / model 0.975)
                                                                         h5: -6.003 beats pk[-6.667,-5.308] (cov ref 0.982 / model 0.995)
                                                                         persistence: +1.409 REGRESSION pk[+1.025,+1.793]
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx           m045  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -4.488 beats pk[-4.995,-3.976] (cov ref 0.982 / model 0.974)
                                                                         h5: -5.499 beats pk[-6.153,-4.861] (cov ref 0.982 / model 0.994)
                                                                         persistence: +0.186 worse, not significant
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx           m060  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -4.007 beats pk[-4.504,-3.506] (cov ref 0.982 / model 0.974)
                                                                         h5: -5.028 beats pk[-5.654,-4.397] (cov ref 0.981 / model 0.993)
                                                                         persistence: -0.665 beats pk[-1.065,-0.236] (cov ref 0.973 / model 0.870)
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx           m075  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -2.640 beats pk[-3.113,-2.170] (cov ref 0.983 / model 0.975)
                                                                         h5: -3.762 beats pk[-4.358,-3.182] (cov ref 0.982 / model 0.994)
                                                                         persistence: -0.041 better, not significant
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx           m090  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.718 beats pk[-3.158,-2.257] (cov ref 0.983 / model 0.974)
                                                                         h5: -3.793 beats pk[-4.366,-3.249] (cov ref 0.982 / model 0.994)
                                                                         persistence: -1.106 beats pk[-1.581,-0.646] (cov ref 0.973 / model 0.809)
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx           m105  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.569 beats pk[-2.990,-2.130] (cov ref 0.983 / model 0.974)
                                                                         h5: -3.690 beats pk[-4.273,-3.104] (cov ref 0.982 / model 0.994)
                                                                         persistence: -1.846 beats pk[-2.329,-1.353] (cov ref 0.972 / model 0.794)
paired_margin_window_intraday.csv     busy  nf_tide_wx_owx           m120  SHADOW (beats every incumbent; SERVE withheld -- cross-season sign clause untestable on autumn-only origins)
                                                                         lvlh5_tft: -2.255 beats pk[-2.665,-1.849] (cov ref 0.982 / model 0.973)
                                                                         h5: -3.410 beats pk[-4.009,-2.808] (cov ref 0.981 / model 0.993)
                                                                         persistence: -2.459 beats pk[-3.005,-1.892] (cov ref 0.972 / model 0.785)
paired_margin_window_slot.csv         all   nf_nhits                    0  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.541 REGRESSION pk[+1.264,+1.835]
                                                                         h5: +1.335 REGRESSION pk[+1.006,+1.674]
paired_margin_window_slot.csv         all   nf_nhits                    1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.482 REGRESSION pk[+1.231,+1.741]
                                                                         h5: +1.304 REGRESSION pk[+1.005,+1.616]
paired_margin_window_slot.csv         all   nf_nhits                    2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.405 REGRESSION pk[+1.171,+1.660]
                                                                         h5: +1.266 REGRESSION pk[+0.981,+1.582]
paired_margin_window_slot.csv         all   nf_nhits                    3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.326 REGRESSION pk[+1.100,+1.573]
                                                                         h5: +1.247 REGRESSION pk[+0.959,+1.564]
paired_margin_window_slot.csv         all   nf_nhits                    4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.303 REGRESSION pk[+1.086,+1.537]
                                                                         h5: +1.279 REGRESSION pk[+1.007,+1.586]
paired_margin_window_slot.csv         all   nf_nhits                    5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.278 REGRESSION pk[+1.076,+1.510]
                                                                         h5: +1.255 REGRESSION pk[+0.991,+1.548]
paired_margin_window_slot.csv         all   nf_nhits                    6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.190 REGRESSION pk[+0.994,+1.395]
                                                                         h5: +1.238 REGRESSION pk[+0.989,+1.520]
paired_margin_window_slot.csv         all   nf_nhits                    7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.924 REGRESSION pk[+0.704,+1.157]
                                                                         h5: +1.107 REGRESSION pk[+0.861,+1.390]
paired_margin_window_slot.csv         all   nf_nhits_x_h5               1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.360 REGRESSION pk[+1.117,+1.612]
                                                                         h5: +1.182 REGRESSION pk[+0.841,+1.520]
paired_margin_window_slot.csv         all   nf_nhits_x_h5               2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.364 REGRESSION pk[+1.105,+1.640]
                                                                         h5: +1.225 REGRESSION pk[+0.869,+1.585]
paired_margin_window_slot.csv         all   nf_nhits_x_h5               3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.365 REGRESSION pk[+1.105,+1.640]
                                                                         h5: +1.286 REGRESSION pk[+0.925,+1.645]
paired_margin_window_slot.csv         all   nf_nhits_x_h5               4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.210 REGRESSION pk[+0.974,+1.454]
                                                                         h5: +1.177 REGRESSION pk[+0.840,+1.515]
paired_margin_window_slot.csv         all   nf_nhits_x_h5               5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.220 REGRESSION pk[+0.977,+1.472]
                                                                         h5: +1.185 REGRESSION pk[+0.853,+1.523]
paired_margin_window_slot.csv         all   nf_nhits_x_h5               6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.101 REGRESSION pk[+0.881,+1.328]
                                                                         h5: +1.135 REGRESSION pk[+0.809,+1.464]
paired_margin_window_slot.csv         all   nf_nhits_x_h5               7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.859 REGRESSION pk[+0.623,+1.107]
                                                                         h5: +1.031 REGRESSION pk[+0.707,+1.359]
paired_margin_window_slot.csv         all   nf_tide                     0  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.142 REGRESSION pk[+0.003,+0.286]
                                                                         h5: -0.051 better, not significant
paired_margin_window_slot.csv         all   nf_tide                     1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.420 REGRESSION pk[+0.279,+0.562]
                                                                         h5: +0.251 REGRESSION pk[+0.083,+0.410]
paired_margin_window_slot.csv         all   nf_tide                     2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.610 REGRESSION pk[+0.419,+0.807]
                                                                         h5: +0.475 REGRESSION pk[+0.322,+0.642]
paired_margin_window_slot.csv         all   nf_tide                     3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.622 REGRESSION pk[+0.450,+0.785]
                                                                         h5: +0.544 REGRESSION pk[+0.400,+0.693]
paired_margin_window_slot.csv         all   nf_tide                     4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.504 REGRESSION pk[+0.314,+0.678]
                                                                         h5: +0.485 REGRESSION pk[+0.341,+0.638]
paired_margin_window_slot.csv         all   nf_tide                     5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.577 REGRESSION pk[+0.367,+0.780]
                                                                         h5: +0.557 REGRESSION pk[+0.405,+0.723]
paired_margin_window_slot.csv         all   nf_tide                     6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.361 REGRESSION pk[+0.183,+0.545]
                                                                         h5: +0.406 REGRESSION pk[+0.255,+0.565]
paired_margin_window_slot.csv         all   nf_tide                     7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.475 REGRESSION pk[+0.269,+0.691]
                                                                         h5: +0.653 REGRESSION pk[+0.518,+0.795]
paired_margin_window_slot.csv         all   nf_tide_wx_owx              0  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: +0.004 worse, not significant
                                                                         h5: -0.190 beats pk[-0.379,-0.005] (cov ref 0.981 / model 0.993)
paired_margin_window_slot.csv         all   nf_tide_wx_owx              1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.291 REGRESSION pk[+0.158,+0.423]
                                                                         h5: +0.120 worse, not significant
paired_margin_window_slot.csv         all   nf_tide_wx_owx              2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.473 REGRESSION pk[+0.296,+0.647]
                                                                         h5: +0.338 REGRESSION pk[+0.190,+0.494]
paired_margin_window_slot.csv         all   nf_tide_wx_owx              3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.380 REGRESSION pk[+0.219,+0.530]
                                                                         h5: +0.303 REGRESSION pk[+0.151,+0.462]
paired_margin_window_slot.csv         all   nf_tide_wx_owx              4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.433 REGRESSION pk[+0.229,+0.615]
                                                                         h5: +0.415 REGRESSION pk[+0.276,+0.568]
paired_margin_window_slot.csv         all   nf_tide_wx_owx              5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.384 REGRESSION pk[+0.202,+0.553]
                                                                         h5: +0.363 REGRESSION pk[+0.222,+0.521]
paired_margin_window_slot.csv         all   nf_tide_wx_owx              6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.275 REGRESSION pk[+0.079,+0.475]
                                                                         h5: +0.317 REGRESSION pk[+0.165,+0.486]
paired_margin_window_slot.csv         all   nf_tide_wx_owx              7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.277 REGRESSION pk[+0.082,+0.475]
                                                                         h5: +0.455 REGRESSION pk[+0.326,+0.593]
paired_margin_window_slot.csv         all   nf_tide_wx_owx_x_h5         1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.284 REGRESSION pk[+0.156,+0.407]
                                                                         h5: +0.115 worse, not significant
paired_margin_window_slot.csv         all   nf_tide_wx_owx_x_h5         2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.328 REGRESSION pk[+0.185,+0.480]
                                                                         h5: +0.197 worse, not significant
paired_margin_window_slot.csv         all   nf_tide_wx_owx_x_h5         3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.311 REGRESSION pk[+0.163,+0.453]
                                                                         h5: +0.235 REGRESSION pk[+0.024,+0.424]
paired_margin_window_slot.csv         all   nf_tide_wx_owx_x_h5         4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.277 REGRESSION pk[+0.107,+0.442]
                                                                         h5: +0.251 REGRESSION pk[+0.060,+0.427]
paired_margin_window_slot.csv         all   nf_tide_wx_owx_x_h5         5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.210 REGRESSION pk[+0.062,+0.355]
                                                                         h5: +0.185 worse, not significant
paired_margin_window_slot.csv         all   nf_tide_wx_owx_x_h5         6  no win
                                                                         lvlh5_tft: +0.127 worse, not significant
                                                                         h5: +0.164 worse, not significant
paired_margin_window_slot.csv         all   nf_tide_wx_owx_x_h5         7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.048 worse, not significant
                                                                         h5: +0.223 REGRESSION pk[+0.045,+0.387]
paired_margin_window_slot.csv         all   nf_tide_x_h5                1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.519 REGRESSION pk[+0.385,+0.652]
                                                                         h5: +0.351 REGRESSION pk[+0.137,+0.543]
paired_margin_window_slot.csv         all   nf_tide_x_h5                2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.591 REGRESSION pk[+0.433,+0.764]
                                                                         h5: +0.461 REGRESSION pk[+0.253,+0.645]
paired_margin_window_slot.csv         all   nf_tide_x_h5                3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.550 REGRESSION pk[+0.395,+0.699]
                                                                         h5: +0.475 REGRESSION pk[+0.277,+0.656]
paired_margin_window_slot.csv         all   nf_tide_x_h5                4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.454 REGRESSION pk[+0.289,+0.612]
                                                                         h5: +0.428 REGRESSION pk[+0.218,+0.618]
paired_margin_window_slot.csv         all   nf_tide_x_h5                5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.524 REGRESSION pk[+0.349,+0.703]
                                                                         h5: +0.496 REGRESSION pk[+0.309,+0.665]
paired_margin_window_slot.csv         all   nf_tide_x_h5                6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.356 REGRESSION pk[+0.199,+0.519]
                                                                         h5: +0.395 REGRESSION pk[+0.190,+0.588]
paired_margin_window_slot.csv         all   nf_tide_x_h5                7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.272 REGRESSION pk[+0.109,+0.440]
                                                                         h5: +0.448 REGRESSION pk[+0.273,+0.615]
paired_margin_window_slot.csv         busy  nf_nhits                    0  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +3.423 REGRESSION pk[+2.898,+3.918]
                                                                         h5: +2.314 REGRESSION pk[+1.570,+3.092]
paired_margin_window_slot.csv         busy  nf_nhits                    1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +3.281 REGRESSION pk[+2.821,+3.733]
                                                                         h5: +2.206 REGRESSION pk[+1.522,+2.928]
paired_margin_window_slot.csv         busy  nf_nhits                    2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +3.102 REGRESSION pk[+2.634,+3.588]
                                                                         h5: +2.131 REGRESSION pk[+1.480,+2.835]
paired_margin_window_slot.csv         busy  nf_nhits                    3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.926 REGRESSION pk[+2.462,+3.429]
                                                                         h5: +2.104 REGRESSION pk[+1.431,+2.825]
paired_margin_window_slot.csv         busy  nf_nhits                    4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.901 REGRESSION pk[+2.475,+3.348]
                                                                         h5: +2.186 REGRESSION pk[+1.528,+2.844]
paired_margin_window_slot.csv         busy  nf_nhits                    5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.794 REGRESSION pk[+2.387,+3.232]
                                                                         h5: +2.132 REGRESSION pk[+1.527,+2.795]
paired_margin_window_slot.csv         busy  nf_nhits                    6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.649 REGRESSION pk[+2.276,+3.011]
                                                                         h5: +2.086 REGRESSION pk[+1.518,+2.698]
paired_margin_window_slot.csv         busy  nf_nhits                    7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.175 REGRESSION pk[+1.751,+2.568]
                                                                         h5: +1.856 REGRESSION pk[+1.290,+2.456]
paired_margin_window_slot.csv         busy  nf_nhits_x_h5               1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.760 REGRESSION pk[+2.256,+3.276]
                                                                         h5: +1.689 REGRESSION pk[+0.886,+2.503]
paired_margin_window_slot.csv         busy  nf_nhits_x_h5               2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.743 REGRESSION pk[+2.179,+3.358]
                                                                         h5: +1.777 REGRESSION pk[+0.935,+2.599]
paired_margin_window_slot.csv         busy  nf_nhits_x_h5               3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.710 REGRESSION pk[+2.134,+3.276]
                                                                         h5: +1.887 REGRESSION pk[+1.036,+2.715]
paired_margin_window_slot.csv         busy  nf_nhits_x_h5               4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.422 REGRESSION pk[+1.905,+2.936]
                                                                         h5: +1.689 REGRESSION pk[+0.891,+2.471]
paired_margin_window_slot.csv         busy  nf_nhits_x_h5               5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.414 REGRESSION pk[+1.900,+2.931]
                                                                         h5: +1.729 REGRESSION pk[+0.952,+2.514]
paired_margin_window_slot.csv         busy  nf_nhits_x_h5               6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +2.242 REGRESSION pk[+1.768,+2.706]
                                                                         h5: +1.655 REGRESSION pk[+0.883,+2.441]
paired_margin_window_slot.csv         busy  nf_nhits_x_h5               7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.760 REGRESSION pk[+1.199,+2.308]
                                                                         h5: +1.421 REGRESSION pk[+0.646,+2.189]
paired_margin_window_slot.csv         busy  nf_tide                     0  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.498 REGRESSION pk[+0.215,+0.790]
                                                                         h5: -0.596 beats pk[-1.009,-0.203] (cov ref 0.980 / model 0.994)
paired_margin_window_slot.csv         busy  nf_tide                     1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.162 REGRESSION pk[+0.851,+1.457]
                                                                         h5: +0.096 worse, not significant
paired_margin_window_slot.csv         busy  nf_tide                     2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.587 REGRESSION pk[+1.167,+1.964]
                                                                         h5: +0.619 REGRESSION pk[+0.251,+1.017]
paired_margin_window_slot.csv         busy  nf_tide                     3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.584 REGRESSION pk[+1.208,+1.940]
                                                                         h5: +0.757 REGRESSION pk[+0.418,+1.093]
paired_margin_window_slot.csv         busy  nf_tide                     4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.371 REGRESSION pk[+0.990,+1.744]
                                                                         h5: +0.662 REGRESSION pk[+0.321,+1.016]
paired_margin_window_slot.csv         busy  nf_tide                     5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.490 REGRESSION pk[+1.030,+1.944]
                                                                         h5: +0.828 REGRESSION pk[+0.473,+1.202]
paired_margin_window_slot.csv         busy  nf_tide                     6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.114 REGRESSION pk[+0.702,+1.510]
                                                                         h5: +0.542 REGRESSION pk[+0.193,+0.899]
paired_margin_window_slot.csv         busy  nf_tide                     7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.382 REGRESSION pk[+0.919,+1.833]
                                                                         h5: +1.051 REGRESSION pk[+0.760,+1.349]
paired_margin_window_slot.csv         busy  nf_tide_wx_owx              0  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: +0.254 worse, not significant
                                                                         h5: -0.842 beats pk[-1.288,-0.423] (cov ref 0.980 / model 0.994)
paired_margin_window_slot.csv         busy  nf_tide_wx_owx              1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.924 REGRESSION pk[+0.633,+1.213]
                                                                         h5: -0.145 better, not significant
paired_margin_window_slot.csv         busy  nf_tide_wx_owx              2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.299 REGRESSION pk[+0.920,+1.659]
                                                                         h5: +0.327 worse, not significant
paired_margin_window_slot.csv         busy  nf_tide_wx_owx              3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.099 REGRESSION pk[+0.736,+1.433]
                                                                         h5: +0.270 worse, not significant
paired_margin_window_slot.csv         busy  nf_tide_wx_owx              4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.233 REGRESSION pk[+0.803,+1.636]
                                                                         h5: +0.523 REGRESSION pk[+0.200,+0.872]
paired_margin_window_slot.csv         busy  nf_tide_wx_owx              5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.097 REGRESSION pk[+0.681,+1.484]
                                                                         h5: +0.432 REGRESSION pk[+0.110,+0.785]
paired_margin_window_slot.csv         busy  nf_tide_wx_owx              6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.954 REGRESSION pk[+0.519,+1.370]
                                                                         h5: +0.374 REGRESSION pk[+0.039,+0.763]
paired_margin_window_slot.csv         busy  nf_tide_wx_owx              7  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.998 REGRESSION pk[+0.560,+1.425]
                                                                         h5: +0.661 REGRESSION pk[+0.380,+0.951]
paired_margin_window_slot.csv         busy  nf_tide_wx_owx_x_h5         1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.474 REGRESSION pk[+0.188,+0.752]
                                                                         h5: -0.589 beats pk[-1.079,-0.151] (cov ref 0.977 / model 1.000)
paired_margin_window_slot.csv         busy  nf_tide_wx_owx_x_h5         2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.558 REGRESSION pk[+0.226,+0.887]
                                                                         h5: -0.402 park-day only (park-cluster CI spans 0)
paired_margin_window_slot.csv         busy  nf_tide_wx_owx_x_h5         3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.510 REGRESSION pk[+0.171,+0.831]
                                                                         h5: -0.314 park-day only (park-cluster CI spans 0)
paired_margin_window_slot.csv         busy  nf_tide_wx_owx_x_h5         4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.451 REGRESSION pk[+0.086,+0.814]
                                                                         h5: -0.275 park-day only (park-cluster CI spans 0)
paired_margin_window_slot.csv         busy  nf_tide_wx_owx_x_h5         5  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: +0.250 worse, not significant
                                                                         h5: -0.422 beats pk[-0.843,-0.040] (cov ref 0.973 / model 1.000)
paired_margin_window_slot.csv         busy  nf_tide_wx_owx_x_h5         6  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: +0.173 worse, not significant
                                                                         h5: -0.417 park-day only (park-cluster CI spans 0)
paired_margin_window_slot.csv         busy  nf_tide_wx_owx_x_h5         7  SHADOW (beats some but not all incumbents)
                                                                         lvlh5_tft: -0.002 better, not significant
                                                                         h5: -0.342 park-day only (park-cluster CI spans 0)
paired_margin_window_slot.csv         busy  nf_tide_x_h5                1  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.858 REGRESSION pk[+0.565,+1.156]
                                                                         h5: -0.204 better, not significant
paired_margin_window_slot.csv         busy  nf_tide_x_h5                2  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +1.034 REGRESSION pk[+0.682,+1.385]
                                                                         h5: +0.074 worse, not significant
paired_margin_window_slot.csv         busy  nf_tide_x_h5                3  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.878 REGRESSION pk[+0.515,+1.208]
                                                                         h5: +0.056 worse, not significant
paired_margin_window_slot.csv         busy  nf_tide_x_h5                4  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.757 REGRESSION pk[+0.405,+1.105]
                                                                         h5: +0.033 worse, not significant
paired_margin_window_slot.csv         busy  nf_tide_x_h5                5  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.806 REGRESSION pk[+0.417,+1.209]
                                                                         h5: +0.125 worse, not significant
paired_margin_window_slot.csv         busy  nf_tide_x_h5                6  DROP on this cell (significant regression vs an incumbent)
                                                                         lvlh5_tft: +0.564 REGRESSION pk[+0.196,+0.928]
                                                                         h5: -0.020 better, not significant
paired_margin_window_slot.csv         busy  nf_tide_x_h5                7  no win
                                                                         lvlh5_tft: +0.348 worse, not significant
                                                                         h5: +0.011 worse, not significant

tally: DROP on this cell=223, SHADOW=84, no win=5
