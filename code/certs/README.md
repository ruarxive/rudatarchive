# Russian Trusted Root CA

Root and intermediate certificates of the Russian national CA (Ministry of Digital
Development). Some portals (rosstat.gov.ru, data.fss.ru and others) use TLS
certificates issued by it. certifi and OS stores do not include them, so requests
fails with `SSLError: certificate verify failed`.

Copied from the rulawmon project, downloaded from gu-st.ru (Gosuslugi file server).

| File | Subject | SHA-256 (DER) |
|---|---|---|
| `russian_trusted_root_ca.pem` | CN=Russian Trusted Root CA | `D2:6D:2D:02:31:B7:C3:9F:92:CC:73:85:12:BA:54:10:35:19:E4:40:5D:68:B5:BD:70:3E:97:88:CA:8E:CF:31` |
| `russian_trusted_sub_ca_2022.pem` | CN=Russian Trusted Sub CA | `BB:BD:E2:10:3E:79:0B:99:9E:C6:2B:D0:3C:F6:25:A5:A2:E7:C3:16:E1:0A:FE:6A:49:0E:ED:EA:D8:B3:FD:9B` |
| `russian_trusted_sub_ca_2024.pem` | CN=Russian Trusted Sub CA | `21:55:78:50:36:C9:00:DB:B5:F1:BB:2A:15:69:C8:0C:55:59:5B:D6:BF:94:86:7A:29:BB:DD:BC:7D:88:A3:F2` |

Build a bundle (certifi + these) and pass it to collectors via `RUDATA_CA_BUNDLE`:

```
cat $(python -c 'import certifi; print(certifi.where())') code/certs/*.pem > ca_bundle.pem
export RUDATA_CA_BUNDLE=$PWD/ca_bundle.pem
```

TLS verification stays on; the bundle only adds trust anchors.
