# merge small elasticsearch indexes to blocks
app can merge small (by params) elasticsearch indexes to blocks
without any dependencies exclude python

## Task
for example, you have different (by size) indexes from different customers,
and you want to merge indexes by one customer to block with limit by size and days

## Example
for example - limit by 3 days and 100kb

you have indexes like this:
```bash
curl -XGET "http://localhost:9200/_cat/indices?s=index"

yellow open customer1.logs-2022.02.23 DteO0YT7RB-AYvm3-7Bq9g 1 1   1 0  18.1kb  18.1kb
yellow open customer1.logs-2022.02.24 z_MTcIBzS56AN4ZUiQ2Kgg 1 1   1 0  18.1kb  18.1kb
yellow open customer1.logs-2022.02.25 BeuaxxsKS9-sbPQxTe81kw 1 1   1 0  18.1kb  18.1kb
yellow open customer1.logs-2022.02.26 WRUz-P_STBuT0RpAzSDszg 1 1   1 0  18.1kb  18.1kb
yellow open customer1.logs-2022.02.27 E5QzEtAGRCyzEI3FjqQVbA 1 1   1 0  18.1kb  18.1kb
yellow open customer1.logs-2022.02.28 yGh9xS4SQfmrudgZtM1Bbg 1 1   1 0  18.1kb  18.1kb
yellow open customer4.logs-2022.02.23 7yD7xnSASPal_eCb8VxRGQ 1 1   1 0  37.6kb  37.6kb
yellow open customer4.logs-2022.02.24 _97Ulg7lRi2Vk400gv3ABQ 1 1   1 0  37.6kb  37.6kb
yellow open customer4.logs-2022.02.25 uA7nn8R1SK2kwbwZqoCaUA 1 1   1 0  37.6kb  37.6kb
yellow open customer4.logs-2022.02.26 7DjCP1HUQUuQmqMP9jtyVw 1 1   1 0  37.6kb  37.6kb
yellow open customer4.logs-2022.02.27 l_n4RPNxQLaWgU2u9HRzWA 1 1   1 0  37.6kb  37.6kb
yellow open customer4.logs-2022.02.28 AVk0-AgRTxuZp-ri_px1LQ 1 1   1 0  37.6kb  37.6kb
yellow open customer5.logs-2022.02.23 wBa_YXqETX2ExijFZpPCWA 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.24 l-JN4WKHSUONjJxW97Mv8g 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.25 DadjxDPfRpmmFjJnEpSq-g 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.26 iZPKGef6Qoy0oHLyYiov7w 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.27 kI8khdnTRdeEzStSdmgHzw 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.28 xkFoWxhJRri-Y7Ai86YV7A 1 1   1 0 128.2kb 128.2kb
```

app can merge these indexes like this:
- "big" (more than params) indexes from customer5 not merged
- "medium"/"small" indexes by customer4, customer1 merged to separated blocks
```bash
curl -XGET "http://localhost:9200/_cat/indices?s=index"

yellow open customer1.logs-2022.02.23-2022.02.26 NpFi7tX8QmeUfxIKXQDKUw 1 1   4 0  21.5kb  21.5kb
yellow open customer1.logs-2022.02.27            E5QzEtAGRCyzEI3FjqQVbA 1 1   1 0  18.1kb  18.1kb
yellow open customer1.logs-2022.02.28            yGh9xS4SQfmrudgZtM1Bbg 1 1   1 0  18.1kb  18.1kb
yellow open customer4.logs-2022.02.23-2022.02.24 ne4lh__7RdWiOOBDppirNw 1 1   2 0  40.3kb  40.3kb
yellow open customer4.logs-2022.02.25-2022.02.26 lmqLCECATPmcYeLtaVARAQ 1 1   2 0  40.3kb  40.3kb
yellow open customer4.logs-2022.02.27            l_n4RPNxQLaWgU2u9HRzWA 1 1   1 0  37.6kb  37.6kb
yellow open customer4.logs-2022.02.28            AVk0-AgRTxuZp-ri_px1LQ 1 1   1 0  37.6kb  37.6kb
yellow open customer5.logs-2022.02.23            wBa_YXqETX2ExijFZpPCWA 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.24            l-JN4WKHSUONjJxW97Mv8g 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.25            DadjxDPfRpmmFjJnEpSq-g 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.26            iZPKGef6Qoy0oHLyYiov7w 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.27            kI8khdnTRdeEzStSdmgHzw 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.28            xkFoWxhJRri-Y7Ai86YV7A 1 1   1 0 128.2kb 128.2kb
```

## How to run
change params (if you want) in `config.py`
run with IDE or from console: `python main.py` 

## How to local tests
generate data from another [project](https://github.com/axel-n/demo-data-for-elasticsearch)
run main.py from step `How to run`