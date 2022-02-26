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
curl -XGET "http://localhost:9200/_cat/indices"

yellow open customer4.logs-2022.02.21 XAlL3XfYT8iGeuoNnIcjzQ 1 1   1 0  37.5kb  37.5kb
yellow open customer1.logs-2022.02.19 LgsvYGvSSS6T8PRkH5G2Iw 1 1   1 0  18.1kb  18.1kb
yellow open customer4.logs-2022.02.20 9_yXDsBjQqKXikrYcgWElw 1 1   1 0  37.5kb  37.5kb
yellow open customer5.logs-2022.02.19 sLQyeimzS7aSdwx-dRq_9g 1 1   1 0 128.1kb 128.1kb
yellow open customer1.logs-2022.02.20 yFAvChg3QSGdPCRbKw_SFg 1 1   1 0  18.1kb  18.1kb
yellow open customer4.logs-2022.02.19 CDmizaYCQLeHURdrrCf45g 1 1   1 0  37.5kb  37.5kb
yellow open customer1.logs-2022.02.21 Agbk6AXBTk-t_OqrzInxbQ 1 1   1 0  18.1kb  18.1kb
yellow open customer5.logs-2022.02.20 KNOhhtFLTuiJBphJbifThg 1 1   1 0 128.1kb 128.1kb
yellow open customer5.logs-2022.02.21 OHpojuLrQAaNmFmeQTf7Gg 1 1   1 0 128.1kb 128.1kb
```

app can merge these indexes like this:
- "big" (more than params) indexes from customer5 not merged
- "medium"/"small" indexes by customer4, customer1 merged to separated blocks
```bash
curl -XGET "http://localhost:9200/_cat/indices"

yellow open customer4.logs-2022.02.21            XAlL3XfYT8iGeuoNnIcjzQ 1 1   1 0  37.7kb  37.7kb
yellow open customer1.logs-2022.02.19-2022.02.20 9WksIhNYTe2eLGG9YyQkZQ 1 1   2 0  19.5kb  19.5kb
yellow open customer5.logs-2022.02.19            sLQyeimzS7aSdwx-dRq_9g 1 1   1 0 128.2kb 128.2kb
yellow open customer4.logs-2022.02.19-2022.02.20 R7OzYKVJTPuhGZdlffHQpQ 1 1   2 0  40.3kb  40.3kb
yellow open customer1.logs-2022.02.21            Agbk6AXBTk-t_OqrzInxbQ 1 1   1 0  18.2kb  18.2kb
yellow open customer5.logs-2022.02.20            KNOhhtFLTuiJBphJbifThg 1 1   1 0 128.2kb 128.2kb
yellow open customer5.logs-2022.02.21            OHpojuLrQAaNmFmeQTf7Gg 1 1   1 0 128.2kb 128.2kb
```

## How to run 
change params (if you want) in `config.py`
run with IDE or from console: `python main.py` 