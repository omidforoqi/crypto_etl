# crypto_etl
crypto transactions data is from https://www.bitstamp.net


for running project run first this command

```bash
echo "PROJECT_NAME=bit" >> .env
```

```bash
docker compose up -d
```

metabase loaded in `localhost:3000` and email is `admin@admin.com` and password is `admin11` 

mage loaded in `localhost:6789`

etl process image in mage: 

![etl process](https://github.com/omidforoqi/crypto_etl/blob/master/img/etl_pipeline.png)

---

I visualize it in the metabase this is a sample from it:

![visualize](https://github.com/omidforoqi/crypto_etl/blob/master/img/visualization.png)

## to-do 
 - [ ] add kafka connector to clickhouse
 - [ ] add more visualization
 - [ ] add secret file to docker compose
 - [ ] integration for power bi
