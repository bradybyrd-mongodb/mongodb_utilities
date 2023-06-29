# ------------------------------------------------------------------- #
#             Srithal Bellary / VP Krishnan Demo
# ------------------------------------------------------------------- #
# 6/29/23

Setting: There are several projects where Redis is positioned as a cache and elastic for search
 The perception is that RelationalDB + REDIS + Elastic is an efficient stack
 We want to show that MongoDB is:
    - way more efficient - single stack
    - performant for transactions
    - performant for fast index queries like Redis
    - has onboard search that does not require data transport

Demo Stack (GCP):
    Postgres
    Redis
    Elastic (maybe...)
    Atlas

Data Set:
    Claim-Member-Provider
    Have dataGen scripts - looking to build about 10M docs, then scale to 100M for demo
    
