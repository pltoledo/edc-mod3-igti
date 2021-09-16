# Structured Streaming
Os dados sendo processados em streaming nesse módulo dizem respeito às ordens de compra e venda de cryptomoedas disponíveis no site <https://exchange.blockchain.com/trade>. 

Para utilizar essa funcionalidade, é preciso criar uma conta no site acima e criar uma chave de API. A chave então deve ser adicionada a um arquivo .env, sobre o nome de `API_TOKEN`.

Depois disso, basta executar os dois scripts de maneira simultânea, começando por `crypto_stream.py`. Os dados então começaram a ser gerados a partir da API e processados em tempo real pelo Spark.