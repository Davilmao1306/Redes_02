# Prototipo P2P de coordenacao de drones

Este projeto implementa um prototipo em Python para coordenacao distribuida de uma frota compartilhada de drones. Ele usa 4 brokers, sensores autonomos e 8 drones simulados, todos em containers separados.

## Ideia geral

- Nao existe servidor central.
- Brokers trocam estado entre si e usam heartbeat para detectar falhas.
- O menor ID de broker ativo atua como coordenador deterministico temporario para despachar a fila global.
- Ocorrencias sao ordenadas por maior severidade, menor timestamp logico de Lamport e menor ID do broker em caso de empate.
- Drones se registram em brokers ativos e tentam reconectar usando a lista de brokers.
- Quando um broker cai, os demais detectam a falha e continuam despachando ocorrencias pendentes ja replicadas.

> Observacao: isto e um prototipo academico. Ele demonstra concorrencia, ordenacao logica e tolerancia a falhas, mas nao substitui um protocolo de consenso completo como Raft/Paxos em producao.

## Arquivos

- `common.py`: modelos, relogio de Lamport e utilitarios.
- `broker.py`: fila distribuida, heartbeat, replicacao, registro de drones e despacho de missoes.
- `sensor.py`: gera ocorrencias aleatorias automaticamente.
- `drone.py`: recebe missoes, simula execucao e reporta conclusao.
- `docker-compose.yml`: sobe 4 brokers, 4 sensores e 8 drones.

## Executar

```bash
docker compose up --build
```

Consultar o estado de um broker:

```bash
curl http://localhost:8001/state
curl http://localhost:8002/state
```

Simular queda de broker:

```bash
docker compose stop broker-1
```

Os logs dos brokers restantes devem mostrar deteccao de falha por heartbeat e continuidade do despacho:

```bash
docker compose logs -f broker-2 broker-3 broker-4
```

Religar broker:

```bash
docker compose start broker-1
```

## O que observar nos logs

- `ocorrencia criada`: sensor enviou uma ocorrencia para algum broker ativo.
- `prioridade=... ts=...`: severidade e timestamp logico da ocorrencia.
- `reserva`: broker coordenador reservou um drone para uma ocorrencia.
- `missao enviada`: drone aceitou a missao.
- `missao concluida`: drone terminou e liberou a frota.
- `falha detectada`: broker deixou de responder heartbeat.
- `redistribuindo ocorrencia pendente`: ocorrencias pendentes do broker falho continuam na fila replicada.

## Teste rapido de concorrencia

1. Execute `docker compose up --build`.
2. Aguarde os drones registrarem.
3. Observe que cada reserva associa um unico `drone` a uma unica `ocorrencia`.
4. Pare o broker coordenador atual, normalmente `broker-1`.
5. Um novo menor ID ativo assume o despacho, por exemplo `broker-2`.

## Limites do prototipo

- A replicacao usa troca periodica de snapshots, suficiente para demonstracao.
- A escolha do coordenador usa menor ID ativo, nao eleicao formal.
- Em falhas simultaneas ou particao de rede real, um protocolo de consenso seria necessario para garantias mais fortes.
